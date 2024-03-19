package org.asw.kafkafactory;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Start a subscriber (seek) onto an instance of a Consumer<br>
 * runs for 24 hours, then needs to be respawned (RMJ can help here, but other
 * methods can be thought of). Iterates once for a seek command.
 */
public class ConsumerGeneric<V> {

	private KafkaClientFactory cf;
	private long timer;
	private Integer errorCount;
	private boolean doPrintValues;
	private KafkaConsumer<String, V> kafkaConsumer;

	private Long startTime;
	private Integer iterator = 0;
	private boolean doCommit;

	/**
	 * constructor<br>
	 * start an instance of a KafkaConsumer
	 * 
	 * @param cf instance of KafkaClientFactory
	 */
	public ConsumerGeneric(KafkaClientFactory cf) {
		this.cf = cf;
		this.errorCount = 0;
		setTimer(typeTimer.DAY_MILLIS);
		this.kafkaConsumer = new KafkaConsumer<String, V>(this.cf.getProperties());
	}

	/**
	 * dataprocessor also prints the Values.
	 * 
	 * @return This ConsumerGeneric (to allow chaining)
	 */
	public ConsumerGeneric<V> printValues() {
		doPrintValues = true;
		return this;
	}

	/**
	 * 1. when seek, then no timer (value=0), subscriber should then iterate exactly
	 * once (while loop) 2. when subscriber, look eternaly (24 hours).
	 * 
	 * @return boolean 
	 */
	public boolean keepIterating() {
		boolean b = false;
		if (this.timer == 0) {
			b = iterator < 1;
			iterator++;
		} else {
			b = System.currentTimeMillis() - this.startTime <= this.timer;
		}
		return b;
	}

	/**
	 * start the subscribe loop
	 * 
	 * @return This ConsumerGeneric (to allow chaining) (is that realy needed??)
	 * @throws Exception - generic Exception
	 */
	public ConsumerGeneric<V> start() throws Exception {
		startTime = System.currentTimeMillis();
		// Long startTime = System.currentTimeMillis();
		cf.print("=== ConsumerGeneric.start ===");
		cf.print(startTime.toString());
		cf.print("Timer " + this.timer);
		cf.print("Start Iterator: "+LocalDateTime.now().toString());
		while (keepIterating()) {
			ConsumerRecords<String, V> records = this.kafkaConsumer.poll(Duration.ofMillis(1000));
			for (ConsumerRecord<String, V> record : records) {
				//cf.print("debug-for1");
				this.processData(record.value().toString());
				//cf.print("debug-for2");
				if (this.doCommit) {
					kafkaConsumer.commitAsync();
				}
				//cf.print("debug-for3");
			}
		}
		cf.print("End Iterator: "+LocalDateTime.now().toString());
		kafkaConsumer.close();
		cf.closeJdbcConnection();
		return this;
	}

	/**
	 * Started a timed Subscriber on a topic<br>
	 * - in scope of subscribing on a topic, it'll run for 1 day. a respawner (like
	 * rmj) should keep it running in a forever loop.<br>
	 * 
	 * @return This ConsumerGeneric (to allow chaining)
	 * @throws Exception generic exception (should not crash)
	 */
	public ConsumerGeneric<V> subscribe() throws Exception {
		this.doCommit = true;
		kafkaConsumer.subscribe(Arrays.asList(cf.getTopic()));
		start();
		return this;
	}

	/**
	 * 
	 * enum timeTimer
	 *
	 */
	public enum typeTimer {
		/**
		 * ZERO = 0, do not iterate (seek)
		 */
		ZERO,
		/**
		 * DAY_MILLIS, iterate for 24 hours
		 */
		DAY_MILLIS,
		/**
		 * HOUR_MILLIS, Iterate for 1 hour
		 */
		HOUR_MILLIS,
		/**
		 * MINUT_MILLIS, iterate for 1 minute
		 */
		MINUTE_MILLIS;
	}

	/**
	 * Set the timer (how long should the subscriber loop before it stops
	 * 
	 * @return This ConsumerGeneric (to allow chaining)
	 * @param t typeTimer
	 */
	public ConsumerGeneric<V> setTimer(typeTimer t) {
		switch (t) {
		case ZERO:
			this.timer = 0;
			break;
		case DAY_MILLIS:
			this.timer = 1000 * 60 * 60 * 24;
			break;
		case HOUR_MILLIS:
			this.timer = 1000 * 60 * 60;
			break;
		case MINUTE_MILLIS:
			this.timer = 1000 * 60;
			break;
		default:
			this.timer = 0;
		}

		return this;
	}

	/**
	 * Start a seek process on an instance of a consumer<br>
	 * (a subscriber that stops after 1 itteration).
	 * 
	 * @return This ConsumerGeneric (to allow chaining)
	 * @throws Exception generic exception
	 */
	public ConsumerGeneric<V> seek() throws Exception {
		this.doCommit = false;
		setTimer(typeTimer.ZERO);
		TopicPartition topicPartition = new TopicPartition(cf.getTopic(), Integer.valueOf(cf.getPartition()));
		kafkaConsumer.assign(Arrays.asList(topicPartition));
		kafkaConsumer.seek(topicPartition, Long.valueOf(cf.getOffset()));
    start();
		return this;
	}
	
	/**
	 * Print a list of topics to the PrintWriter<br>
	 * Just instantiate a Consumer and the call this method:<br>
	 * new Consumer(kafkaClientFactory).printTopics();<br>
	 * <em>do not forget to initialize a printwriter in the kafkaFactory</em> :-)
	 */
	public void printTopics() {
	    Map<String, List<PartitionInfo>> m = this.kafkaConsumer.listTopics();
	    for (Map.Entry<String, List<PartitionInfo>> entry : m.entrySet()) {
	      cf.print(entry.getKey());
	    }
	}

	/**
	 * Process the message returned from a consumer record
	 * 
	 * @param value String - the message from an consumer record
	 * @throws Exception generic exception
	 */
	public void processData(String value) throws Exception {
		LocalDateTime start = LocalDateTime.now();
		cf.print("startTime Processing: "+start.toString());		
		if (KafkaUtil.isNotBlank(cf.getJdbcQuery()) && cf.jdbcConnection() != null) {
			try(CallableStatement stmt = cf.jdbcConnection().prepareCall("{ call " + cf.getJdbcQuery() + " }")){
				stmt.setString(1, value);
				stmt.execute();
			} catch (SQLException e) {
				this.errorCount++;
				cf.print(e.toString());
				if (this.errorCount > 100) {
					throw new Exception(String.format(
							"Max number of SQLExceptions in KafkaConsumerRecordProcessor.processData(). Last Message: %s",
							e.toString()));
				}
			}
		}
		
		if (this.doPrintValues) {
			cf.print(value);
		}
		
		LocalDateTime end = LocalDateTime.now();
		cf.print("endTime Processing: "+end.toString());
		cf.print("duration Processing: "+Duration.between(start,end).toMillis());
	}
}
