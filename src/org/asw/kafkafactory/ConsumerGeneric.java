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

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Start a subscriber (seek) onto an instance of a Consumer<br>
 * runs for 24 hours, then needs to be respawned (RMJ can help here, but other
 * methods can be thought of). Iterates once for a seek command.
 */

public class ConsumerGeneric<V> {

	private KafkaClientFactory kafkaClientFactory;
	private long timer;
	private Integer errorCount;
	private boolean doPrintValues;
	private boolean doPrintMetadata;
	private boolean doPrintProcessingdata;
	private KafkaConsumer<String, V> kafkaConsumer;

	private Long startTime;
	private Integer iterator = 0;
	private boolean doCommit;
	private Integer maxErrorCount;

	/**
	 * constructor<br>
	 * start an instance of a KafkaConsumer
	 * 
	 * @param cf instance of KafkaClientFactory
	 * @throws Exception 
	 */
	public ConsumerGeneric(KafkaClientFactory cf) throws Exception {
		this.kafkaClientFactory = cf;
		this.errorCount = 0;
		
		if (cf.getMaxErrorCount() == null) {
			this.maxErrorCount = 100;
		} else {
			try {
			  this.maxErrorCount = Integer.parseInt(cf.getMaxErrorCount());
			} catch (Exception e) {
				throw new Exception(String.format("maxErrorCount should be an integer above zero"));				
			}
		}
	
		this.setTimer(typeTimer.DAY_MILLIS);

		try {
		  this.kafkaConsumer = new KafkaConsumer<String, V>(this.kafkaClientFactory.getProperties());
		} catch (Exception e) {
			this.kafkaConsumer.close();
      print("kafkaConsumer closed!");
			throw new Exception ("org.asw.kafkafactory.ConsumerGeneric<V>.ConsumerGeneric(KafkaClientFactory cf)",e);
		} 
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
	 * dataprocessor also prints the Values.
	 * 
	 * @return This ConsumerGeneric (to allow chaining)
	 */
	public ConsumerGeneric<V> printMetadata() {
		doPrintMetadata = true;
		return this;
	}
	
	/**
	 * dataprocessor also prints the Values.
	 * 
	 * @return This ConsumerGeneric (to allow chaining)
	 */
	public ConsumerGeneric<V> printProcessingdata() {
		doPrintProcessingdata = true;
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
		try {
			startTime = System.currentTimeMillis();

			print("=== Consumer start ===");
			print(startTime.toString());
			print("Timer " + this.timer);
			print("Start Iterator: " + LocalDateTime.now().toString());
			while (keepIterating()) {
				ConsumerRecords<String, V> records = this.kafkaConsumer.poll(Duration.ofMillis(1000));
				for (ConsumerRecord<String, V> record : records) {
					this.processData(record);
					if (this.doCommit) {
						kafkaConsumer.commitAsync();
					}
				}
			}
			kafkaClientFactory.print("End Iterator: " + LocalDateTime.now().toString());
		} catch (Exception e) {
			throw new Exception("org.asw.kafkafactory.ConsumerGeneric<V>.start()", e);
		} finally {
			this.kafkaConsumer.close();
			print("kafkaConsumer closed!");
			kafkaClientFactory.closeJdbcConnection();
		}
		return this;
	}

	class messageMetaData{
		String key;
		String topic;
		String partition;
		String offset;
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
		try {
			this.kafkaConsumer.subscribe(Arrays.asList(this.kafkaClientFactory.getTopic()));
			start();
		} catch (Exception e) {
			this.kafkaConsumer.close();
			print("kafkaConsumer closed!");
			throw new Exception("org.asw.kafkafactory.ConsumerGeneric<V>.subscribe()", e);
		}
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
		try {
			this.doCommit = false;
			setTimer(typeTimer.ZERO);
			TopicPartition topicPartition = new TopicPartition(kafkaClientFactory.getTopic(),
					Integer.valueOf(kafkaClientFactory.getPartition()));
			this.kafkaConsumer = new KafkaConsumer<String, V>(this.kafkaClientFactory.getProperties());
			this.kafkaConsumer.assign(Arrays.asList(topicPartition));
			this.kafkaConsumer.seek(topicPartition, Long.valueOf(kafkaClientFactory.getOffset()));
			start();
		} catch (Exception e) {
			throw new Exception("org.asw.kafkafactory.ConsumerGeneric<V>.seek()", e);
		}
    
		return this;
	}
	
	private void print(String s) {
		this.kafkaClientFactory.print(s);
	}
	
	
	/**
	 * Print a list of topics to the PrintWriter<br>
	 * Just instantiate a Consumer and the call this method:<br>
	 * new Consumer(kafkaClientFactory).printTopics();<br>
	 * <em>do not forget to initialize a printwriter in the kafkaFactory</em> :-)
	 */
	public void printTopics() {
		  try {
		  	Map<String, List<PartitionInfo>> m = this.kafkaConsumer.listTopics();
		    for (Map.Entry<String, List<PartitionInfo>> entry : m.entrySet()) {
		    	print(entry.getKey());
		    }
		  } finally {
		  	this.kafkaConsumer.close();
		  }
	}

	/**
	 * Process the returned ConsumerRecord<br>
	 * <strong>this should be a private method.</strong> 
	 * 
	 * @param record - the complete ConsumerRecord
	 * @throws Exception generic exception
	 */
	public void processData(ConsumerRecord<String, V> record) throws Exception {

		String value = record.value().toString();
		String metaData = new ObjectMapper().writeValueAsString(new RecordMetadata(record));

		LocalDateTime start = LocalDateTime.now();
		if (KafkaUtil.isNotBlank(kafkaClientFactory.getJdbcQuery()) && kafkaClientFactory.jdbcConnection() != null) {
			try (CallableStatement stmt = kafkaClientFactory.jdbcConnection().prepareCall("{ call " + kafkaClientFactory.getJdbcQuery() + " }")) {
				switch (stmt.getParameterMetaData().getParameterCount()) {
				case 0:
					throw new Exception(String.format("Assign at least 1 bindvariable!"));
				case 1:
					try {
						stmt.setString(1, value);
					} catch (Exception e) {
						print("setString(1) failed: " + e.toString());
						throw new Exception(e);
					}
					break;
				case 2:
					try {
						stmt.setString(1, value);
						stmt.setString(2, metaData);
					} catch (Exception e) {
						print("setString(2) failed: " + e.toString());
						throw new Exception(e);
					}
					break;
				default:
					throw new Exception(String.format("Max number bindvariables (2) exceeded!"));
				}

				stmt.execute();
			} catch (SQLException e) {
				deadLetter(metaData);
				this.errorCount++;
				kafkaClientFactory.print(e.toString());
				if (this.errorCount >= this.maxErrorCount) {
					throw new Exception(String.format(
							"Max number (%s) of Errors in KafkaConsumer.processData().%n Last Error Message: %s%n",
							this.maxErrorCount,e.toString()));
				}
			}
		}

		if (this.doPrintValues) {
			kafkaClientFactory.print(value);
		}

		if (this.doPrintMetadata) {
			kafkaClientFactory.print(metaData);
		}

    if (this.doPrintProcessingdata) {
  		LocalDateTime end = LocalDateTime.now();
  		kafkaClientFactory.print(String.format("Processing Key: %s, End: %s, Duration: %s (ms) %n", record.key(), end.toString(),
  				Duration.between(start, end).toMillis()));
    }
	}
	
	private void deadLetter(String metadata) {
		//
	}
	
	class RecordMetadata{
		
		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Long getOffset() {
			return offset;
		}

		public void setOffset(Long offset) {
			this.offset = offset;
		}

		public Integer getPartition() {
			return partition;
		}

		public void setPartition(Integer partition) {
			this.partition = partition;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		String key;
		Long offset;
		Integer partition;
		Long timestamp;
		String topic;

		/**
		 * @param s
		 */
		public RecordMetadata(ConsumerRecord<String, V> record) {
			this.key = record.key();
			this.offset = record.offset();
			this.partition = record.partition();
			this.timestamp = record.timestamp();
      this.topic = record.topic();
		}
	}
}
