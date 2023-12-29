package org.asw.kafkafactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * Start a subscriber (seek) onto an instance of a Consumer<br>
 * runs for 24 hours, then needs to be respawned (RMJ can help here, but other methods can be thought of).
 * Iterates once for a seek command.
 */
public class ConsumerGeneric<V> {

	private KafkaClientFactory cf;
  private long timer;
	private Integer errorCount;
	private boolean doPrintValues;
	private KafkaConsumer<String,V> kafkaConsumer;
	
	private Long startTime;
	private Integer iterator=0; 
	boolean doCommit;
	
	/**
	 * constructor<br>
	 * start an instance of a KafkaConsumer
	 * @param cf instance of KafkaClientFactory
	 */
	public ConsumerGeneric(KafkaClientFactory cf) {
    this.cf = cf;
    this.errorCount = 0;
    setTimer(typeTimer.MINUTE_MILLIS);
    this.kafkaConsumer = new KafkaConsumer<String,V>(this.cf.getProperties());
	}
	
  /**
   * dataprocessor also prints the Values.
   * @return Consumer instance
   */
	public ConsumerGeneric<V> printValues() {
		doPrintValues=true;
		return this;
	}
	
	private boolean keepIterating() {
		boolean i = false;
		if (this.timer==0) {
      i = iterator < 1; 	  	
	  } else {
	  	i = System.currentTimeMillis() - this.startTime <= this.timer;
	  }
		iterator++;
		return i;
	}
	
  /**
   * start the subscribe loop
   * @return ConsumerGeneric instance (is that realy needed??)
   * @throws Exception - generic Exception
   */
	private ConsumerGeneric<V> start() throws Exception {
		startTime = System.currentTimeMillis();
		//Long startTime = System.currentTimeMillis();
		cf.print(startTime.toString());
		cf.print("Timer "+this.timer);
    cf.print(LocalDateTime.now().toString());
		while (keepIterating()) {
      ConsumerRecords<String, V> records = kafkaConsumer.poll(Duration.ofMillis(1000));
	  	for (ConsumerRecord<String, V> record : records) {
	  		 this.processData(record.value().toString());
	  		 if (this.doCommit) {
	  		   kafkaConsumer.commitAsync();
	  		 }
	  	}
		}
  	cf.print(LocalDateTime.now().toString());
		kafkaConsumer.close();
		cf.closeJdbcConnection();
		return this;
	}
	
	/**
	 * Started a timed Subscriber on a topic<br>
	 * - in scope of subscribing on a topic, it'll run for 1 day. a respawner (like rmj) should keep it running in a forever loop.<br>
	 *   
	 * @return Consumer instance of this class
	 * @throws Exception generic exception (should not crash)
	 */	
	public ConsumerGeneric<V> subscribe() throws Exception {
		this.doCommit=true;
		kafkaConsumer.subscribe(Arrays.asList(cf.getTopic()));
    //setTimer(typeTimer.DAY_MILLIS);
		//setTimer(typeTimer.ZERO);
		return start();
	}
	
	private enum typeTimer{
		ZERO,DAY_MILLIS,MINUTE_MILLIS;
	}
	
	private void setTimer(typeTimer t) {
		switch(t) {
		case ZERO:
		  this.timer=0;
		  break;
		case DAY_MILLIS:
			this.timer=1000 * 60 * 60 * 24;
			break;
		case 	MINUTE_MILLIS:
			this.timer= 1000 * 60;
			break;
		default:
			this.timer=0;
		}
	}
	
	/**
	 * Start a seek process on an instance of a consumer<br>
	 * (a subscriber that stops after 1 itteration).
	 * @return Consumer instance of class
	 * @throws Exception generic exception
	 */
	public ConsumerGeneric<V> seek() throws Exception {
		this.doCommit=false;
		setTimer(typeTimer.ZERO);
		TopicPartition topicPartition = new TopicPartition(cf.getTopic(), Integer.valueOf(cf.getPartition()));
		kafkaConsumer.assign(Arrays.asList(topicPartition));
		kafkaConsumer.seek(topicPartition, Long.valueOf(cf.getOffset()));

		return start();
	}

	/**
	 * Process the message returned from a consumer record
	 * @param value String - the message from an consumer record
	 * @throws Exception generic exception
	 */
	public void processData(String value) throws Exception {
		if (cf.jdbcConnection() != null && KafkaUtil.isNotBlank(cf.getJdbcQuery())) {
	    String query = cf.getJdbcQuery();
	    try (PreparedStatement stmt=cf.jdbcConnection().prepareStatement(query)) {
	    	stmt.setString(1, value);
	    	stmt.execute();
	    } catch (SQLException e) {
	    	this.errorCount++;
	    	cf.print(e.toString());
	    	if (this.errorCount > 100) {
	    		throw new Exception(String.format("Max number of SQLExceptions in KafkaConsumerRecordProcessor.processData(). Last Message: %s",e.toString()));
	    	}
	    }
		} 
		
		if (this.doPrintValues) {
			cf.print(value);
		}
	}	
}
