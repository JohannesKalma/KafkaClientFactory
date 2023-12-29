package org.asw.kafkafactory;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
/**
* publish a message on a instance of a KafkaProducer<br>
* 
* <p>A producer publishes messages to Kafka topics.</p>
* 
* <p>Depending on parametersettings in the KafkaClientFactory: <br>
* - publish a single String serialized message<br>
* - publish a single AVRO serialized message<br>
* - publish String serialized messages from a ref cursor<br>
* - publish AVRO serialized messages from a ref cursor<br></p>
* 
* when both value and jdbc values are set: both will be done (a single message and the ref cursor), in case of AVRO serializing, both message types must match the AVRO schema.
*/
public class Producer {

	private RecordMetadata recordMetadata;
	private KafkaClientFactory cf;
  private Connection connection;
  private PrintWriter p;
  
	KafkaProducer<String,SpecificRecord> kafkaProducerAVRO;
	KafkaProducer<String,String> kafkaProducerString;
	
	///**
	// * Return publisher's RecordMetadata object (containing topic, partition, offset, ea)
	// * 
	// * @return recordMetadata RecordMetadata 
	// */
	//public RecordMetadata getRecordMetadata() {
	//	return recordMetadata;
	//}

	//private void setRecordMetadata(RecordMetadata recordMetadata) {
	//	this.recordMetadata = recordMetadata;
	//}
	/**
	 * Constructor instantiate a Producer (either String or AVRO)
	 * @param cf KafkaClientFactory instance
	 * @param p PrintWriter needed for printing (jcsOut in RMJ)
	 * @throws Exception generic exception
	 */	
	public Producer(KafkaClientFactory cf,PrintWriter p) throws Exception {
		this(cf);
		this.p = p;
	}
	
	/**
	 * Constructor instantiate a Producer (either String or AVRO)
	 * @param cf KafkaClientFactory instance
	 * @throws Exception generic exception
	 */
	public Producer(KafkaClientFactory cf) throws Exception {
		this.cf = cf;
    if (cf.publishValue() instanceof String) {
      	KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String,String>(cf.getProperties());
				this.kafkaProducerString = kafkaProducer;
    }
 
    if 	(cf.publishValue() instanceof SpecificRecord) {
        KafkaProducer<String,SpecificRecord> kafkaProducer = new KafkaProducer<String,SpecificRecord>(cf.getProperties());
    		this.kafkaProducerAVRO = kafkaProducer;
    }
	}
	
	/**
	 * publish a message on instance of class
	 * @return Producer instance
	 * @throws Exception generic exception
	 */
	public Producer publish() throws Exception {
		if (cf.publishValue() != null) {
			if (cf.publishValue() instanceof SpecificRecord) {
		  	this.recordMetadata=this.kafkaProducerAVRO.send(new ProducerRecord<String, SpecificRecord>(cf.getTopic(),cf.getKey(),(SpecificRecord)cf.publishValue())).get();		
		  	kafkaProducerAVRO.close();
			}			
			if (cf.publishValue() instanceof String) {
	    	this.recordMetadata=this.kafkaProducerString.send(new ProducerRecord<String, String>(cf.getTopic(),cf.getKey(),(String)cf.publishValue())).get();
	    	kafkaProducerString.close();
	    }
			this.closeKafkaProducers();
		} 
		
		if (KafkaUtil.isNotBlank(cf.getJdbcQuery())) {
    	publishFromRefCursor();
    }

		return this;
	}

	private void closeKafkaProducers() {
		if (kafkaProducerAVRO != null) kafkaProducerAVRO.close();
		if (kafkaProducerString != null) kafkaProducerString.close();
	}
	
	private Producer publishWithCallback(String messageId) throws Exception {
		if (cf.publishValue() instanceof SpecificRecord) {
	  	this.kafkaProducerAVRO.send(new ProducerRecord<String, SpecificRecord>(cf.getTopic(),cf.getKey(),(SpecificRecord)cf.publishValue()),new ProducerCallback(messageId));		
		}			
		if (cf.publishValue() instanceof String) {
    	this.kafkaProducerString.send(new ProducerRecord<String, String>(cf.getTopic(),cf.getKey(),(String)cf.publishValue()),new ProducerCallback(messageId));
    }
		return this;
	}
	
	private class ProducerCallback implements Callback {
		
		String messageId;
		
		public ProducerCallback(String messageId) {
			this.messageId = messageId;
		}
		
		@Override
		public void onCompletion(RecordMetadata m, Exception e) {
	    if (e != null) {
	      print(String.format("Error messageId: %s, topic: %s, partition: %s, offset: %s, errormessage: %%s%n",this.messageId, m.topic(), m.partition(), m.offset(),e.toString()));
	    }
		}
	}
	
	private Producer publishFromRefCursor() throws Exception {
    connection = cf.jdbcConnection();
    
  	CallableStatement stmt = this.connection.prepareCall ("{ ? = call " + cf.getJdbcQuery()  + " }");
    stmt.registerOutParameter (1,Types.REF_CURSOR);
  	stmt.execute();
    
  	try (ResultSet rset = (ResultSet)stmt.getObject (1)){
     rset.setFetchSize(1000);
     while (rset.next ()) {
       //stat.incrI();

       BigDecimal bdid    = rset.getBigDecimal(1);
       String id    = bdid.toString();
       //String topic = rset.getString (2);
       //String key   = rset.getString (3);
       //String value = rset.getString (4);

       cf.setTopic(rset.getString (2));
       cf.setKey(rset.getString (3));
       cf.setValue(rset.getString (4));
       
       this.publishWithCallback(id);
     }

     //stat.printStats();
   
 	  }
    connection.close();
    closeKafkaProducers();
    return this;
	}
	
	private void print(String s) {
		if (p != null) {
			p.println(s);
		}
	}
	
	/**
	 * For published (single) message, print a formatted list: topic, partition, offset, timestamp (utc epoch value)<br> 
	 * metaData is empty for ref cursors. Use printErrors instead.<br>
	 * Assumed printWriter has been enabled in KafkaClientFactory
	 * @return KafkaPublisher instance
	 */
	public Producer printMetadata() {
		return printMetadata(cf.getPrintwriter());
	}
	
	/**
	 * For published (single) message, print a formatted list: topic, partition, offset, timestamp (utc epoch value)<br> 
	 * metaData is empty for ref cursors. Use printErrors instead.
	 * 
	 * @param p PrintWriter
	 * @return KafkaPublisher instance
	 */
	public Producer printMetadata(PrintWriter p) {
		this.p = p;
  	print("==== Producer MetaData ====");
		print(String.format("topic: %s",this.recordMetadata.topic()));
		print(String.format("partition: %s",this.recordMetadata.partition()));
		print(String.format("offset: %s",this.recordMetadata.offset()));
		print(String.format("timestamp: %s",this.recordMetadata.timestamp()));
		return this;
	}
}
