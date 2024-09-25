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
 * <p>
 * A producer publishes messages to Kafka topics.
 * </p>
 * 
 * <p>
 * Depending on parametersettings in the KafkaClientFactory: <br>
 * - publish a single String serialized message<br>
 * - publish a single AVRO serialized message<br>
 * - publish String serialized messages from a ref cursor<br>
 * - publish AVRO serialized messages from a ref cursor<br>
 * </p>
 * 
 * when both value and jdbc values are set: both will be done (a single message
 * and the ref cursor), in case of AVRO serializing, both message types must
 * match the AVRO schema.
 */
public class Producer {

	private RecordMetadata recordMetadata;
	private KafkaClientFactory kafkaClientFactory;

	KafkaProducer<String, SpecificRecord> kafkaProducerAVRO;
	KafkaProducer<String, String> kafkaProducerString;

	ProducerStatistics stats;
	
	boolean printCallbackMetaData = false;

	int messageCount;

//	/**
//	 * Constructor instantiate a Producer (either String or AVRO)
//	 * 
//	 * @param cf KafkaClientFactory instance
//	 * @param p  PrintWriter needed for printing (jcsOut in RMJ)
//	 * @throws Exception generic exception
//	 */
//	public Producer(KafkaClientFactory cf, PrintWriter p) throws Exception {
//		this(cf);
//		this.printwriter = p;
//		this.messageCount = 0;
//	}

	/**
	 * Constructor instantiate a Producer (either String or AVRO)
	 * 
	 * @param cf KafkaClientFactory instance
	 * @throws Exception generic exception
	 */
	public Producer(KafkaClientFactory cf) throws Exception {
		this.kafkaClientFactory = cf;
	}

	/**
	 * publish a message on instance of class
	 * 
	 * @return This Producer (to allow chaining)
	 * @throws Exception generic exception
	 */
	public Producer publish() throws Exception {
    
		try {
			switch (this.kafkaClientFactory.getTypeDeSer()) {
			case AVROSER:
				this.kafkaProducerAVRO = new KafkaProducer<String, SpecificRecord>(this.kafkaClientFactory.getProperties());
				break;
			default:
				this.kafkaProducerString = new KafkaProducer<String, String>(this.kafkaClientFactory.getProperties());
				break;
			}

			if (KafkaUtil.isNotBlank(this.kafkaClientFactory.getJdbcQuery())) {
				publishFromRefCursor();
			} else {
				if (this.kafkaClientFactory.publishValue() != null) {
					if (this.kafkaClientFactory.publishValue() instanceof SpecificRecord
							|| this.kafkaClientFactory.publishValue() instanceof String) {
						publishSingleMessage();
					}
				} else {
					throw new Exception("Nothing to publish. Expecting either a jdbcQuery OR a value!");
				}
			}
		} catch (Exception e) {
			throw new Exception("org.asw.kafkafactory.Producer.publish()",e);
		} finally {
			this.closeKafkaProducers();
		}

		return this;
	}

	private void closeKafkaProducers() {
		if (kafkaProducerAVRO != null) {
			kafkaProducerAVRO.close();
		  print("Avro kafkaProducer closed!");
		}  
		
		if (kafkaProducerString != null) {
			kafkaProducerString.close();
		  print("String kafkaProducer closed!");
		}  
	}

	private Producer publishSingleMessage() throws Exception {
		try {
			if (kafkaClientFactory.publishValue() instanceof SpecificRecord) {
				this.recordMetadata = this.kafkaProducerAVRO
						.send(new ProducerRecord<String, SpecificRecord>(kafkaClientFactory.getTopic(), kafkaClientFactory.getKey(),
								(SpecificRecord) kafkaClientFactory.publishValue()))
						.get();
			}
			if (kafkaClientFactory.publishValue() instanceof String) {
				this.recordMetadata = this.kafkaProducerString
						.send(new ProducerRecord<String, String>(kafkaClientFactory.getTopic(), kafkaClientFactory.getKey(),
								(String) kafkaClientFactory.publishValue()))
						.get();
			}

			this.printRecordMetaData();
			
		} catch (Exception e) {
			throw new Exception("org.asw.kafkafactory.Producer.publishSingleMessage()", e);
		}

		return this;
	}

	private Producer publishFromRefCursor() throws Exception {
		
		try {
			CallableStatement stmt = kafkaClientFactory.jdbcConnection().prepareCall("{ ? = call " + kafkaClientFactory.getJdbcQuery() + " }");
			stmt.registerOutParameter(1, Types.REF_CURSOR);
			stmt.execute();
			this.stats = new ProducerStatistics(kafkaClientFactory);

			try (ResultSet rset = (ResultSet) stmt.getObject(1)) {
				rset.setFetchSize(1000);
				while (rset.next()) {
					this.stats.incrI();
					BigDecimal bdid = rset.getBigDecimal(1);
					String id = bdid.toString();

					kafkaClientFactory.setTopic(rset.getString(2));
					kafkaClientFactory.setKey(rset.getString(3));
					kafkaClientFactory.setValue(rset.getString(4));

					this.publishWithCallback(id);
				}
			}
			this.stats.printStats();
		} catch (Exception e) {
			throw new Exception("org.asw.kafkafactory.Producer.publishFromRefCursor()",e);
		} finally {
			kafkaClientFactory.closeJdbcConnection();
		}	
		return this;
	}
	
	private Producer publishWithCallback(String messageId) throws Exception {
		try {
			if (kafkaClientFactory.publishValue() instanceof SpecificRecord) {
				this.kafkaProducerAVRO.send(
						new ProducerRecord<String, SpecificRecord>(kafkaClientFactory.getTopic(), kafkaClientFactory.getKey(),
								(SpecificRecord) kafkaClientFactory.publishValue()),
						//new ProducerCallback(messageId, this.printCallbackMetaData));
				    new ProducerCallback(messageId));
			}
			if (kafkaClientFactory.publishValue() instanceof String) {
				this.kafkaProducerString.send(
						new ProducerRecord<String, String>(kafkaClientFactory.getTopic(), kafkaClientFactory.getKey(),
								(String) kafkaClientFactory.publishValue()),
						//new ProducerCallback(messageId, this.printCallbackMetaData));
						new ProducerCallback(messageId));
			}
		} catch (Exception e) {
			throw new Exception("org.asw.kafkafactory.Producer.publishWithCallback",e);
		}
		return this;
	}	
	
	private class ProducerCallback implements Callback {

		String messageId;
		//boolean doPrintMetadata = false;

		//public ProducerCallback(String messageId, boolean doPrintMetadata) {
		public ProducerCallback(String messageId) {
			this.messageId = messageId;
			//this.doPrintMetadata = doPrintMetadata;
		}

		@Override
		public void onCompletion(RecordMetadata m, Exception e) {
			if (e != null) {
				print(String.format("Error messageId: %s, topic: %s, partition: %s, offset: %s, errormessage: %s%n",
						this.messageId, m.topic(), m.partition(), m.offset(), e.toString()));
			} //else {
				//if (doPrintMetadata) {
					//print(String.format("Succes messageId: %s, topic: %s, partition: %s, offset: %s, timestamp: %s",
					//		this.messageId, m.topic(), m.partition(), m.offset(), m.timestamp()));
				//}
			//}
		}
	}

	private void print(String s) {
		this.kafkaClientFactory.print(s);
	}

	/**
	 * For published (single) message, print a formatted list: topic, partition,
	 * offset, timestamp (utc epoch value)<br>
	 * metaData is empty for ref cursors. Use printErrors instead.<br>
	 * 
	 * @return This Producer (to allow chaining)
	 */
	public Producer printMetadata() {
		this.printCallbackMetaData = true;
		return this;
	}
	
	private void printRecordMetaData() {
		if (this.recordMetadata != null && printCallbackMetaData ) {
			print("==== Producer MetaData ====");
			print(String.format("topic: %s", this.recordMetadata.topic()));
			print(String.format("partition: %s", this.recordMetadata.partition()));
			print(String.format("offset: %s", this.recordMetadata.offset()));
			print(String.format("timestamp: %s", this.recordMetadata.timestamp()));
		}
	}

}
