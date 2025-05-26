package org.asw.kafkafactory;

import java.sql.CallableStatement;
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
	
	boolean printRecordMetaData = false;
	
    //ErrorHandler errorHandler;

	/**
	 * Constructor instantiate a Producer (either String or AVRO)
	 * 
	 * @param kafkaClientFactory KafkaClientFactory instance
	 * @throws Exception generic exception
	 */
	public Producer(KafkaClientFactory kafkaClientFactory) throws Exception {
		this.kafkaClientFactory = kafkaClientFactory;
		kafkaClientFactory.printInit();
		//this.errorHandler = new ErrorHandler(this.kafkaClientFactory);
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
				if (this.kafkaClientFactory.publishValue() != null 
					  && (this.kafkaClientFactory.publishValue() instanceof SpecificRecord
							  || this.kafkaClientFactory.publishValue() instanceof String )) {
						publishSingleMessage();
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
		} catch (Exception e) {
			throw new Exception("org.asw.kafkafactory.Producer.publishSingleMessage()", e);
		} finally {
			this.printRecordMetaData();
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
					String id = String.valueOf(rset.getBigDecimal(1));
          
					kafkaClientFactory.setTopic(rset.getString(2));
					kafkaClientFactory.setKey(rset.getString(3));
					kafkaClientFactory.setValue(rset.getString(4));
          
					if ((KafkaUtil.isNotBlank(kafkaClientFactory.getTopic()) && KafkaUtil.isNotBlank(kafkaClientFactory.getValue()))) { 
   					  this.publishWithCallback(id,this.stats.getI());
					} else {
						try {
						  print(String.format("cursor % returned invalid topic and/or value. Got parametervalues: %s, %s, %s, %s", rset.getCursorName(),String.valueOf(rset.getBigDecimal(1)),rset.getString(2),rset.getString(3),rset.getString(4)));
						} catch (Exception e) {
							print("cursor returned invalid values");
						}
					}
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
	
	private Producer publishWithCallback(String messageId,Integer i) throws Exception {
		try {
			if (kafkaClientFactory.publishValue() instanceof SpecificRecord) {
				this.kafkaProducerAVRO.send(new ProducerRecord<String, SpecificRecord>(kafkaClientFactory.getTopic(),
						kafkaClientFactory.getKey(), (SpecificRecord) kafkaClientFactory.publishValue()),
						new ProducerCallback(messageId, i));
			}
			if (this.kafkaClientFactory.publishValue() instanceof String) {
				this.kafkaProducerString.send(new ProducerRecord<String, String>(kafkaClientFactory.getTopic(),
						kafkaClientFactory.getKey(), (String) kafkaClientFactory.publishValue()),
						new ProducerCallback(messageId, i));
			}
		} catch (Exception e) {
			throw new Exception("org.asw.kafkafactory.Producer.publishWithCallback",e);
		}
		return this;
	}	
	
	private class ProducerCallback implements Callback {

		String messageId;
		Integer i;
		public ProducerCallback(String messageId,Integer i) {
			this.messageId = messageId;
			this.i = i;
		}

		private void printMeta(RecordMetadata m) {
		  print(String.format("Published messageId: %s, topic: %s, partition: %s, offset: %s, timestamp: %s",
				this.messageId, m.topic(), m.partition(), m.offset(), m.timestamp()));			
		}
		
		@Override
		public void onCompletion(RecordMetadata m, Exception e) {
			if (e != null) {
				print(String.format("Error messageId: %s, topic: %s, partition: %s, offset: %s, errormessage: %s",
						this.messageId, m.topic(), m.partition(), m.offset(), e.toString()));
			} else {
				print("first published message:");
				printMeta(m);
				if ((i>1 && i < 100) && printRecordMetaData) {
						printMeta(m);
				} else if (i == 100 || i==1000 || i==1000) {
					print(String.format("more than %s messages published ....",i));
					printMeta(m);
				}
			}
		}
	}

	private void print(String s) {
		//kafkaClientFactory.print(s);
		kafkaClientFactory.printLog(s);
	}

	/**
	 * For published (single) message, print a formatted list: topic, partition,
	 * offset, timestamp (utc epoch value)<br>
	 * metaData is empty for ref cursors. Use printErrors instead.<br>
	 * 
	 * @return This Producer (to allow chaining)
	 */
	public Producer printMetadata() {
		this.printRecordMetaData = true;
		return this;
	}
	
	private void printRecordMetaData() {
		if (this.recordMetadata != null && this.printRecordMetaData ) {
			print("==== Producer MetaData ====");
			print(String.format("topic: %s", this.recordMetadata.topic()));
			print(String.format("partition: %s", this.recordMetadata.partition()));
			print(String.format("offset: %s", this.recordMetadata.offset()));
			print(String.format("timestamp: %s", this.recordMetadata.timestamp()));
		}
	}
}
