package org.asw.kafkafactory;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.concurrent.ExecutionException;

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
	private Connection connection;

	private PrintWriter printwriter;

	KafkaProducer<String, SpecificRecord> kafkaProducerAVRO;
	KafkaProducer<String, String> kafkaProducerString;

	ProducerStatistics stats;

	/**
	 * Constructor instantiate a Producer (either String or AVRO)
	 * 
	 * @param cf KafkaClientFactory instance
	 * @param p  PrintWriter needed for printing (jcsOut in RMJ)
	 * @throws Exception generic exception
	 */
	public Producer(KafkaClientFactory cf, PrintWriter p) throws Exception {
		this(cf);
		this.printwriter = p;
	}

	/**
	 * Constructor instantiate a Producer (either String or AVRO)
	 * 
	 * @param cf KafkaClientFactory instance
	 * @throws Exception generic exception
	 */
	public Producer(KafkaClientFactory cf) throws Exception {
		this.kafkaClientFactory = cf;
		this.printwriter = cf.printwriter;
		
		switch (cf.getTypeDeSer()) {
		case STRINGSER:
			this.kafkaProducerString = new KafkaProducer<String, String>(cf.getProperties());
		case AVROSER:
			this.kafkaProducerAVRO = new KafkaProducer<String, SpecificRecord>(cf.getProperties());
		default:
			this.kafkaProducerString = new KafkaProducer<String, String>(cf.getProperties());
		}
	}

	/**
	 * publish a message on instance of class
	 * 
	 * @return This Producer (to allow chaining)
	 * @throws Exception generic exception
	 */
	public Producer publish() throws Exception {
		if (kafkaClientFactory.publishValue() instanceof SpecificRecord || kafkaClientFactory.publishValue() instanceof String) {
			publishSingleMessage();
		}

		if (KafkaUtil.isNotBlank(kafkaClientFactory.getJdbcQuery())) {
			publishFromRefCursor();
		}

		return this;
	}

	private void closeKafkaProducers() {
		if (kafkaProducerAVRO != null)
			kafkaProducerAVRO.close();
		if (kafkaProducerString != null)
			kafkaProducerString.close();
	}
	
	private Producer publishSingleMessage() throws Exception {
		if (kafkaClientFactory.publishValue() instanceof SpecificRecord) {
			this.recordMetadata = this.kafkaProducerAVRO
					.send(new ProducerRecord<String, SpecificRecord>(kafkaClientFactory.getTopic(), kafkaClientFactory.getKey(),
							(SpecificRecord) kafkaClientFactory.publishValue()))
					.get();
			//kafkaProducerAVRO.close();
		}
		if (kafkaClientFactory.publishValue() instanceof String) {
			this.recordMetadata = this.kafkaProducerString
					.send(new ProducerRecord<String, String>(kafkaClientFactory.getTopic(), kafkaClientFactory.getKey(),
							(String) kafkaClientFactory.publishValue()))
					.get();
			//kafkaProducerString.close();
		}
		this.closeKafkaProducers();
		
		return this;
	}

	private Producer publishWithCallback(String messageId) throws Exception {
		if (kafkaClientFactory.publishValue() instanceof SpecificRecord) {
			this.kafkaProducerAVRO.send(new ProducerRecord<String, SpecificRecord>(kafkaClientFactory.getTopic(),
					kafkaClientFactory.getKey(), (SpecificRecord) kafkaClientFactory.publishValue()),
					new ProducerCallback(messageId));
		}
		if (kafkaClientFactory.publishValue() instanceof String) {
			this.kafkaProducerString.send(new ProducerRecord<String, String>(kafkaClientFactory.getTopic(),
					kafkaClientFactory.getKey(), (String) kafkaClientFactory.publishValue()), new ProducerCallback(messageId));
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
			//kafkaClientFactory.print(this.messageId+" "+m.topic()+" "+m.partition()+" "+m.offset()+" "+m.timestamp());
			if (e != null) {
				print(String.format("Error messageId: %s, topic: %s, partition: %s, offset: %s, errormessage: %%s%n",
						this.messageId, m.topic(), m.partition(), m.offset(), e.toString()));
			}
		}
	}

	private Producer publishFromRefCursor() throws Exception {
		connection = kafkaClientFactory.jdbcConnection();
    
		CallableStatement stmt = this.connection.prepareCall("{ ? = call " + kafkaClientFactory.getJdbcQuery() + " }");
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

		connection.close();
		closeKafkaProducers();
		return this;
	}

	private void print(String s) {
		if (printwriter != null) {
			printwriter.println(s);
		}
	}

	/**
	 * For published (single) message, print a formatted list: topic, partition,
	 * offset, timestamp (utc epoch value)<br>
	 * metaData is empty for ref cursors. Use printErrors instead.<br>
	 * Assumed printWriter has been enabled in KafkaClientFactory
	 * 
	 * @return This Producer (to allow chaining)
	 */
	public Producer printMetadata() {
		return printMetadata(kafkaClientFactory.getPrintwriter());
	}

	/**
	 * For published (single) message, print a formatted list: topic, partition,
	 * offset, timestamp (utc epoch value)<br>
	 * metaData is empty for ref cursors. Use printErrors instead.
	 * 
	 * @param p PrintWriter
	 * @return This Producer (to allow chaining)
	 */
	public Producer printMetadata(PrintWriter p) {
		this.printwriter = p;
		if (this.recordMetadata != null) {
			print("==== Producer MetaData ====");
			print(String.format("topic: %s", this.recordMetadata.topic()));
		  print(String.format("partition: %s", this.recordMetadata.partition()));
		  print(String.format("offset: %s", this.recordMetadata.offset()));
		  print(String.format("timestamp: %s", this.recordMetadata.timestamp()));
		}  
		return this;
	}
  /**
   * not used yet
   * @param p
   * @return
   */
	//private Producer printStatistics(PrintWriter p) {
	//	return this;
	//}
	
}
