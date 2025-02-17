package org.asw.kafkafactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

/**
 * DTO for all subscribers/publishers.<br>
 * It contains all values needed for the connection to any kafka server (for
 * example the bootstrapServer for a list of brokers).<br>
 *
 * Advice is to <i>not</i> explicitly set the values, but instead use an
 * ObjectMapper (like Jackson) and then only configure it with those values that
 * are needed for your subscriber or consumer.<br>
 * One such mapper is the JobParameterMap, that can be used as bridge for
 * RunMyJobs scheduled Kafka processes<br>
 * <br>
 * Examples of usages: - When the message is String serialized, then there is no
 * need to use a schema server. In that case just leave out about the
 * schemaRegistryURL and schemaRegistryCredentials.<br>
 * - When the message needs some AVRO serializing, then the schemaRegistryUrl ,
 * Credential and className for the (de)serialzing are interesting
 * variables.<br>
 * - For a subscriber the value is not needed instead the 3 jdbc values might be
 * of interest, to configure a jdbc class that can be used to send a formatted
 * jdbcQuery.<br>
 * <br>
 * The typeDeSer is mandatory, to tell the client what kind of (de)serializer
 * must be used.<br>
 * DTO fields that can be used for mapping:<br>
 * - bootstrapServers {@link String} (list of brokers)<br>
 * - bootstrapServersCredentials {@link Credentials}<br>
 * - bootstrapServerTruststoreCertificate {@link String} (the PEM Certificate
 * Document)<br>
 * - schemaRegistryCredentials {@link Credentials}<br>
 * - schemaRegistryURL {@link String}<br>
 * - typeDeSer {@link typeDeSer} - groupId {@link String}<br>
 * - topic {@link String}<br>
 * - className {@link String} (DTO class needed for AVRO (de)serializing)<br>
 * - key {@link String}<br>
 * - value {@link String}<br>
 * - partition {@link String}<br>
 * - offset {@link String}<br>
 * - jdbcUrl {@link String}<br>
 * - jdbcCredentials {@link Credentials}<br>
 * - jdbcQuery String<br>
 */
public class KafkaClientFactory {
	//public
	private String bootstrapServers;
	private Credentials bootstrapServersCredentials;
	private String bootstrapServerTruststoreCertificate;
	private String groupId;
	private String schemaRegistryURL;
	private Credentials schemaRegistryCredentials;
	private String topic;
	private String className;
	private String partition;
	private String offset;
	private String value;
	private String key;
	private typeDeSer typeDeSer;
	private String jdbcUrl;
	private Credentials jdbcCredentials;
	private String jdbcQuery;
	private String maxErrorCount;
	private String duration;
	private yesNo printProperties;
	private yesNo printParameters;
	private yesNo printInfo;
	
	//private
	//private SpecificRecord specificRecord;
	//protected PrintWriter printwriter;
	protected PrintWriter deadLetterPrintWriter;
	protected PrintWriter kafkaProcessLogPrintWriter;
	protected Connection jdbcConnection;
	
	public yesNo getPrintProperties() {
		return printProperties;
	}

	public void setPrintProperties(yesNo printProperties) {
		this.printProperties = printProperties;
	}

	public yesNo getPrintParameters() {
		return printParameters;
	}

	public void setPrintParameters(yesNo printParameters) {
		this.printParameters = printParameters;
	}
	
	public yesNo getPrintInfo() {
		return printInfo;
	}

	public void setPrintInfo(yesNo printInfo) {
		this.printInfo = printInfo;
	}
	
	/**
	 * get consumer duration in ISO 8601 format
	 * 
	 * @return String value
	 */
	public String getDuration() {
		return duration;
	}

	/**
	 * set consumer duration in ISO 8601 format
	 * 
	 * @param duration String
	 */
	public void setDuration(String duration) {
		this.duration = duration;
	}

	/**
	 * get maxErrorCount
	 * 
	 * @return String value
	 */
	public String getMaxErrorCount() {
		return maxErrorCount;
	}

	/**
	 * set maxError count, number of errors before consumer or producer quits.
	 * 
	 * @param maxErrorCount String
	 */
	public void setMaxErrorCount(String maxErrorCount) {
		this.maxErrorCount = maxErrorCount;
	}

	///**
	// * get instance of PrintWriter
	// * 
	// * @return instance of PrintWriter
	// */
	//protected PrintWriter getPrintwriter() {
	//	return printwriter;
	//}

	/**
	 * set PrintWriter where to write the output (jcsOut for RMJ, System.out for
	 * java)
	 * 
	 * @param printwriter where to write the output (jcsOut for RMJ, System.out for
	 *                    java)
	 * @return This KafkaClientFactory (to allow chaining)
	 */
	public KafkaClientFactory setPrintwriter(PrintWriter printwriter) {
		//this.printwriter = printwriter;
	  this.kafkaProcessLogPrintWriter.println("This printwriter is deprecated.");
		return this;
	}
	
	protected PrintWriter getDeadLetterPrintWriter() {
		return deadLetterPrintWriter;
	}
  
	/**
	 * set the deadLetter Printwriter (needed for RMJ implementation)
	 * 
	 * @param deadLetterPrintWriter Printwriter object
	 */
	public void setDeadLetterPrintWriter(PrintWriter deadLetterPrintWriter) {
		this.deadLetterPrintWriter = deadLetterPrintWriter;
	}

	protected PrintWriter getKafkaProcessLogPrintWriter() {
		return kafkaProcessLogPrintWriter;
	}

  /**
   * set kafkaClientFactory Process Log Printwriter (needed for RMJ implementation)
   * @param kafkaProcessLogPrintWriter object
   */
	public void setKafkaProcessLogPrintWriter(PrintWriter kafkaProcessLogPrintWriter) {
		this.kafkaProcessLogPrintWriter = kafkaProcessLogPrintWriter;
	}

	/**
	 * enum typeDeSer
	 * 
	 */
	public enum typeDeSer {
		/**
		 * AVROSER producer message should be AVRO serialized
		 */
		AVROSER,
		/**
		 * AVROSER consumer return value should be AVRO deserialized
		 */
		AVRODES,
		/**
		 * STRINGSER producer message should be plain String serialized
		 */
		STRINGSER,
		/**
		 * STRINGDES consumer return value is plain String format
		 */
		STRINGDES;
	}
	
	/**
	 * enum YesNo
	 */
	public enum yesNo {
		/**
		 * Y yes
		 */
		Y,
		/**
		 * N no
		 */
		N
		}

	/**
	 * get typeDeSer()<br>
	 * 
	 * @return typeDeSer
	 */
	public typeDeSer getTypeDeSer() {
		return typeDeSer;
	}

	/**
	 * set typeDeSer<br>
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param typeDeSer enum typeDeSer
	 */
	public KafkaClientFactory setTypeDeSer(typeDeSer typeDeSer) {
		this.typeDeSer = typeDeSer;
		return this;
	}

	/**
	 * get schemaRegistryURL<br>
	 * 
	 * @return schemaRegistryURL String
	 */
	public String getSchemaRegistryURL() {
		return schemaRegistryURL;
	}

	/**
	 * set schemaRegistryURL<br>
	 * 
	 * @return instance of KafkaClientFactory
	 * @param schemaRegistryURL String
	 */
	public KafkaClientFactory setSchemaRegistryURL(String schemaRegistryURL) {
		this.schemaRegistryURL = schemaRegistryURL;
		return this;
	}

	/**
	 * get instance of SchemaRegistryCredentials<br>
	 * 
	 * @return schemaRegistryCredentials String
	 */
	public Credentials getSchemaRegistryCredentials() {
		return schemaRegistryCredentials;
	}

	/**
	 * set object schemaRegistryCredentials
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param schemaRegistryCredentials Credentials
	 */
	public KafkaClientFactory setSchemaRegistryCredentials(Credentials schemaRegistryCredentials) {
		this.schemaRegistryCredentials = schemaRegistryCredentials;
		return this;
	}

	/**
	 * get groupId()<br>
	 * 
	 * @return groupId String
	 */
	public String getGroupId() {
		return groupId;
	}

	/**
	 * set groupId<br>
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param groupId String
	 */
	public KafkaClientFactory setGroupId(String groupId) {
		this.groupId = groupId;
		return this;
	}

	/**
	 * get bootstrapServers (a list of brokers)<br>
	 * 
	 * @return bootstrapServers String
	 */
	public String getBootstrapServers() {
		return bootstrapServers;
	}

	/**
	 * set bootstrapServers (a list of brokers)<br>
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param bootstrapServers STring
	 */
	public KafkaClientFactory setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		return this;
	}

	/**
	 * get instance of bootstrapServersCredentials()
	 * 
	 * @return bootstrapServersCredentials Credentials
	 */
	public Credentials getBootstrapServersCredentials() {
		return bootstrapServersCredentials;
	}

	/**
	 * set instance of bootstrapServersCredentials<br>
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param bootstrapServersCredentials Credentials
	 */
	public KafkaClientFactory setBootstrapServersCredentials(Credentials bootstrapServersCredentials) {
		this.bootstrapServersCredentials = bootstrapServersCredentials;
		return this;
	}

	/**
	 * get bootstrapServerTruststoreCertificate<br>
	 * <br>
	 * This is also known as the PEM Certificate Document
	 * 
	 * @return bootstrapServerTruststoreCertificate String
	 */
	public String getBootstrapServerTruststoreCertificate() {
		return bootstrapServerTruststoreCertificate;
	}

	/**
	 * get bootstrapServerTruststoreCertificate<br>
	 * <br>
	 * This is also known as the PEM Certificate Document
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param bootstrapServerTruststoreCertificate String
	 */
	public KafkaClientFactory setBootstrapServerTruststoreCertificate(String bootstrapServerTruststoreCertificate) {
		this.bootstrapServerTruststoreCertificate = bootstrapServerTruststoreCertificate;
		return this;
	}

	/**
	 * get Kafka topic<br>
	 * 
	 * @return topic String
	 */
	public String getTopic() {
		return topic;
	}

	/**
	 * set Kafka topic<br>
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param topic String
	 */
	public KafkaClientFactory setTopic(String topic) {
		this.topic = topic;
		return this;
	}

	/**
	 * get className<br>
	 * The DTO needed for AVRO (de)serializing
	 * 
	 * @return topic String
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * set className<br>
	 * The DTO needed for AVRO (de)serializing
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param className String
	 */
	public KafkaClientFactory setClassName(String className) {
		this.className = className;
		return this;
	}

	/**
	 * get partition()<br>
	 * partition needed to seek a message
	 * 
	 * @return partition String
	 */
	public String getPartition() {
		return partition;
	}

	/**
	 * set partition<br>
	 * partition needed to seek a message
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param partition String
	 */
	public KafkaClientFactory setPartition(String partition) {
		this.partition = partition;
		return this;
	}

	/**
	 * get offset()<br>
	 * offset needed to seek a message
	 * 
	 * @return offset String
	 */
	public String getOffset() {
		return offset;
	}

	/**
	 * set offset offset needed to seek a message
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param offset String
	 */
	public KafkaClientFactory setOffset(String offset) {
		this.offset = offset;
		return this;
	}

	/**
	 * get value<br>
	 * value parsed as Kafka message
	 * 
	 * @return value String
	 */
	public String getValue() {
		return value;
	}

	/**
	 * set value<br>
	 * value parsed as Kafka message
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param value String
	 */
	public KafkaClientFactory setValue(String value) {
		this.value = value;
		return this;
	}

	/**
	 * get key()<br> 
	 * According to Kafka documentation, optional, but in this lib
	 * mandatory
	 * 
	 * @return key String
	 */
	public String getKey() {
		return key;
	}

	/**
	 * set key<br> 
	 * According to Kafka documentation, optional, but in this lib
	 * mandatory
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param key String
	 */
	public KafkaClientFactory setKey(String key) {
		this.key = key;
		return this;
	}

	/**
	 * get jdbcUrl<br>
	 * For dataprocessing
	 * 
	 * @return jdbcUrl String
	 */
	public String getJdbcUrl() {
		return jdbcUrl;
	}

	/**
	 * set jdbcUrl<br>
	 * For dataprocessing (consumer) and dataretreiving (ref cursor for producer)<br>
	 * must have the format jdbc:oracle:thin:@[host]:[port]:[sid]<br>
	 * For example: jdbc:oracle:thin:@//myhost.us.example.com:1521/devdb
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param jdbcUrl String
	 */
	public KafkaClientFactory setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
		return this;
	}

	/**
	 * get object of jdbcCredentials<br>
	 * For dataprocessing (consumer) and dataretreiving (ref cursor for producer)
	 * 
	 * @return jdbcCredentials Credentials
	 */
	public Credentials getJdbcCredentials() {
		return jdbcCredentials;
	}

	/**
	 * set object of jdbcCredentials<br>
	 * For dataprocessing
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param jdbcCredentials Credentials
	 */
	public KafkaClientFactory setJdbcCredentials(Credentials jdbcCredentials) {
		this.jdbcCredentials = jdbcCredentials;
		return this;
	}
	
	/*
	 * Jackson DTO mapper fails on this one. 
	 *public KafkaClientFactory setJdbcCredentials(String jdbcCredentialsJson) {
		ObjectMapper objectMapper = new ObjectMapper();
		this.jdbcCredentials = objectMapper.readValue(jdbcCredentialsJson, Credentials.class);
		return this;
	}*/
	
	/**
	 * get jdbcQuery()<br>
	 * For dataprocessing
	 * 
	 * @return jdbcQuery String
	 */
	public String getJdbcQuery() {
		return jdbcQuery;
	}

	/**
	 * set jdbcQuery<br>
	 * For dataprocessing
	 * 
	 * <p>
	 * for subscriber:<br>
	 * - [[PACKAGE.]PROCEDURE_NAME(param1=>'abcd',param2=>'?')]: 1 parameter for the
	 * return value of the topic
	 * </p>
	 * 
	 * <p>
	 * for the producer (ref cursor):<br>
	 * - for a producer, a function returning a ref cursor is expected [FUNCTION_NAME returning a ref
	 * cursor]<br>
	 * the select statement of this ref cursor must have the form: SELECT id,topic,key,value FROM ... WHERE ....<br>
	 * <strong>id</strong> is an identifier for your query needed for the callback of the published message<br>
	 * <strong>topic</strong> and key will be the identifiers where message will be published<br>
	 * <strong>message</strong> is the message that will be published on a topic. 
	 * </p>
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @param jdbcQuery String
	 */
	public KafkaClientFactory setJdbcQuery(String jdbcQuery) {
		//this.jdbcQuery = String.format("BEGIN %s; END;", jdbcQuery);
		this.jdbcQuery = jdbcQuery;
		return this;
	}

	
	/**
	 * set a JDBC connection for factory url and credentials<br>
	 * result is a Connection value
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 * @throws ClassNotFoundException --
	 * @throws SQLException           --
	 */	
	public KafkaClientFactory setJdbcConnection() throws ClassNotFoundException, SQLException {
		if (KafkaUtil.isNotBlank(getJdbcUrl())) {
			//jdbcUrl : jdbc:oracle:thin:@//ams-ibm-ds10.nl.aswatson.net:1521/gitaswo
			if (getJdbcUrl().trim().startsWith("jdbc:oracle")){
  			setOracleJdbcConnection();
			}
			if (getJdbcUrl().trim().startsWith("jdbc:mysql")) {
				setMysqlJdbcConnection();
			}
		}
		
		print("Connection " + jdbcConnection.toString());
		printDatabaseMetaData();
		return this;
	}

	private KafkaClientFactory setOracleJdbcConnection() throws ClassNotFoundException, SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		this.jdbcConnection = DriverManager.getConnection(getJdbcUrl(), getJdbcCredentials().getUserName(),
				getJdbcCredentials().getPassword());
		return this;
	}
	
	private KafkaClientFactory setMysqlJdbcConnection() throws ClassNotFoundException, SQLException {
    Class.forName("com.mysql.cj.jdbc.Driver");
		this.jdbcConnection = DriverManager.getConnection(getJdbcUrl(), getJdbcCredentials().getUserName(),
				getJdbcCredentials().getPassword());
		return this;
	}	

	/**
	 * if set, return a jdbc connection.<br>
	 * when not set, set the connection (@link #setJdbcConnection())
	 * 
	 * @return Connection a jdbc connection
	 * 
	 * @throws ClassNotFoundException only needed for the setJdbcConnection
	 * @throws SQLException           only needed for the setJdbcConnection
	 */
	public Connection jdbcConnection() throws ClassNotFoundException, SQLException {
		if (this.jdbcConnection == null) {
			setJdbcConnection();
		}
		return this.jdbcConnection;
	}

	/**
	 * close an open jdbc connection
	 * 
	 * @throws SQLException --
	 */
	public void closeJdbcConnection() throws SQLException {
		if (this.jdbcConnection != null) {
			this.jdbcConnection.close();
			print("jdbcConnection closed!");
		}
	}

	private void printDatabaseMetaData() throws SQLException {
		DatabaseMetaData dbmd = jdbcConnection.getMetaData();
		print("==== JDBC info ====");
		print(String.format("Driver Name: %s", dbmd.getDriverName()));
		print(String.format("Driver Version:  %s", dbmd.getDriverVersion()));
		print(String.format("Database Username:  %s", dbmd.getUserName()));
	}

	/**
	 * This in not part of the DTO, it generates a SpecificRecord based on
	 * KafkaClientFactory DTO values
	 * 
	 * @return SpecificRecord needed for AVRO (de)serializing. className DTO Mapping
	 *         of both value on className from KafkaClientFactory DTO.
	 * @throws Exception generic exception
	 */
	public SpecificRecord specificRecord() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		return (SpecificRecord) mapper.readValue(getValue(), Class.forName(getClassName()));
	}

	/**
	 * This is not part of the DTO, it's being called from the publisher<br>
	 * When publisher is in AVROSER mode, then return a specificRecord object else the value as String.<br>
	 * This method is not for general use.
	 * 
	 * @return if AVROSER object of type {@link #specificRecord()} else Object of
	 *         type {@link #getValue()} {@link String}
	 * @throws Exception generic exception
	 */
	public Object publishValue() throws Exception {
		switch (getTypeDeSer()) {
		case AVROSER:
			return specificRecord();
		default:
			return getValue();
		}
	}

	/**
	 * This is not part of the DTO, it's the result of the KafkaClientFactory DTO
	 * values needed for instantiating a Kafka client.
	 * 
	 * @return properties for KafkaClient
	 */
	public Properties getProperties() {
		Properties properties = new Properties();

		if (KafkaUtil.isNotBlank(this.getBootstrapServers())) {
			properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
		}

		if (this.getBootstrapServersCredentials() != null) {
			// SASL_SSL
			properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
			properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
			properties.put(SaslConfigs.SASL_JAAS_CONFIG, String.format(
					"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
					this.getBootstrapServersCredentials().getUserName(), this.getBootstrapServersCredentials().getPassword()));
			// SSL PemCertification
			if (this.getBootstrapServerTruststoreCertificate() != null) {
				properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
				properties.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, this.getBootstrapServerTruststoreCertificate());
			}
		}

		if (KafkaUtil.isNotBlank(this.getGroupId()))
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.getGroupId());
		
		/*if (getTypeDeSer() != null) {
			switch (getTypeDeSer()) {
			case AVROSER:
			case STRINGSER:
				properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
				//if (KafkaUtil.isNotBlank(this.getJdbcQuery())) {
				//	properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
				//}
				break;
				
			default:
				//
			}
		}*/
		
		if (getTypeDeSer() != null) {
			switch (getTypeDeSer()) {
			case AVROSER:
				properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
				properties.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
				properties.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION,true);
				properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
				//properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
				properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
				break;
			case AVRODES:
				properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
				properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
				properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
				break;
			case STRINGSER:
				properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
				properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
				properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
				break;
			case STRINGDES:
				properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
				properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
				properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
				break;
			default:
				//
			}
		}

		if (KafkaUtil.isNotBlank(this.getSchemaRegistryURL())) {
			properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryURL());
			if (this.getSchemaRegistryCredentials() != null) {
				properties.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
				properties.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, String.format("%s:%s",
						this.getSchemaRegistryCredentials().getUserName(), this.getSchemaRegistryCredentials().getPassword()));
			}
		}

		return properties;
	}

	/**
	 * Print generated properties for an instantiated KafkaClientFactory:<br>
	 * initialize printwriter (setPrintWriter) is assumed
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 */
	public KafkaClientFactory printProperties() {
		return printProperties(this.getKafkaProcessLogPrintWriter());
	}
  
	private String obfuscatePrintPropertiesValue(String key,String value) {
		//String[] obfuscateKeys = {"ssl.truststore.certificates", "basic.auth.user.info", "ssl.truststore.certificates","sasl.jaas.config"};
		String[] obfuscateKeys = {};
		String obfuscatedValue = value;
		boolean isCredentialParameter = Arrays.stream(obfuscateKeys).anyMatch(s -> s.equals(key));
		if (isCredentialParameter) {
  		obfuscatedValue = "Obfuscated Property Value";
    }
		return obfuscatedValue;
	}
	
	/**
	 * Print generated properties for an instantiated KafkaClientFactory:<br>
	 * 
	 * @param p PrintWriter
	 * @return This KafkaClientFactory (to allow chaining)
	 */
	public KafkaClientFactory printProperties(PrintWriter p) {
		print("==== Kafka Client Properties ==== ");
		Enumeration<Object> keys = getProperties().keys();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = "non printable value";
			try {
				value = (String) getProperties().get(key).toString();
				value = obfuscatePrintPropertiesValue(key,value);
			} catch (Exception e) {
			}
			print(String.format("%s: %s", key, value));
		}
		return this;
	}
	
	protected void printInit() {
		
		if (this.getPrintParameters() != null && yesNo.Y.equals(this.getPrintParameters())) {
				this.printParameters();
			}

		if (this.getPrintProperties() != null && yesNo.Y.equals(this.getPrintProperties())) {
				this.printProperties();
		}

		if (this.getPrintInfo() != null && yesNo.Y.equals(this.getPrintInfo())) {
				this.printInfo();
		}
		
	}

	/**
	 * Print a string to the PrintWriter
	 * 
	 * @param m String
	 */
	public void print(String m) {
		this.kafkaProcessLogPrintWriter.println(m);
	}
	
	/**
	 * Print a string to the KafkaProcessLogPrintWriter
	 * 
	 * @param m String
	 */
	public void printLog(String m) {
		this.kafkaProcessLogPrintWriter.println(m);
	}
	
	/**
	 * Print a string to the DeadLetterPrintWriter
	 * 
	 * @param m String
	 */
	public void printDL(String m) {
		this.deadLetterPrintWriter.println(m);		
	}
	
	/**
	 * Print a key value string to the PrintWriter
	 * 
	 * @param k key String
	 * @param v value String
	 */
	public void printkv(String k,String v) {
		if (KafkaUtil.isNotBlank(v))
			this.kafkaProcessLogPrintWriter.println(String.format("%s: %s%n",k,v));
		  //this.printwriter.printf("%s: %s%n",k,v);
	}

	/**
	 * Print all DTO values for an instantiated KafkaClientFactory:<br>
	 * Method prints all get methods<br>
	 * Assumed that printWriter is initialized
	 * 
	 * @return This KafkaClientFactory (to allow chaining)
	 */
	public KafkaClientFactory printParameters() {
		return printParameters(this.getKafkaProcessLogPrintWriter());
	}

	/**
	 * Print all DTO values for an instantiated KafkaClientFactory:<br>
	 * Method prints all get methods
	 * 
	 * @param p PrintWriter
	 * @return This KafkaClientFactory (to allow chaining)
	 */
	public KafkaClientFactory printParameters(PrintWriter p) {
		print("==== Mapped Parameters ====");
		ObjectMapper mapper = new ObjectMapper();
		try {
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
			print(mapper.writer().withDefaultPrettyPrinter().writeValueAsString(this));
		} catch (JsonProcessingException e) {
			print(String.format("Parameter Print Error: %s%n", e.toString()));
		}

		return this;
	}
	
	/**
	 * Print (obfuscated) values to printwriter
	 * @return This KafkaClientFactory (to allow chaining)
	 */
	public KafkaClientFactory printInfo(){
		print("==== Info ====");
		printkv("bootstrapServers",this.getBootstrapServers());
		if (this.getBootstrapServersCredentials() != null) {
		  printkv("bootstrapServersCredentials [userName only]",this.getBootstrapServersCredentials().getUserName());
		}
		printkv("groupId",this.getGroupId());
		printkv("schemaRegistryURL",this.getSchemaRegistryURL());
		if (this.getSchemaRegistryCredentials() != null) {
			printkv("schemaRegistryCredentials [userName only]",this.getSchemaRegistryCredentials().getUserName());
		}
		printkv("topic",this.getTopic());
		printkv("className",this.getClassName());
		printkv("partition",this.getPartition());
		printkv("offset",this.getOffset());
		printkv("value",this.getValue());
		printkv("key",this.getKey());
		String typeDeSer = "";
    if (this.getTypeDeSer() != null) {
		  typeDeSer=this.getTypeDeSer().toString();
    }
    printkv("typeDeSer",typeDeSer);
		printkv("jdbcUrl",this.getJdbcUrl());
		if (this.getJdbcCredentials() != null) {
		  printkv("jdbcCredentials [userName only]",this.getJdbcCredentials().getUserName());
		}
		printkv("jdbcQuery",this.getJdbcQuery());
		printProperties();

		return this;
	}

	/**
	 * constructor
	 */
	public KafkaClientFactory() {
		//this.printwriter = new PrintWriter(System.out,true);
		this.deadLetterPrintWriter = new PrintWriter(System.out,true);
		this.kafkaProcessLogPrintWriter = new PrintWriter(System.out,true);
		this.setPrintInfo(yesNo.N);
		this.setPrintParameters(yesNo.N);
		this.setPrintProperties(yesNo.N);
	}
	
	/** 
	 * deprecated constructor 
	 * */
	//public KafkaClientFactory(PrintWriter printwriter) {
	//	this.printwriter = printwriter;
	//}

}