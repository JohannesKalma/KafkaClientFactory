## KafkaClientFactory Datamapper (DTO) for Kafka Consumers and Producers

Provide an instance of class KafkaClientFactory to run a Consumer or a Producer.<br>
The KafkaClientFactory can be instantiated as POJO, for example map a JSON onto it.
- create an instance and set all needed values
- create an instance by mapping a json file on the class (jackson)
- create an instance from RMJ by using the [ParameterMapper](https://github.com/JohannesKalma/RunMyJobsKafkaClientFactoryParameterMapper)

What can this factory be of help for
- the configuration of the bootstrap servers, with optional ssl connection.
- schema server AVRO verification.
- produce data from a oracle jdbc ref cursors,
- transfer consumer messages onto a oracle jdbc connection.
- seek a single message on a partition with offset
- create an avcs file (AVRO schema) from a json data file 
- build DTO classes from an avsc file (AVRO schema), compile the package into a jar 

For our project, the consumers and producers run from Redwood RunMyJobs, that uses it's own sandboxed Printwriter. In that case the jcsOut should be used instead of System.out as parameter for the setPrintWriter.

Examples of Instances:
Produce a message on a topic and print the propertie,parameters and metadata of the published message
```
PrintWriter pw = new PrintWriter(System.out,true);
KafkaClientFactory cf = new KafkaClientFactory().setBootstrapServers("host1:9095,host2:9095,host3:9095")
                                                .setBootstrapServersCredentials(new Credentials("username","passwarod"))
                                                .setTypeDeSer(typeDeSer.STRINGSER)
                                                .setTopic("myTopic")
                                                .setValue("this is the message")
                                                .setPrintwriter(pw)
                                                .printProperties()
                                                .printParameters();

new Producer(cf).publish().printMetadata();
```
This same producer can also be setup from a json. Here Jackson is needed to map the json on the values in an instantiated class.
```
String json = "{\r\n"
             + "  \"bootstrapServers\" : \"host1:9095,host2:9095,host3:9095\",\r\n"
             + "  \"bootstrapServersCredentials\" : {\r\n"
             + "    \"userName\" : \"username\",\r\n"
             + "    \"password\" : \"password\"\r\n"
             + "  },\r\n"
             + "  \"topic\" : \"myTopic\",\r\n"
             + "  \"value\" : \"this is the message\",\r\n
             + "  \"typeDeSer\" : \"STRINGSER\"\r\n
             + "}";

ObjectMapper mapper = new ObjectMapper();
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);		
new Producer((KafkaClientFactory)mapper.readValue(json, KafkaClientFactory.class).setPrintwriter(pw).printProperties().printParameters()).publish().printMetadata();
```

The result is a set of Kafka properties:
```
security.protocol: SASL_SSL
value.serializer: class org.apache.kafka.common.serialization.StringSerializer
sasl.mechanism: PLAIN
sasl.jaas.config: org.apache.kafka.common.security.plain.PlainLoginModule required username="username" password="passwarod";
acks: all
bootstrap.servers: host1:9095,host2:9095,host3:9095
key.serializer: class org.apache.kafka.common.serialization.StringSerializer
```

DTO variables that can be set :
- bootstrapServers - String (list of brokers)
- bootstrapServersCredentials - Credentials
- bootstrapServerTruststoreCertificate - String (the PEM Certificate Document)
- schemaRegistrycredentials - Credentials
- schemaRegistryURL - String
- typeDeSer - KafkaClientFactory.typeDeSer
- groupId - String
- topic - String
- className - String (DTO class needed for AVRO serializing)
- key - String
- value - String
- partition - String
- offset - String
- jdbcUrl - String
- jdbcCredentials - Credentials
- jdbcQuery - String

Johannes K
