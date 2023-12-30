KafkaClientFactory with Consumer and Producer Wrappers

Provide an instance of class KafkaClientFactory to run a Consumer or a Producer.<br>
The KafkaClientFactory can be instantiated as POJO, for example map a JSON onto it.
- create an instance and set all needed values
- create an instance by mapping a json file on the class (jackson)
- create an instance from RMJ by using the ParameterMapper (see other project)

For our project, the consumers and producers run from Redwood RunMyJobs, that uses it's own sandboxed Printwriter. In that case the jcsOut should be used as parameter for the setPrintWriter.

Examples of Instances:
Produce a message on a topic and print the propertie,parameters and metadata of the published message
```
PrintWriter pw = new PrintWriter(System.out,true);
KafkaClientFactory cf = new KafkaClientFactory().setBootstrapServers("host1:9095,host2:9095,host3:9095")
				                .setBootstrapServersCredentials(new Credentials("username","passwarod"))
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
             + "  \"value\" : \"this is the message\"\r\n
             + "}";

ObjectMapper mapper = new ObjectMapper();
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);		
new Producer((KafkaClientFactory)mapper.readValue(json, KafkaClientFactory.class).setPrintwriter(jcsOut).printProperties().printParameters()).publish().printMetadata();
```
