KafkaClientFactory with Consumer and Producer Wrappers

Provide an instance of class KafkaClientFactory to run a Consumer or a Producer.<br>
The KafkaClientFactory can be instantiated as POJO, for example map a JSON onto it.
- create an instance and set all needed values
- create an instance by mapping a json file on the class (jackson)
- create an instance from RMJ by using the ParameterMapper (see other project)


Examples of Instances:
Instance for a seek session
```
PrintWriter pw = new PrintWriter();
KafkaClientFactory cf = new KafakClientFactory()
                        .setBootstrapServers("host1:9095,host2:9095,host3:9095")
				                .setBootstrapServersCredentials(new Credentials("username","passwarod"))
				                .setTopic("myTopic")
				                .setValue("this is the message")
				                .setPrintwriter(pw)
				                .printProperties()
				                .printParameters();

new Producer(cf).publish().printMetadata();
``` 