package org.asw.kafkafactory;

/**
 * Wrapper on ConsumerGeneric to initiate it implicitly with String type<br> 
 * For some reason the String Type also works for AVRO deserializer...<br>
 * At least some readable json is returned.<br>
 * For now: It works.
 */
public class Consumer extends ConsumerGeneric<String> {
	/**
	 * Initiate the generic consumer 
	 * @param cf KafkaClientFactory instance
	 * @throws Exception generic 
	 */
	public Consumer(KafkaClientFactory cf) throws Exception {
		super(cf);
		// TODO Auto-generated constructor stub
	}

}
