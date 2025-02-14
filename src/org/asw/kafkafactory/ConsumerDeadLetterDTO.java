package org.asw.kafkafactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;
/**
 * Deadletter DTO
 * @author jkalma
 *
 */
public class ConsumerDeadLetterDTO {

	/**
	 * getData return the kafka message
	 * 
	 * @return kafka message
	 */
	public String getData() {
		return data;
	}

	/**
	 * add kafka message to dto
	 * 
	 * @param data String the message
	 */
	public void setData(String data) {
		this.data = data;
	}

	/**
	 * getMetaData return metadata objec
	 * 
	 * @return ConsumerRecordMetaDataDTO object
	 */
	public ConsumerRecordMetaDataDTO getMetaData() {
		return metaData;
	}
	
	/**
	 * add kafka message metadata to dto
	 * 
	 * @param metaData ConsumerRecordMetaDataDTO instance
	 */
	public void setMetaData(ConsumerRecordMetaDataDTO metaData) {
		this.metaData = metaData;
	}

	String data;
	String errorMessage;
	
	/**
	 * getErrorMessage()
	 * 
	 * @return String errormessage
	 */
	public String getErrorMessage() {
		return errorMessage;
	}

	/**
	 * setErrorMessage()
	 * 
	 * @param errorMessage String
	 */
	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	ConsumerRecordMetaDataDTO metaData;

	/**
	 * Constructor
	 * 
	 * @param <V> inherit from ConsumerRecord
	 * @param record the kafka message
	 */
	public <V> ConsumerDeadLetterDTO(ConsumerRecord<String, V> record) {
    metaData = new ConsumerRecordMetaDataDTO(record);
    this.data = record.value().toString();
	}
	
	/**
	 * Constructor
	 * 
	 * @param <V> inherit from ConsumerRecord
	 * @param record the kafka message
	 */
	public <V> ConsumerDeadLetterDTO(ConsumerRecord<String, V> record,String error) {
    metaData = new ConsumerRecordMetaDataDTO(record);
    this.data = record.value().toString();
    this.errorMessage = error;
	}
	
	

}
