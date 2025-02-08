package org.asw.kafkafactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerDeadLetterDTO {

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public ConsumerRecordMetaDataDTO getMetaData() {
		return metaData;
	}

	public void setMetaData(ConsumerRecordMetaDataDTO metaData) {
		this.metaData = metaData;
	}

	String data;
	ConsumerRecordMetaDataDTO metaData;
	
	public <V> ConsumerDeadLetterDTO(ConsumerRecord<String, V> record) {
    metaData = new ConsumerRecordMetaDataDTO(record);
    this.data = record.value().toString();
	}

}
