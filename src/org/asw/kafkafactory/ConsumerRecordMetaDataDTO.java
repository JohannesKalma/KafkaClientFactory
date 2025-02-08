package org.asw.kafkafactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerRecordMetaDataDTO {
		
		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public Long getOffset() {
			return offset;
		}

		public void setOffset(Long offset) {
			this.offset = offset;
		}

		public Integer getPartition() {
			return partition;
		}

		public void setPartition(Integer partition) {
			this.partition = partition;
		}

		public Long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		String key;
		Long offset;
		Integer partition;
		Long timestamp;
		String topic;

		/**
		 * @param <V>
		 * @param s
		 * @return 
		 */
		
		public <V> ConsumerRecordMetaDataDTO(ConsumerRecord<String, V> record) {
			this.key = record.key();
			this.offset = record.offset();
			this.partition = record.partition();
			this.timestamp = record.timestamp();
      this.topic = record.topic();
		}
	}

