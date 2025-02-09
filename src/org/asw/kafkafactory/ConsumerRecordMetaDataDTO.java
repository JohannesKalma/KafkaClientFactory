package org.asw.kafkafactory;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author jkalma
 *
 */
public class ConsumerRecordMetaDataDTO {
		
		/**
		 * get the key value
		 * 
		 * @return the key String
		 */
		public String getKey() {
			return key;
		}

		/**
		 * set the key value
		 * 
		 * @param key String
		 */
		public void setKey(String key) {
			this.key = key;
		}

		/**
		 * get Offset value of message
		 * 
		 * @return Long value
		 */
		public Long getOffset() {
			return offset;
		}

		/**
		 * set offsetvalue message
		 * 
		 * @param offset Long
		 */
		public void setOffset(Long offset) {
			this.offset = offset;
		}

		/**
		 * partition of kafka message
		 * @return partition Integer
		 */
		public Integer getPartition() {
			return partition;
		}

		/**
		 * set partition value
		 * 
		 * @param partition Integer
		 */
		public void setPartition(Integer partition) {
			this.partition = partition;
		}

		/**
		 * message timestamp
		 * 
		 * @return timestamp Long
		 */
		public Long getTimestamp() {
			return timestamp;
		}

		/**
		 * set the timestamp
		 * 
		 * @param timestamp Long
		 */
		public void setTimestamp(Long timestamp) {
			this.timestamp = timestamp;
		}

		/**
		 * message topic
		 * 
		 * @return topic String
		 */
		public String getTopic() {
			return topic;
		}

		/**
		 * set topic
		 * 
		 * @param topic String
		 */
		public void setTopic(String topic) {
			this.topic = topic;
		}

		String key;
		Long offset;
		Integer partition;
		Long timestamp;
		String topic;
		
		/**
		 * constructor
		 * 
		 * @param <V> generic
		 * @param record kafka record
		 */
		public <V> ConsumerRecordMetaDataDTO(ConsumerRecord<String, V> record) {
			this.key = record.key();
			this.offset = record.offset();
			this.partition = record.partition();
			this.timestamp = record.timestamp();
      this.topic = record.topic();
		}
	}

