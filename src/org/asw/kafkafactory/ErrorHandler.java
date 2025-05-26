package org.asw.kafkafactory;

public class ErrorHandler {

	private Integer errorCount;
	private Integer maxErrorCount;
	private KafkaClientFactory kafkaClientFactory;
	
	public ErrorHandler(KafkaClientFactory kafkaClientFactory) throws Exception {
		this.kafkaClientFactory = kafkaClientFactory;
		this.errorCount = 0;
		if (KafkaUtil.isBlank(this.kafkaClientFactory.getMaxErrorCount())) {
			this.maxErrorCount = 100;
		} else {
			try {
			  this.maxErrorCount = Integer.parseInt(kafkaClientFactory.getMaxErrorCount());
			} catch (Exception e) {
				throw new Exception(String.format("maxErrorCount should be an integer"));
			}
		}
	}

	public Integer getErrorCount() {
		return errorCount;
	}

	public void setErrorCount(Integer errorCount) {
		this.errorCount = errorCount;
	}

	public Integer getMaxErrorCount() {
		return maxErrorCount;
	}

	public void setMaxErrorCount(Integer maxErrorCount) {
		this.maxErrorCount = maxErrorCount;
	}

	public KafkaClientFactory getKafkaClientFactory() {
		return kafkaClientFactory;
	}

	public void setKafkaClientFactory(KafkaClientFactory kafkaClientFactory) {
		this.kafkaClientFactory = kafkaClientFactory;
	}

	
	
	
}
