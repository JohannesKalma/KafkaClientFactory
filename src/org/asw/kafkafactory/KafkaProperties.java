package org.asw.kafkafactory;

import java.util.Properties;

/**
 * 
 * public class KafkaProperties can be used as extension on other classes
 *
 */
public class KafkaProperties {

	Properties properties;

	/**
	 * get Properties
	 * 
	 * @return properties Properties
	 */
	public Properties getProperties() {
		return properties;
	}

	/**
	 * set Properties
	 * 
	 * @param properties Properties
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	/**
	 * constructor
	 */
	public KafkaProperties() {
		properties = new Properties();
	}

}
