package org.asw.kafkafactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Credentials provides a Username and a Password<br>
 * It's a DTO for all KafkaClientFactory credentials<br>
 * DTO values for mapping:<br>
 * - userName {@link String}<br>
 * - passWord {@link String}<br>
 */
public class Credentials {

	private String userName;

	@JsonProperty
	private String password;

	/**
	 * Get the UserName. This method return a String containing the username when
	 * set.
	 * 
	 * @return The Username
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * Set the UserName
	 * 
	 * @param userName String
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}

	/**
	 * Get the password. This method returns a String containing the (unencrypted)
	 * password when set.
	 * 
	 * @return The password
	 */
	@JsonIgnore
	public String getPassword() {
		return password;
	}

	/**
	 * Set the Password
	 * 
	 * @param password String
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Constructor - Credentials()
	 */
	public Credentials() {
	}

	/**
	 * Constructor - Credentials(String userName, String password)
	 * <p>
	 * 
	 * Set the userName and the password when class is initialized
	 * 
	 * @param userName String
	 * @param password String
	 */
	public Credentials(String userName, String password) {
		this.userName = userName;
		this.password = password;
	}

}
