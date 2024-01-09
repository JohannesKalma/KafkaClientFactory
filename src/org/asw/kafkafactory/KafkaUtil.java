package org.asw.kafkafactory;

/**
 * Static Utilities<br>
 * Copied from org/apache/commons/lang3/StringUtils.java<br>
 * https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/StringUtils.java#L5281<br>
 *
 */
public interface KafkaUtil {

	/**
	 * Copied from org/apache/commons/lang3/StringUtils.java<br>
	 * https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/StringUtils.java#L5281<br>
	 * <br>
	 * Gets a CharSequence length or {@code 0} if the CharSequence is {@code null}.
	 *
	 * @param cs a CharSequence or {@code null}
	 * @return CharSequence length or {@code 0} if the CharSequence is {@code null}.
	 * @since 2.4
	 * @since 3.0 Changed signature from length(String) to length(CharSequence)
	 */
	public static int length(final CharSequence cs) {
		return cs == null ? 0 : cs.length();
	}

	/**
	 * Copied from org/apache/commons/lang3/StringUtils.java<br>
	 * https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/StringUtils.java#L5281<br>
	 * <br>
	 * Checks if a CharSequence is empty (""), null or whitespace only.
	 *
	 * <p>
	 * Whitespace is defined by {@link Character#isWhitespace(char)}.
	 * </p>
	 *
	 * <pre>
	 * StringUtils.isBlank(null)      = true
	 * StringUtils.isBlank("")        = true
	 * StringUtils.isBlank(" ")       = true
	 * StringUtils.isBlank("bob")     = false
	 * StringUtils.isBlank("  bob  ") = false
	 * </pre>
	 *
	 * @param cs the CharSequence to check, may be null
	 * @return {@code true} if the CharSequence is null, empty or whitespace only
	 * @since 2.0
	 * @since 3.0 Changed signature from isBlank(String) to isBlank(CharSequence)
	 */
	public static boolean isBlank(final CharSequence cs) {
		final int strLen = length(cs);
		if (strLen == 0) {
			return true;
		}
		for (int i = 0; i < strLen; i++) {
			if (!Character.isWhitespace(cs.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Copied from org/apache/commons/lang3/StringUtils.java<br>
	 * https://github.com/apache/commons-lang/blob/master/src/main/java/org/apache/commons/lang3/StringUtils.java#L5281<br>
	 * <br>
	 * Checks if a CharSequence is not empty (""), not null and not whitespace only.
	 *
	 * <p>
	 * Whitespace is defined by {@link Character#isWhitespace(char)}.
	 * </p>
	 *
	 * <pre>
	 * StringUtils.isNotBlank(null)      = false
	 * StringUtils.isNotBlank("")        = false
	 * StringUtils.isNotBlank(" ")       = false
	 * StringUtils.isNotBlank("bob")     = true
	 * StringUtils.isNotBlank("  bob  ") = true
	 * </pre>
	 *
	 * @param cs the CharSequence to check, may be null
	 * @return {@code true} if the CharSequence is not empty and not null and not
	 *         whitespace only
	 * @since 2.0
	 * @since 3.0 Changed signature from isNotBlank(String) to
	 *        isNotBlank(CharSequence)
	 */
	public static boolean isNotBlank(final CharSequence cs) {
		return !isBlank(cs);
	}

}
