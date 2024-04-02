package org.asw.kafkafactory;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * ReadFile class
 * 
 * @author JKALMA 
 *
 */
public class ReadFile {

	String content;
	Path path;
	
	/**
	 * Read any file and set the path
	 * 
	 * @param path - full fill path
	 * @throws Exception - generic exception
	 */
	public ReadFile(String path) throws Exception {
    this.path = Path.of(path);
	}

	/**
	 * Return the content of a file as a String
	 * 
	 * @return String - the content of the file
	 * @throws Exception generic exception
	 */
	public String getContent() throws Exception {
		return Files.readString(this.path);
	}

}
