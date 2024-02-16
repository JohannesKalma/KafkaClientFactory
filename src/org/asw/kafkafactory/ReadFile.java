package org.asw.kafkafactory;

import java.nio.file.Files;
import java.nio.file.Path;

public class ReadFile {

	String content;
	Path path;
	
	public ReadFile(String path) throws Exception {
    this.path = Path.of(path); 
	}

	public String getContent() throws Exception {
		return Files.readString(this.path);
	}
	
	public static void main(String[] args) throws Exception {
		String path = "topics/api.order.consignmentUpdate/api.order.consignmentUpdate.avsc";
    System.out.println(new ReadFile(path).getContent());
	}

}
