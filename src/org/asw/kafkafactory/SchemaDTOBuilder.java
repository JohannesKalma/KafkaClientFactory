package org.asw.kafkafactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.lang.model.SourceVersion;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.avro.*;
//import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 * Tools to build a DTO jar from a topic linked schema from a schema server.<br>
 * usage:<br>
 * new SchemaDTOBuilder().setSchema(schema).buildDTOsrc();<br>
 * Where schema is a string. The source builder uses the apache Schema class for conversion<br> 
 * It expects a name and namespace for filecreation.
 *
 */
public class SchemaDTOBuilder {

	KafkaClientFactory cf;
	public KafkaClientFactory getKafkaClientFactory() {
		return cf;
	}

	public void setKafkaClientFactory(KafkaClientFactory cf) {
		this.cf = cf;
	}

	SchemaRegistryClient schemaRegistryClient;
	Properties prop;
	String topic;
	File srcDir;
	String baseDirString;
	File baseDir;
	String schemaName;
	String namespace;
  PrintWriter printwriter;
  Schema schema;
	
  /**
   * Topic from schedulingserver the DTO should be based on
   * @param topic - topic name
   * @return This SchemaDTOBuilder (for chaining) 
   */
	public SchemaDTOBuilder setTopic(String topic) {
		this.topic = topic;
		return this;
	}
  
	/**
	 * constructor
	 */
	public SchemaDTOBuilder() {
	}

	/**
	 * initiate a client to the schemaRegistry server
	 * 
	 * Instantiate the KafkaClientFactory with:<br>
	 * the schemaRegistryURL<br>
	 * the schemaRegistrycredentials {@link Credentials}<br>
   * and a topic<br>
	 * 
	 * @param kafkaClientFactory - instance of KafkaClientFactory 
   * @return This SchemaDTOBuilder (for chaining) 
	 */
	public SchemaDTOBuilder schemaRegistryClient(KafkaClientFactory kafkaClientFactory) {
		this.cf = kafkaClientFactory;
		this.prop = cf.getProperties();
		this.topic = cf.getTopic();
		this.printwriter = cf.getPrintwriter();
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Map<String, String> propMap = (Map) prop;
		schemaRegistryClient = new CachedSchemaRegistryClient((String) prop.get("schema.registry.url"), 10000, propMap);

		return this;
	}

	private void print(String s) {
		if (cf != null && cf.printwriter != null) {
			cf.print(s);
		} else {
			System.out.println(s);
		}
	}
	
	private SchemaMetadata getSchemaMetaData(String subject) throws Exception {
		return this.schemaRegistryClient.getLatestSchemaMetadata(subject);
	}
	
	/**
	 * get the schema from latest topic schema metadata
   * @return This SchemaDTOBuilder (for chaining) 
	 * @throws Exception generic Exception
	 */
	public SchemaDTOBuilder setSchemaFromTopic() throws Exception {
		//List<String> topicList = new ArrayList<>();
		//topicList.add(this.topic + "-value");
    String topic = this.topic + "-value";
    String schema = this.getSchemaMetaData(topic).getSchema();
    this.setSchema(schema);
    
		//for (String topic : topicList) {
		//	this.schema = this.schemaRegistryClient.getLatestSchemaMetadata(topic).getSchema();
		//	break;
		//}
    return this;
	}
	
	
	/**
	 * Print the list of schemas (subjects) available on the schema server
	 * @return this SchemaDTOBuilder
	 * @throws Exception generic
	 */
	public SchemaDTOBuilder listSchemas() throws Exception {
		Collection<String> c = this.schemaRegistryClient.getAllSubjects();
		for (String s : c) {
			Integer schemaId = this.getSchemaMetaData(s).getId();
			this.print(String.format("%s [%s]",s, schemaId));
		}
		return this;
	}
	
	/**
	 * Print the schema as linked to the topic
	 * usage 
	 * @return this SchemaDTOBuilder
	 * @throws Exception generic
	 */
	public SchemaDTOBuilder printSchema() throws Exception {
		ObjectMapper m = new ObjectMapper();
		String s = m.readTree(this.schema.toString()).toPrettyString();
		this.print(s);
		return this;
	}
	
	//public void addSchema() throws Exception {
	//	this.schemaRegistryClient.
	//}
	
	/**
	 * set manually a schema for instance of SchemaDTOBuilder
	 * @param schema String
   * @return This SchemaDTOBuilder (for chaining) 
	 */
	public SchemaDTOBuilder setSchema(String schema) {
		this.schema = new Schema.Parser().parse(schema);
  	return this;
	}
	
  /**
   * get the schema from the registy server for a topic and write java sources of a DTO in the system temp folder
   * 
   * @return This SchemaDTOBuilder (for chaining)  
   * @throws Exception generic exception
   */
	public SchemaDTOBuilder buildDTOsrc() throws Exception {
		String t = String.valueOf(System.currentTimeMillis());

		this.schemaName = schema.getName();
		this.namespace = schema.getNamespace();
		this.baseDirString = System.getProperty("java.io.tmpdir") + File.separator + this.schemaName + "_" + t;
		this.baseDir = new File(baseDirString);
		this.srcDir = new File(baseDir + "/src");
		if (!srcDir.exists()) {
			srcDir.mkdirs();
		}
		// write the location java files are written to.
		this.print(String.format("buildDTOsrc() write sources to: %s%n",srcDir.toString()));

		SpecificCompiler specificCompiler = new SpecificCompiler(this.schema);
		// source isn't a file, but a variable, so first parameter of compileToDestination is null. 
		specificCompiler.compileToDestination(null, srcDir);

		return this;
	}
	
	

	/**
	 * compile the java sources into binary classes
	 * 
   * @return This SchemaDTOBuilder (for chaining)  
	 * @throws Exception - generic exception
	 */
	public SchemaDTOBuilder compileDTOclasses() throws Exception {
		Path root = Path.of(this.srcDir.toURI());
		this.print(String.format("compileDTOclasses() to: %s%n",root.toString()));
		List<Path> paths = new ArrayList<>();
		
		Files.walk(root).filter(Files::isRegularFile).forEach(path -> paths.add(path));
		
		this.print("=========== paths");
		for(Path x : paths) {
			this.print("Path:" + x.toString());
		}
		this.print("=========== end paths");
	
		JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
	
		Set<SourceVersion> versionSet = javaCompiler.getSourceVersions();
		this.print("=========== versionSet");
		for (SourceVersion sv : versionSet) {
			this.print(sv.name());
		}
		this.print("=========== end versionSet");

		StandardJavaFileManager fileManager = javaCompiler.getStandardFileManager(null, null, null);
		Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromPaths(paths);
		
		this.print("=========== objects");
		for (JavaFileObject o  : compilationUnits ) {
		  this.print(o.getName());
		}
		this.print("=========== end objects");
		
		DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
		
		JavaCompiler.CompilationTask task = javaCompiler.getTask(cf.printwriter, fileManager, diagnostics, null, null, compilationUnits);
		task.call();
		
		this.print("=========== diagnostics");
		for (Diagnostic<? extends JavaFileObject> d : diagnostics.getDiagnostics()) {
			 this.print(d.getCode());
			 this.print(d.getMessage(null));
			 this.print(String.valueOf(d.getSource()));
			 this.print(String.valueOf(d.getKind()));
			 //this.print(String.valueOf(d.getPosition()));
			 //this.print(String.valueOf(d.getStartPosition()));
			 //this.print(String.valueOf(d.getEndPosition()));
		}
		this.print("=========== end diagnostics");
		return this;
	}

	private static Manifest getManifest() {
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		return manifest;
	}
	
  /**
   * Write all sources and compiled classes into a valid jar file with a standard MANIFEST
   * 
   * @throws Exception - generic Exception
   */
	public void createJar() throws Exception {
		Path sourcePath = Path.of(this.srcDir.toURI());

		File jarFile = new File(this.baseDirString + File.separator + this.namespace + ".jar");

		try (FileOutputStream fileOutputStream = new FileOutputStream(jarFile);
				JarOutputStream jarOutputStream = new JarOutputStream(fileOutputStream, getManifest())) {

			Files.walk(sourcePath).filter(Files::isRegularFile).forEach(file -> {
				try {
					String entryName = sourcePath.relativize(file).toString().replace(File.separator, "/");
					this.print(entryName);
					JarEntry jarEntry = new JarEntry(entryName);
					jarOutputStream.putNextEntry(jarEntry);
					Files.copy(file, jarOutputStream);
					jarOutputStream.closeEntry();
				} catch (IOException e) {
					throw new RuntimeException("Error adding file to JAR: " + e.getMessage(), e);
				}
			});
		} catch(Exception e) {
			this.print(String.format("Exception createJar() %s%n",e.toString()));
		}
		
		this.print(String.format("created jarfile: %s%n", jarFile.getPath()));
	}
}