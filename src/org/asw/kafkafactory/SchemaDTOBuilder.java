package org.asw.kafkafactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.util.RandomData;

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

	Iterable<String> compilerOptions ;
	
	KafkaClientFactory cf;
	
	String artifactPath;
	String artifact;
	
	/**
	 * getArtifactPath()
	 * @return string
	 */
	public String getArtifactPath() {
		return artifactPath;
	}

	/**
	 * getArtifact()
	 * 
	 * @return string
	 */
	public String getArtifact() {
		return artifact;
	}

	/**
	 * return kafkaClientFactory instance
	 * 
	 * @return kafkaCleintFactory instance
	 */
	public KafkaClientFactory getKafkaClientFactory() {
		return cf;
	}

	/**
	 * setKafkaClientFactory
	 * 
	 * @param cf KafkaClientFactory instance
	 */
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
  SchemaMetadata schemaMetadata;
	
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
		//this.logPrintwriter = cf.getKafkaProcessLogPrintWriter(); 
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Map<String, String> propMap = (Map) prop;
		this.schemaRegistryClient = new CachedSchemaRegistryClient((String) prop.get("schema.registry.url"), 10000, propMap);

		return this;
	}

	private void print(String s) {
		if (cf != null && cf.getPrintwriter() != null) {
			cf.print(s);
		} else {
			System.out.println(s);
		}
		//this.logPrintwriter.println(s);
	}

	/**
	 * printAllVersions()
	 * 
	 * @return class instance
	 * @throws Exception generic
	 */
	public SchemaDTOBuilder printAllVersions() throws Exception {
		List<Integer> versionList = this.schemaRegistryClient.getAllVersions(this.topic+"-value");
		
		String list = "";
		for (Integer version: versionList) {
      list = list + version + ";";			
		}
		
		print("Versions: "+list +"[for topic "+this.topic+"-value]");
		
		return this;
	
	}
	
	private SchemaMetadata getSchemaMetaData(String subject) throws Exception {
		return this.schemaRegistryClient.getLatestSchemaMetadata(subject);
	}
	
	/**
	 * printSchema()
	 * 
	 * @return class instance
	 * @throws Exception generic
	 */
	public SchemaDTOBuilder printSchemaId() throws Exception {
		print("SchemaId: "+String.valueOf(this.schemaRegistryClient.getLatestSchemaMetadata(this.topic+"-value").getId()));
		
		return this;
	}
	
	/**
	 * enum schemTypeEnum
	 * 
	 **/
	public enum schemaTypeEnum{
		/**KEY**/
		KEY,
		/**VALUE**/
		VALUE
	}
	
	schemaTypeEnum schemaType = schemaTypeEnum.VALUE;
	
	/**
	 * setSchemaKey()
	 * 
	 * @return class instance
	 */
	public SchemaDTOBuilder setSchemaTypeKey(){
		this.schemaType = schemaTypeEnum.KEY;
		return this;
	}
	
	/**
	 * setSchemaTypeValue()
	 * 
	 * @return class instance
	 */
	public SchemaDTOBuilder setSchemaTypeValue(){
		this.schemaType = schemaTypeEnum.VALUE;
		return this;
	}
	
	/**
	 * setSchemaType()
	 * 
	 * @param schemaType enum
	 * @return class instance
	 */
	public SchemaDTOBuilder setSchemaType(schemaTypeEnum schemaType) {
		this.schemaType = schemaType;
		return this;
	}
	
	private String formatTopic() {
		
		String formattedTopicName = String.format("%s-%s",this.topic,"value");
		switch (this.schemaType) {
		  case KEY:
		  	formattedTopicName = String.format("%s-%s",this.topic,"key");
		  	break;
		  default:	
		  	//
		}
		
		return formattedTopicName;
	}
	
	/**
	 * get the schema from latest topic schema metadata
   * @return This SchemaDTOBuilder (for chaining) 
	 * @throws Exception generic Exception
	 */
	public SchemaDTOBuilder setSchemaFromTopic() throws Exception {
		
		this.schemaMetadata = this.getSchemaMetaData(formatTopic());
    String schema = this.schemaMetadata.getSchema();
    this.setSchema(schema);
    
		//for (String topic : topicList) {
		//	this.schema = this.schemaRegistryClient.getLatestSchemaMetadata(topic).getSchema();
		//	break;
		//}
    return this;
	}

	/**
	 * listSchemas()
	 * 
	 * @param find schema name
	 * @return class instance
	 * @throws Exception generic
	 */
	public SchemaDTOBuilder listSchemas(String find) throws Exception {
		Collection<String> c = this.schemaRegistryClient.getAllSubjects();
		for (String s : c) {
			if (s.matches(find)) {
			  Integer schemaId = this.getSchemaMetaData(s).getId();
			  this.print(String.format("%s [%s]",s, schemaId));
			}  
		}
		return this;
	}
	
	/**
	 * Print the list of schemas (subjects) available on the schema server
	 * @return this SchemaDTOBuilder
	 * @throws Exception generic
	 */
	public SchemaDTOBuilder listSchemas() throws Exception {
    return this.listSchemas(".*");		
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
	 * buildClassPath
	 * 
	 * @param path string
	 * @return class instance
	 */
	public SchemaDTOBuilder buildClassPath(String path) {
		return setBuilderClassPath(path);
	}
	
	/**
	 * setBuilderClassPath
	 * 
	 * @param path string
	 * @return class instance
	 */
	public SchemaDTOBuilder setBuilderClassPath(String path) {
		
		StringBuilder classPath = new StringBuilder();
		classPath.append(System.getProperty("java.class.path"));
		if(KafkaUtil.isNotBlank(path)) {
		  File pathFile = new File(path);
		  for (File file : pathFile.listFiles()) {
			  if (file.isFile() && file.getName().endsWith(".jar")) {
			  	classPath.append(System.getProperty("path.separator"));
				  classPath.append(path);
				  classPath.append(file.getName());
        }
		  }  
		}
		
		compilerOptions = Arrays.asList( "-classpath",classPath.toString());
		
		for (String s : compilerOptions) {
			print(s);
		}

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
		
		//this.print("=========== paths");
		//for(Path x : paths) {
	  //		this.print("Path:" + x.toString());
		//}
		//this.print("=========== end paths");
	
		JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
	
		//Set<SourceVersion> versionSet = javaCompiler.getSourceVersions();
		//this.print("=========== versionSet");
		//for (SourceVersion sv : versionSet) {
		//	this.print(sv.name());
		//}
		//this.print("=========== end versionSet");

		StandardJavaFileManager fileManager = javaCompiler.getStandardFileManager(null, null, null);
		Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromPaths(paths);
		
		this.print("=========== objects");
		for (JavaFileObject o  : compilationUnits ) {
		  this.print(o.getName());
		}
		this.print("=========== end objects");
		
		DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
		
		JavaCompiler.CompilationTask task = javaCompiler.getTask(cf.getPrintwriter(), fileManager, diagnostics, compilerOptions, null, compilationUnits);
		task.call();
		
		if (diagnostics.getDiagnostics().size()>0) {
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
		}
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
   * @return class instance
   * @throws Exception - generic Exception
   */
	public SchemaDTOBuilder buildArtifact() throws Exception {
		Path sourcePath = Path.of(this.srcDir.toURI());

		LocalDateTime currentDate = LocalDateTime.now();
    String format = "yyyyMMddHHmmss";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
    String formattedDateTime = currentDate.format(formatter);
		this.artifact = this.namespace +"."+this.schemaName.toLowerCase()+"."+ formattedDateTime +".jar";
		File jarFile = new File(this.baseDirString + File.separator + this.artifact);

		try (FileOutputStream fileOutputStream = new FileOutputStream(jarFile);
				JarOutputStream jarOutputStream = new JarOutputStream(fileOutputStream, getManifest())) {

			Files.walk(sourcePath).filter(Files::isRegularFile).forEach(file -> {
				try {
					String entryName = sourcePath.relativize(file).toString().replace(File.separator, "/");
					//this.print(entryName);
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
		this.artifactPath = jarFile.getPath();
		this.print(String.format("created jarfile: %s%n", this.artifactPath));
		
		return this;
	}
	
	/**
	 * print BullshitData()
	 */
	public void generateBullshitData(){
    Iterator<Object> it = new RandomData(this.schema, 1).iterator();
    System.out.println(it.next());
	}
	
}