package org.asw.kafkafactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class SchemaDTOBuilder {

	SchemaRegistryClient schemaRegistryClient;
	Properties prop;
	String topic;
	File srcDir;
	String baseDirString;
	File baseDir;
	String schemaName;
  String namespace;
	
	public String getTopic() {
		return topic;
	}

	public SchemaDTOBuilder setTopic(String topic) {
		this.topic = topic;
		return this;
	}

	public SchemaDTOBuilder() {
	}

	public SchemaDTOBuilder schemaRegistryClient(KafkaClientFactory cf) {
		this.prop = cf.getProperties();
		this.topic = cf.getTopic();
		@SuppressWarnings({ "unchecked", "rawtypes" })
		Map<String, String> propMap = (Map) prop;
		schemaRegistryClient = new CachedSchemaRegistryClient((String) prop.get("schema.registry.url"), 10000, propMap);

		return this;
	}

	public SchemaDTOBuilder buildDTOsrc() throws Exception {
		List<String> topics = new ArrayList<>();
		topics.add(this.topic + "-value");
		String r = null;

		for (String top : topics) {
			r = this.schemaRegistryClient.getLatestSchemaMetadata(top).getSchema();
			break;
		}
		//System.out.println(r);
		Schema schema = new Schema.Parser().parse(r);
		String t = String.valueOf(System.currentTimeMillis());
		this.schemaName = schema.getName();
		this.namespace=schema.getNamespace();
		this.baseDirString = System.getProperty("java.io.tmpdir") + File.separator + this.schemaName + "_" + t;
		this.baseDir = new File(baseDirString);
		this.srcDir = new File(baseDir + "/src");
		if (!srcDir.exists()) {
			srcDir.mkdirs();
		}

		SpecificCompiler specificCompiler = new SpecificCompiler(schema);
		specificCompiler.compileToDestination(null, srcDir);

		return this;
	}

	public SchemaDTOBuilder compileDTOclasses() throws Exception {
		Path root = Path.of(this.srcDir.toURI());
		List<Path> paths = new ArrayList<>();
		Files.walk(root).filter(Files::isRegularFile).forEach(path -> paths.add(path));

		JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
		StandardJavaFileManager fileManager = javaCompiler.getStandardFileManager(null, null, null);
		Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromPaths(paths);

		JavaCompiler.CompilationTask task = javaCompiler.getTask(null, fileManager, null, null, null, compilationUnits);
		task.call();

		return this;
	}

	public static Manifest getManifest() {
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		return manifest;
	}

	public void createJar() throws Exception {
		Path sourcePath = Path.of(this.srcDir.toURI());

		File jarFile = new File(this.baseDirString + File.separator + this.namespace + ".jar");

		try (FileOutputStream fileOutputStream = new FileOutputStream(jarFile);
				JarOutputStream jarOutputStream = new JarOutputStream(fileOutputStream, getManifest())) {
			
			Files.walk(sourcePath).filter(Files::isRegularFile).forEach(file -> {
				try {
					String entryName = sourcePath.relativize(file).toString().replace(File.separator, "/");
					JarEntry jarEntry = new JarEntry(entryName);
					jarOutputStream.putNextEntry(jarEntry);
					Files.copy(file, jarOutputStream);
					jarOutputStream.closeEntry();
				} catch (IOException e) {
					throw new RuntimeException("Error adding file to JAR: " + e.getMessage(), e);
				}
			});
		}
		System.out.println(jarFile.getPath());
	}
}