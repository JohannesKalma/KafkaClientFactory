package org.asw.kafkafactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** 
 * Build an AVRO schema (stub) based on a JSON data string.<br>
 * Stricktly not part of the Factory<br>
 * use:
 * 
 * <pre>
 * String convertedSchema = new AvroSchemaBuilder().setNameSpace([namespace String])
 *					.setClassName([name of DTO class])
 *					.setDescription([description of this purpose of this schema])
 *					.convert("a data string in json format to be read");
 *				                                            
 * System.out.println(convertedSchema);
 * </pre>
 * 
 * <br>
 * <em>Result is not a final schema, a stub is generated that needs further
 * (minimal) manual modifications (set decend DTO names, document fields
 * and)</em><br>
 * Also when values should be based on a enum, then this enum must be added
 * manually:<br>
 * From a json conversion, the type will be converted as string,
 * 
 * <pre>
 * "type": ["null",string]
 * </pre>
 * 
 * This should be replaced by a structure like this:
 * 
 * <pre>
 * "type": [
 *   "null",
 *     {
 *       "name": "MyShapeEnum",
 *       "symbols": [
 *         "Square",
 *         "Circle",
 *         "Triangle"
 *         ],
 *       "type": "enum"
 *     }
 *   ]
 * </pre>
 */


public class AvroSchemaBuilder {

	// private static final Logger logger =
	// LoggerFactory.getLogger(Json2SchemaBuilder.class);

	private static final String NAME = "name";
	private static final String TYPE = "type";
	private static final String ARRAY = "array";
	private static final String ITEMS = "items";
	private static final String STRING = "string";
	private static final String LONG = "long";
	private static final String DOUBLE = "double";
	private static final String RECORD = "record";
	private static final String FIELDS = "fields";
	private static final String NULL = "null";
	private static final String BOOLEAN = "boolean";
	private static final String NAMESPACE = "namespace";
	private static final String DOC = "doc";
	private static final String DEFAULT = "default";

	private String className;
	private String description;

	private final ObjectMapper mapper;

	private String nameSpace;

	/**
	 * set the Namespace
	 * 
	 * @param nameSpace String
	 * @return this AvroSchemaBuilder for chaining
	 */
	public AvroSchemaBuilder setNameSpace(String nameSpace) {
		this.nameSpace = nameSpace;
		return this;
	}

	/**
	 * set the className of this schema
	 * 
	 * @param className String
	 * @return this AvroSchemaBuilder for chaining
	 */
	public AvroSchemaBuilder setClassName(String className) {
		this.className = className;
		return this;
	}

	/**
	 * set description (doc) of this schema
	 * 
	 * @param description String
	 * @return this AvroSchemaBuilder for chaining
	 */
	public AvroSchemaBuilder setDescription(String description) {
		this.description = description;
		return this;
	}

	/**
	 * Constructor
	 *
	 */
	public AvroSchemaBuilder() {
		this.mapper = new ObjectMapper();
	}

	/**
	 * validation
	 *
	 * @param avroSchemaString to validate
	 * @param jsonString       to validate
	 * @return true if validated, false otherwise
	 * @throws IOException Exception
	 */
	/*
	 * @SuppressWarnings("unchecked") public boolean validateX(final String
	 * avroSchemaString, final String jsonString) throws IOException { Schema.Parser
	 * t = new Schema.Parser(); Schema schema = t.parse(avroSchemaString);
	 * GenericDatumReader reader = new GenericDatumReader(schema); JsonDecoder
	 * decoder = DecoderFactory.get().jsonDecoder(schema, jsonString);
	 * reader.read(null, decoder); return true; }
	 */

	/**
	 * convert to avro schema
	 *
	 * @param json to convert
	 * @return avro schema json
	 * @throws IOException exception
	 */
	public String convert(final String json) throws IOException {
		final JsonNode jsonNode = mapper.readTree(json);
		final ObjectNode finalSchema = mapper.createObjectNode();
		finalSchema.put(NAMESPACE, this.nameSpace).put(NAME, this.className).put(DOC, this.description).put(TYPE, RECORD)
				.set(FIELDS, getFields(jsonNode));
		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalSchema);
	}

	private JsonNode typeNode(String type) {
		final ArrayNode typeNode = mapper.createArrayNode();
		typeNode.add(NULL).add(type);
		return typeNode;
	}

	private JsonNode typeNode(ObjectNode type) {
		final ArrayNode typeNode = mapper.createArrayNode();
		typeNode.add(NULL).add(type);
		return typeNode;
	}

	/**
	 * @param jsonNode to getFields
	 * @return array nodes of fields
	 */
	private ArrayNode getFields(final JsonNode jsonNode) {
		final ArrayNode fields = mapper.createArrayNode();
		final Iterator<Map.Entry<String, JsonNode>> elements = jsonNode.fields();

		Map.Entry<String, JsonNode> map;
		while (elements.hasNext()) {
			map = elements.next();
			final JsonNode thisNode = map.getValue();
			/* for object and array */
			final ObjectNode objectNode = mapper.createObjectNode();

			switch (thisNode.getNodeType()) {

			case NUMBER:
				fields.add(mapper.createObjectNode().put(NAME, map.getKey()).put(DOC, map.getKey()).putNull(DEFAULT)
						.set(TYPE, typeNode(thisNode.isLong() ? LONG : DOUBLE)));
				break;

			case STRING:
				fields.add(mapper.createObjectNode().put(NAME, map.getKey()).put(DOC, map.getKey()).putNull(DEFAULT)
						.set(TYPE, typeNode(STRING)));
				break;

			case OBJECT:
				objectNode.put(NAME, map.getKey()).set(TYPE,
						typeNode(mapper.createObjectNode().put(TYPE, RECORD).put(NAME, map.getKey() + "DTO")
								.put(NAMESPACE, this.nameSpace).putNull(DEFAULT).set(FIELDS, getFields(thisNode))));
				fields.add(objectNode);
				break;

			case ARRAY:
				final ArrayNode arrayNode = (ArrayNode) thisNode;
				final JsonNode element = arrayNode.get(0);
				objectNode.putNull(DEFAULT).put(NAME, map.getKey());

				if (element == null) {
					throw new RuntimeException("Unable to guess at schema type for empty array");
				}

				switch (element.getNodeType()) {
				case NUMBER:
					objectNode.set(TYPE,
							mapper.createObjectNode().put(TYPE, ARRAY).put(ITEMS, (thisNode.isLong() ? LONG : DOUBLE)));
					break;
				case STRING:
					objectNode.set(TYPE, mapper.createObjectNode().put(TYPE, ARRAY).put(ITEMS, STRING));
					break;
				default:
					objectNode.set(TYPE,
							typeNode(mapper.createObjectNode().put(TYPE, ARRAY).set(ITEMS,
									mapper.createObjectNode().put(TYPE, RECORD).put(NAME, map.getKey() + "DTO")
											.put(NAMESPACE, this.nameSpace).set(FIELDS, getFields(element)))));
				}
				fields.add(objectNode);
				break;

			case NULL:
				objectNode.put(NAME, map.getKey()).putArray(TYPE).add(NULL);
				fields.add(objectNode);
				break;

			case BOOLEAN:
				fields.add(mapper.createObjectNode().put(NAME, map.getKey()).put(DOC, map.getKey()).set(TYPE,
						typeNode(BOOLEAN)));
				break;

			default:
				throw new RuntimeException("Unable to determine action for ndoetype " + thisNode.getNodeType()
						+ "; Allowed types are ARRAY, STRING, NUMBER, OBJECT");
			}
		}
		return fields;
	}

}
