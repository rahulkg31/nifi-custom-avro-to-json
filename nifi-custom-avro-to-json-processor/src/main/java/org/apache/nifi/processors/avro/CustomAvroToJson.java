package org.apache.nifi.processors.avro;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;

@SideEffectFree
@SupportsBatching
@Tags({ "avro", "convert", "json" })
@CapabilityDescription("Converts a Binary Avro record (with/without Based64 encoded string) into a JSON object.")
public class CustomAvroToJson extends AbstractProcessor {
	private static final byte[] EMPTY_JSON_OBJECT = "{}".getBytes(StandardCharsets.UTF_8);
	public static final PropertyDescriptor AVRO_SCHEMA = new PropertyDescriptor.Builder().name("avroSchema")
			.description("Avro schema text for decoding").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor IS_BASE64_ENCODING_USED = new PropertyDescriptor.Builder()
			.name("isBase64EncodingUsed")
			.description("Is Base64 encoding used to convert avro binary data into avro string?").required(true)
			.allowableValues("true", "false").defaultValue("false").build();
	public static final PropertyDescriptor INCLUDE_JSONPATH_IN_FLOWFILE_ATTRIBUTES = new PropertyDescriptor.Builder().name("isJsonPathsRequired")
			.description(
					"Indicates whether json paths are written to a FlowFile attribute.")
			.required(true).allowableValues("true", "false")
			.defaultValue("false").build();
	public static final Relationship SUCCESS_RELATION = new Relationship.Builder().name("SUCCESS")
			.description("A FlowFile is routed to this relationship after it has been converted to String").build();
	public static final Relationship FAILURE_RELATION = new Relationship.Builder().name("FAILURE").description(
			"A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to String for any reason")
			.build();
	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	private volatile Schema schema = null;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		super.init(context);

		// descriptors
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(AVRO_SCHEMA);
		descriptors.add(IS_BASE64_ENCODING_USED);
		descriptors.add(INCLUDE_JSONPATH_IN_FLOWFILE_ATTRIBUTES);
		this.descriptors = Collections.unmodifiableList(descriptors);

		// relationships
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(SUCCESS_RELATION);
		relationships.add(FAILURE_RELATION);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		// extract values of the properties
		String stringSchema = context.getProperty(AVRO_SCHEMA).getValue();
		String isBase64EncodingUsed = context.getProperty(IS_BASE64_ENCODING_USED).getValue();
		String isJsonPathsRequired = context.getProperty(INCLUDE_JSONPATH_IN_FLOWFILE_ATTRIBUTES).getValue();
		Map<String, String> pathsAndValues = new HashMap<>();

		try {
			flowFile = session.write(flowFile, new StreamCallback() {
				@Override
				public void process(InputStream rawIn, OutputStream rawOut) throws IOException {
					// schema parsing
					if (schema == null) {
						schema = new Schema.Parser().parse(stringSchema);
					}

					// process the data
					final GenericData genericData = GenericData.get();
					try (final InputStream in = new BufferedInputStream(rawIn);
							final OutputStream out = new BufferedOutputStream(rawOut)) {
						// decoder based on base64 encoding used or not
						final Decoder decoder = ("true".equals(isBase64EncodingUsed))
								? DecoderFactory.get().binaryDecoder(
										Base64.getDecoder().decode(new String(IOUtils.toByteArray(in))), null)
								: DecoderFactory.get().binaryDecoder(in, null);

						// decode the data
						final DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
						final GenericRecord record = reader.read(null, decoder);

						// send the decoded data
						final byte[] outputBytes = (record == null) ? EMPTY_JSON_OBJECT
								: genericData.toString(record).getBytes(StandardCharsets.UTF_8);
						out.write(outputBytes);
						
						// when json paths are added to flowfile attribute
						if ("true".equals(isJsonPathsRequired)) {
							// parse
							Configuration configuration = Configuration.defaultConfiguration()
									.addOptions(Option.ALWAYS_RETURN_LIST);
							Object document = JsonPath.using(configuration).parse(genericData.toString(record)).json();

							// extract all paths and their values from the JSON document
							extractPaths("", document, pathsAndValues);
						}
					}
				}
			});
		} catch (final ProcessException pe) {
			getLogger().error("Failed to decrypt {} data due to {}; transferring to failure",
					new Object[] { flowFile, pe });
			session.transfer(flowFile, FAILURE_RELATION);
			return;
		}

		// return success
		if ("true".equals(isJsonPathsRequired)) {
			flowFile = session.putAllAttributes(flowFile, pathsAndValues);
		} else {
			flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
		}

		session.transfer(flowFile, SUCCESS_RELATION);
	}

	private void extractPaths(String parentPath, Object obj, Map<String, String> pathsAndValues) {
		if (obj instanceof Map) {
			Map<String, Object> map = (Map<String, Object>) obj;
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				String currentPath = parentPath.isEmpty() ? "$." + key : parentPath + "." + key;
				extractPaths(currentPath, value, pathsAndValues);
			}
		} else {
			pathsAndValues.put(parentPath, (obj == null) ? "" : obj.toString());
		}
	}
}
