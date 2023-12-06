package org.apache.nifi.processors.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.processors.avro.CustomAvroToJson;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

public class TestCustomAvroDecoder {
	private String schemaString = "{\"namespace\":\"example.avro\",\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"int\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";

	@Test
	public void testWithBase64Encoding() throws IOException {
		final TestRunner runner = TestRunners.newTestRunner(new CustomAvroToJson());
		final Schema schema = new Schema.Parser().parse(schemaString);

		runner.setProperty(CustomAvroToJson.AVRO_SCHEMA, schemaString);
		runner.setProperty(CustomAvroToJson.IS_BASE64_ENCODING_USED, "true");

		final GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "Rahul");
		user1.put("favorite_number", 256);

		final byte[] out1 = AvroTestUtil.serialize(user1, schema, "true");
		runner.enqueue(out1);

		runner.run();

		runner.assertAllFlowFilesTransferred(CustomAvroToJson.SUCCESS_RELATION, 1);
		final MockFlowFile out = runner.getFlowFilesForRelationship(CustomAvroToJson.SUCCESS_RELATION).get(0);
		out.assertContentEquals("{\"name\": \"Rahul\", \"favorite_number\": 256, \"favorite_color\": null}");
	}
	
	@Test
	public void testWithoutBase64Encoding() throws IOException {
		final TestRunner runner = TestRunners.newTestRunner(new CustomAvroToJson());
		final Schema schema = new Schema.Parser().parse(schemaString);

		runner.setProperty(CustomAvroToJson.AVRO_SCHEMA, schemaString);

		final GenericRecord user1 = new GenericData.Record(schema);
		user1.put("name", "Rahul");
		user1.put("favorite_number", 256);

		final byte[] out1 = AvroTestUtil.serialize(user1, schema, "false");
		runner.enqueue(out1);

		runner.run();

		runner.assertAllFlowFilesTransferred(CustomAvroToJson.SUCCESS_RELATION, 1);
		final MockFlowFile out = runner.getFlowFilesForRelationship(CustomAvroToJson.SUCCESS_RELATION).get(0);
		out.assertContentEquals("{\"name\": \"Rahul\", \"favorite_number\": 256, \"favorite_color\": null}");
	}

}
