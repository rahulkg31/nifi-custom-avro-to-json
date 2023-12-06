package org.apache.nifi.processors.avro;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroTestUtil {

	public static byte[] serialize(GenericRecord payload, Schema schema, String isBase64EncodingUsed)
			throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		datumWriter.write(payload, binaryEncoder);
		binaryEncoder.flush();
		byteArrayOutputStream.close();
		byte[] bytes = ("true".equals(isBase64EncodingUsed))
				? Base64.getEncoder().encodeToString(byteArrayOutputStream.toByteArray()).getBytes()
				: byteArrayOutputStream.toByteArray();
		return bytes;
	}

}