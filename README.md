## Nifi Custom Avro to JSON Processor

### Description

This NiFi processor converts Binary Avro records (possibly Base64-encoded) into JSON objects. It provides an option to include JSON paths in FlowFile attributes.

### Properties

- `avroSchema`: Avro schema text for decoding.
- `isBase64EncodingUsed`: Specifies if Base64 encoding is used to convert Avro binary data into Avro strings.
- `isJsonPathsRequired`: Indicates whether JSON paths are written to a FlowFile attribute.

### Build

`mvn clean install`

### Setup

Add the `nifi-custom-avro-to-json-nar/target/nifi-custom-avro-to-json-nar-1.0.nar` file in the `lib/` directory of Nifi setup.

