## Nifi Custom Avro Decoder

### Properties - 

`avroSchema` - avro schema text for decoding

`isBase64EncodingUsed` - true/false, is Base64 encoding used to convert avro binary data into avro string?

`isJsonPathsRequired` - indicates whether json paths are written to a FlowFile attribute.

### Build

`mvn clean install`

### Setup

Add the `nifi-custom-avro-decoder-nar/target/nifi-custom-avro-to-json-nar-1.0.nar` file in the `lib/` directory.

