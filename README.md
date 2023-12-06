## Nifi Custom Avro Decoder

### Properties - 

Avro schema - avro schema text for decoding

Base64 encoding used - true/false, is Base64 encoding used to convert avro binary data into avro string?

### Build

`mvn clean install`

### Setup

Add the `nifi-custom-avro-decoder-nar/target/nifi-custom-avro-decoder-nar-1.0.nar` file in the `lib/` directory.

