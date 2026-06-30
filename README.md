# NiFi IP2Location Record Bundle

Apache NiFi extension for enriching FlowFile records with geolocation data from an IP2Location BIN database.

This project builds one deployable NAR:

```text
nifi-ip2location-nar/target/nifi-ip2location-nar-1.0.0.nar
```

The NAR provides two NiFi components:

```text
StandardIP2LocationDatabaseService
IP2LocationEnrichRecord
```

## Why this design

The Controller Service opens and manages the IP2Location BIN file once. The processor gives users a simple drag-and-drop enrichment component in the NiFi canvas.

```text
GetFile / ConsumeKafka / ListenHTTP
        ↓
IP2LocationEnrichRecord
        ↓
PutFile / PublishKafka / PutDatabaseRecord
```

The processor uses NiFi RecordReader and RecordSetWriter services, so users can work with JSON, CSV, Avro, XML, and other record formats through normal NiFi UI configuration.

## Build

Requirements:

- Java 21
- Maven 3.9+
- Apache NiFi 2.x target runtime

Build:

```bash
mvn clean package
```

NAR output:

```text
nifi-ip2location-nar/target/nifi-ip2location-nar-1.0.0.nar
```

## Install

Copy the NAR to a configured NiFi extensions directory:

```bash
mkdir -p /opt/nifi/extensions/ip2location
cp nifi-ip2location-nar/target/nifi-ip2location-nar-1.0.0.nar /opt/nifi/extensions/ip2location/
```

Add to `conf/nifi.properties`:

```properties
nifi.nar.library.directory.ip2location=/opt/nifi/extensions/ip2location
```

Restart NiFi.

## NiFi UI usage

1. Add Controller Service: `StandardIP2LocationDatabaseService`
2. Configure:

   ```text
   IP2Location BIN File = /opt/ip2location/DB26.BIN
   Database Access Method = File I/O or Memory Mapped File
   Lookup Cache Size = 10000
   ```

3. Add Processor: `IP2LocationEnrichRecord`
4. Configure:

   ```text
   IP2Location Database Service = StandardIP2LocationDatabaseService
   Record Reader = JsonTreeReader / CSVReader / AvroReader / XMLReader
   Record Writer = JsonRecordSetWriter / CSVRecordSetWriter / AvroRecordSetWriter
   IP Field Path = /ip
   Output Field Name = ip2location
   ```

## Example

Input:

```json
{
  "order_id": "A1001",
  "ip": "8.8.8.8"
}
```

Output:

```json
{
  "order_id": "A1001",
  "ip": "8.8.8.8",
  "ip2location": {
    "status": "OK",
    "country_short": "US",
    "country_long": "United States of America",
    "region": "California",
    "city": "Mountain View",
    "latitude": 37.40599,
    "longitude": -122.078514,
    "zipcode": "94043",
    "timezone": "-07:00"
  }
}
```

## Notes

- This starter uses a simple slash-separated IP field path such as `/ip` or `/client/ip`.
- The output field is a top-level object field, defaulting to `ip2location`.
- In a NiFi cluster, the BIN file must exist at the same configured path on every node.
