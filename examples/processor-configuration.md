# Example Processor Configuration

## StandardIP2LocationDatabaseService

```text
IP2Location BIN File = /opt/ip2location/DB26.BIN
Database Access Method = File I/O
Lookup Cache Size = 10000
```

## IP2LocationEnrichRecord

```text
IP2Location Database Service = StandardIP2LocationDatabaseService
Record Reader = JsonTreeReader
Record Writer = JsonRecordSetWriter
IP Field Path = /ip
Output Field Name = ip2location
Return Fields = country_short,country_long,region,city,latitude,longitude,zipcode,timezone,isp,asn,as,usagetype,district
Include Status = true
Include Empty Values = false
Non-OK Lookup Strategy = Keep Record
```
