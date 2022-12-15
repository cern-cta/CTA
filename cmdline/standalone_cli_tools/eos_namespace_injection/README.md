# How to use the cta-eos-namespace-inject tool
The cta-eos--namespace-inject-tool can be used inject an eos file if the meta data is already present in the catalogue. To use the tool call

```cta-eos-namespace-inject --json/-j <json-path>```

where the json file is a text file with one json object for each line, example:

```
{"eosPath": "/eos/ctaeos/file1", "diskInstance": "ctaeos", "archiveId": "4294967296", "size": "420", "checksumType": "ADLER32", "checksumValue": "ac94824f"}
{"eosPath": "/eos/ctaeos/file2", "diskInstance": "ctaeos", "archiveId": "4294967297", "size": "420", "checksumType": "ADLER32", "checksumValue": "ac94824f"}
{"eosPath": "/eos/ctaeos/file3", "diskInstance": "ctaeos", "archiveId": "4294967298", "size": "420", "checksumType": "ADLER32", "checksumValue": "ac94824f"}
```

The tool must be run from the frontend as it needs access to both eos and the catalogue. This means that and ```cta-cli.conf``` and a ```eos.grpc.keytab``` must be copied to the frontend. ```cta-cli``` should be placed in /etc/cta, while the location of ```eos.grpc.keytab``` should be specified in ```cta-frontend-xrootd.conf```.