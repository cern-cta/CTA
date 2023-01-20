# How to use the cta-change-storage-class
The cta-change-storage-class can be used to change the storaegclass of files. To storage class update of the eos containers must be done manually by an operator.

```cta-change-storage-class --id/-I <archiveFileID> | --json/-j <path> --storageclassname/-n <storageClassName> [--frequenzy/-t <eosRequestFrequency>]```

where the json file is a text file with one json object for each line, example:

```
{"archiveId": <archiveId>}
{"archiveId": <archiveId>}
{"archiveId": <archiveId>}
```

The tool must be run from the frontend as it needs access to both eos and the catalogue. This means that and ```cta-cli.conf``` and a ```eos.grpc.keytab``` must be copied to the frontend. ```cta-cli``` should be placed in /etc/cta, while the location of ```eos.grpc.keytab``` should be specified in ```cta-frontend-xrootd.conf```. If the tool fails to update any of the files, the ids can be found in ```/tmp/skippedArchiveIds.txt``` in json format.