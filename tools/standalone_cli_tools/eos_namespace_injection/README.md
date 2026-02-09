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

For authentication you must have a way of communicating with ```mgm``` as well as a valid grpc key for comunication with ```EOS```.

To set up kerberos for the frontend:

1. Check that a keytab file for the cta admin user is present, in this example ```/root/```
2. Run kinit: ```KRB5CCNAME=/tmp/${CTAADMIN_USER}/krb5cc_0 kinit -kt /root/${CTAADMIN_USER}.keytab ${CTAADMIN_USER}@TEST.CTA```
3. Make sure that the correct ```KRB5CCNAME``` is set

To set up grpc for comunication between the frontend and ```EOS```:

1. Make sure that you have the correct grpc key in ```eos.grpc.keytab```
2. Add a grpc gateway: ```eos -r 0 0 vid add gateway ${FRONTEND_IP} grpc```