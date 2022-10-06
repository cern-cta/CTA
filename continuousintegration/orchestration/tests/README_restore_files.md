# Restore Files Unit Test
The test checks if the restore tool restores a deleted file from the recycle bin. The steps are:
- Creates a new file 
- Deletes the file
- Restores the file 
- Checks consistency: archive id, checksum and file size for deleted and restored file 
- Checks consistency: fxid for restored file in EOS and CTA

## How to Run Test
The test can be run with the following command: 

`./restore_files.sh -n <kubernetes-instance>`

In addition, the tool must be run from the folder:

`~/CTA/continuousintegration/orchestration/tests`

There must also be a keytab file provided in `/etc/cta/eos.grpc.keytab`. Use the same token as on the eos side. The file should be on the form:

``` 
# disk instance  endpoint (host:port)           gRPC token
eosdev           <eoshostname>.cern.ch:50051    <token>
```

In `continuousintegration/docker/ctafrontend/cc7/etc/cta/cta-frontend-xrootd.conf` an option should specify the path to the file:

`cta.ns.config /etc/cta/eos.grpc.keytab`