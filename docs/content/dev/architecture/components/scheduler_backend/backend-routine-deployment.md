# Reporting Routine Deployment Configuration  

The following is an example of a `yaml` configuration of our CI deployment spawning 2 disk reporting daemons for the Archive, 2 for Retrieve, 1 for the Repack expansion and 1 for the Repack reporting routines. These are distinct daemons deployed and configured with distinct sleep intervals and batchSizes in order to allow the desired throughput. With the configuration below, we can expect a maximum of 1.6 kHz of reporting througput from the two archive reporting daemons together `(2*4000/5)`.


```yaml
    diskReportArchive:
      replicas: 2
      sleepIntervalSecs: 5
      batchSize: 4000
      softTimeoutSecs: 30
    diskReportRetrieve:
      replicas: 2
      sleepIntervalSecs: 5
      batchSize: 4000
      softTimeoutSecs: 30
    repackExpand:
      replicas: 1
      sleepIntervalSecs: 10
      maxToToExpand: 2
    repackReport:
      replicas: 1
      sleepIntervalSecs: 10
      softTimeoutSecs: 30
```

Once should also keep in mind to configure the EOS Workflow engine to run frequently enough (currently the maximum frequency is 1 second) and have enough available threads for the work to be done. For example: 


```text
    eos space config default space.wfe.ntx=2000
    eos space config default space.wfe.interval=1
```

This means 2kHz max throughput allowed for the WFE. 

In the `taped` daemon configuration the number of parallel files to be buffered (bufferCount) should conincides with
the increased batchSizes for pgsched deployment. The batch number of jobs passed around in the drive should not be lower than these limits above.  E.g. `taped bufferCount 10000`.




