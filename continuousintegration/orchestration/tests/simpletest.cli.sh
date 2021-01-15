#!/bin/bash

cta bs -u root --hostname $(hostname -i) -m "docker cli"

cta logicallibrary add --name VLSTK --comment "ctasystest"

cta tapepool add --name ctasystest --partialtapesnumber 5 --encrypted false --comment "ctasystest"

cta tape add --logicallibrary VLSTK --tapepool ctasystest --capacity 1000000000 --comment "ctasystest" --vid ${VID} --full false

cta storageclass add --instance root --name ctaStorageClass --copynb 1 --comment "ctasystest"

cta archiveroute add --instance root --storageclass ctaStorageClass --copynb 1 --tapepool ctasystest --comment "ctasystest"

cta mountpolicy add --name ctasystest --archivepriority 1 --minarchiverequestage 1 --retrievepriority 1 --minretrieverequestage 1 --maxdrivesallowed 1 --comment "ctasystest"

cta requestermountrule add --instance root --name root --mountpolicy ctasystest --comment "ctasystest"

cta drive up VDSTK1
