# Remote Scripts

The scripts in this directory are meant to be copied to the corresponding container/pod so that they can be executed there.

**The goal is to have as few as possible of such scripts. The vast majority of the scripts in this directory are due to the ongoing migration the Python system tests.**

**Do as much as possible in the test itself. Basically the only reason to have a script executing on the pod itself is if performance of the different steps are important (as is the case for the stress test)**
