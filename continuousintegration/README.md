# Folder content

* `ci_helpers`: helper scripts used by gilab_ci
* `docker`: Docker files and content needed to build the needed docker images
* `orchestration`: orchestration specific files: this contains all the pods definitions and *startup scripts* launched in the pod containers during instanciation.
* `ci_runner`: setup scripts for running the ci tests using RPMs
* `ci_runner/vmBootstrap`: As set of scripts describing the preparation of a machine for fresh install to running CI scripts **MUST NOT BE RUN BLINDLY**

# How to update the EOS version used in CI

1. ssh to **aiadm**, then:
```
$ cd /eos/user/c/ctareg/www/cta-ci-repo/
$ cp <path_to_rpms>/eos-*.rpm eos/x86_64/
$ ./update_repos.sh
```
2. In the CTA repository, update the yum version lock list with the new EOS version:
```
/CTA/continuousintegration/docker/ctafrontend/alma9/etc/yum/pluginconf.d/versionlock.list
```
