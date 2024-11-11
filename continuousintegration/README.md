# Folder content

* `ci_helpers`: helper scripts used by gilab_ci
* `ci_runner`: setup scripts for running the ci tests using RPMs
* `docker`: Docker files and content needed to build the needed docker images
* `orchestration`: orchestration specific files: this contains all the pods definitions and *startup scripts* launched in the pod containers during instanciation.

# How to update the EOS version used in CI

1. ssh to **aiadm**, then:
```
$ cd /eos/user/c/ctareg/www/cta-ci-repo/
$ cp <path_to_rpms>/eos-*.rpm eos/x86_64/
$ ./update_repos.sh
```
2. In the CTA repository, update the yum version lock list with the new EOS version:
```
/CTA/continuousintegration/docker/alma9/etc/yum/pluginconf.d/versionlock.list
```
