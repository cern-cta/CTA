Folder content:
* `ci_helpers`: helper scripts used by gilab_ci
* `docker`: Docker files and content needed to build the needed docker images
* `orchestration`: orchestration specific files: this contains all the pods definitions and *startup scripts* launched in the pod containers during instanciation.
* `buildtree_runner`: setup scripts for running the ci tests from the build tree instead of RPMs
* `buildtree_runner/vmBootstrap`: As set of scripts describing the preparation of a machine for fresh install to running CI scripts **MUST NOT BE RUN BLINDLY**
