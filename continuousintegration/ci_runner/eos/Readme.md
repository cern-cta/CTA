### How to use

#### 1. Build the docker image

This script will build the docker image that will be used to build Eos.

```bash
./prepare_eos_builder.sh <eos_version>
```

The `<eos_version>` is the version of Eos to build. It can be `master` or a tag name like `5.0.0`.

#### 2. Build Eos

##### 2.1. Build Eos

This script will build Eos and put the binaries in the `build` directory.

```bash
docker run -it --rm -v ~/eos:/eos:z eosbuilder:dev
```

The folder `~/eos` will be mounted in the docker container as `/eos`. And it's where the Eos source code is expected to be.

##### 2.2. Build Eos RPMs

This script will build Eos RPMs and put them in the `~/eos/build` directory.

```bash
docker run -it --rm -v ~/eos:/eos:z eosbuilder:dev rpm
```

The generated RPMs will be in the `~/eos/build/RPMS/x86_64` directory.
