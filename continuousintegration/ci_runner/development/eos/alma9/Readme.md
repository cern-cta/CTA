### How to use

#### 1. Build the docker image

This script will build the docker image that can be used for EOS development:

```bash
./prepare_eosdev_image.sh [<eos_version>]
```
The `<eos_version>` is the version of EOS to get the dependencies from. It can be a commit, branch or a tag. \
If not defined, `<eos_version>` will default to `master`.

#### 2. Run the docker image

To list the latest built images, run:
```bash
podman images
```

To launch one of them (detached), proceed with:
```bash
./start_eosdev.sh [-p <port>] [-v <volumemount>] <image>
```

The `<port>` parameter can be used to forward port `<port>` on the localhost to port `22` on the container. This can be used by ssh clients to access the container through a network. \
The `<volumemount>` parameter can be used to mount a volume. By default, it's `~/shared:/root/shared:Z`.
The `<image>` is the image of the container that we want to run.

##### 2.1 Setup `ssh` server

By default, the image comes with `openssh-server` installed and running. \
In order to accept SSH connections, we need to upload a public key to the container.

Example:
```bash
# Do not upload private key!
podman cp <id_rsa_or_other.pub> <container_id>:/root/.ssh/authorized_keys
```

And also open the port `<port>` on the host:
```bash
sudo firewall-cmd --permanent --add-port=<port>/tcp
sudo firewall-cmd --reload
sudo firewall-cmd --list-all
```

With this completed, accessing the container is simple:
```bash
# Access the container on host <hostname> and port <port>
ssh -p <port> root@<hostname>
```

#### 3. Build EOS

Once inside the container, building EOS is simple!
```bash
git clone https://gitlab.cern.ch/dss/eos.git
cd eos/
git submodule update --init --recursive
mkdir ../eos_build
cd ../eos_build
cmake3 ../eos
make -j 4
```

To validate that CTA was build correctly, run the unit tests inside `eos_build`:
```bash
# We need to set the EOS libraries path, unless we have installed them
cd eos_build
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/common:$(pwd)/mq
# Run the tests
cd unit_tests/
./eos-unit-tests
```

##### 3.1. Build EOS RPMs

Building the RPMs follows a similar procedure:
```bash
mkdir eos_rpm
cd eos_rpm
cmake3 ../eos
make rpm
```

In order to make the RPMs available to the host dev machine, copy them to the `shared` directory (mounted volume):
```bash
cp eos_rpm/RPMS/x86_64/* /root/shared/
```