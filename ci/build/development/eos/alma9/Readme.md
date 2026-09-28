# EOS development-container helpers

This page describes the local image-preparation and container-start scripts in this directory. For building EOS and testing it with CTA, follow [EOS Environment Setup](../../../../../docs/content/dev/guides/integrations/eos/environment-setup.md).

### How to use

#### 1. Build the docker image

This script will build the docker image that can be used for EOS development:

```bash
./prepare_eosdev_image.sh [<eos_version>]
```
The `<eos_version>` is the version of EOS to get the dependencies from. It can be a commit, branch or a tag. \
If not defined, `<eos_version>` will default to `master`. However, for EOS development, it's recommended to use a tagged commit instead of `master` (to work from a reproducible source revision).

#### 2. Run the docker image

To list the latest built images, run:
```bash
podman images
```

To launch one of them (detached), proceed with:
```bash
./start_eosdev.sh [-p <port>] [-v <mount>] <image>
```

The `<port>` parameter can be used to forward port `<port>` on the localhost to port `22` on the container. This can be used by ssh clients to access the container through a network. \
The `<mount>` parameter can be used to mount a local directory to the container directory `/shared`.
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
