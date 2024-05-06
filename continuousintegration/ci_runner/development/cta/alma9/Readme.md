### How to use

#### 1. Build the docker image

This script will build the docker image that can be used for CTA development:

```bash
./prepare_ctadev_image.sh [<cta_version>]
```
The `<cta_version>` is the version of CTA to get the dependencies from. It can be a commit, branch or a tag. \
If not defined, `<cta_version>` will default to `main`.

#### 2. Run the docker image

To list the latest built images, run:
```bash
podman images
```

The output should look like this:
```
REPOSITORY                                       TAG         IMAGE ID      CREATED      SIZE
localhost/ctadev                                 dev         78e0d6e583a3  2 days ago   1.84 GB
gitlab-registry.cern.ch/linuxsupport/alma9-base  latest      dc893533419c  5 days ago   231 MB
gcr.io/k8s-minikube/kicbase                      v0.0.43     619d67e74933  2 weeks ago  1.27 GB
```

To launch one of them (detached), proceed with:
```bash
./start_ctadev.sh [-p <port>] [-v <mount>] <image>
```

For `<image>`, select `localhost/ctadev`

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

#### 3. Build CTA

Once inside the container, building CTA is simple!
```bash
git clone https://gitlab.cern.ch/cta/CTA.git
cd CTA/
git submodule update --init --recursive
mkdir ../CTA_build
cd ../CTA_build
cmake3 ../CTA
make -j 4
```

To validate that CTA was build correctly, run the unit tests inside `CTA_build`:
```bash
cd tests/
./cta-unitTests
```

##### 3.1. Build CTA RPMs

Building the RPMs follows a similar procedure:
```bash
mkdir CTA_rpm
cd CTA_rpm
cmake3 ../CTA
make cta_rpm
```

In order to make the RPMs available to the host dev machine, copy them to the `shared` directory (mounted volume):
```bash
cp CTA_rpm/RPM/RPMS/x86_64/* /root/shared/
```