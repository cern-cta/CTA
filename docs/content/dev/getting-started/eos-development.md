# Developing with EOS

You may need to do [EOS development](https://gitlab.cern.ch/dss/eos) at some point, most often in the WFE module.
An automated CI pipeline generates an [EOS container image](https://gitlab.cern.ch/dss/eos/container_registry/10191) for every
merge request against the EOS repository. That is usually the easiest choice for small changes that do not require frequent
rebuilds, but for more extensive work you may prefer to build a local image.


## Setting up your EOS development environment

[The EOS documentation](https://eos-docs.web.cern.ch/diopside/manual/develop.html#develop) contains a complete guide to
setting up your development environment. If you are using the Kubernetes-based CTA development environment, you can instead
use the existing EOS development image (`localhost/eosdev`) to start with a ready-to-use setup:

```console
$ podman run --rm -dit --name eos-build-home-cirunner-shared-CTA-el9 -v /home/cirunner/shared/eos:/shared/eos:z localhost/eosdev:dev
```

Connect to the container with `podman exec`:

```console
$ podman exec -it eos-build-home-cirunner-shared-CTA-el9 /bin/bash
```

Then follow the EOS documentation. For example:

!!! terminal "eos-build-home-cirunner-shared-CTA-el9"

    ```console
    # dnf install -y ninja-build
    # cd /shared/eos
    # mkdir build
    # cd build
    # cmake3 .. -Wno-dev
    # make rpm
    ```


## Building your own EOS deployment image

The [`eos-docker`](https://gitlab.cern.ch/eos/eos-docker) repository provides several `Dockerfile`s for building EOS
container images meant for deployment. To build a local image, you will need to point it at your RPM directory:

```console
$ cd ~/shared
$ ln -s eos/build el-9_artifacts # assuming `build` is your EOS build directory
$ git clone ssh://git@gitlab.cern.ch:7999/eos/eos-docker.git
$ cd eos-docker
$ podman build -f ./eos-docker/Dockerfile_el9 .. --build-arg EOS_CODENAME=diopside -t eos-ci-local:latest
```

To use the resulting image in your (K8s-based) development environment, import it into k3s:

```console
$ podman save localhost/eos-ci-local:latest | sudo /usr/local/bin/k3s ctr images import -
```

Or, if you are using minikube:

```console
$ podman save localhost/eos-ci-local:latest | minikube image load --overwrite -
```

To use it as part of your CTA development setup:

```console
$ ./build_deploy.sh --eos-image-repository localhost/eos-ci-local --eos-image-tag latest
```

### Going one step further: RPM-free Dockerfile

The official EOS container images are built from RPMs, which are then extracted during image creation. You can speed up the process by bypassing RPMs entirely, which should save you some time in a development setup.
A suitable [Dockerfile is available here](https://gitlab.cern.ch/-/snippets/3824).

When using this approach, you will do a regular install, and EOS will need to know where the installed binaries are located:

!!! terminal "eos-build-home-cirunner-shared-CTA-el9"

    ```console
    # cd build
    # cmake3 ../ -DCMAKE_INSTALL_PREFIX=/shared/eos/build/install -Wno-dev
    # make install -j 16
    ```

Then build the image from the downloaded `Dockerfile`:

```console
$ cd ~/shared/eos
$ wget https://gitlab.cern.ch/-/snippets/3824/raw/master/Dockerfile
$ podman build . -t eos-ci-local:latest
```

!!! warning

    This `Dockerfile` is not officially maintained and may become out of sync with upstream EOS. It is intended for development use only. Use it at your own risk, and please do let someone know if it breaks.
