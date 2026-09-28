# Developing with EOS

Use this page when changing EOS or testing a different EOS image against CTA. Prepare a working [CTA development environment](../../../getting-started/environment-setup.md) first. For an introduction to the workflows, follow the [archive and retrieve walkthrough](../../../getting-started/archive-retrieve-walkthrough.md).

## Select an EOS image

Prefer an existing image from the [EOS container registry](https://gitlab.cern.ch/dss/eos/container_registry/10191) when testing a published build. Select the image matching the EOS revision you want to validate and record that revision with the test results.

For small changes, an image produced by the EOS project's CI may be sufficient. For frequent rebuilds, use the local workflow below. It complements the [EOS development guide](https://eos-docs.web.cern.ch/diopside/manual/develop.html), particularly when working on the workflow engine (WFE).

## Set up your EOS development environment

The [EOS development guide](https://eos-docs.web.cern.ch/diopside/manual/develop.html) covers setting up an EOS build environment. If you already have the EOS development image (`localhost/eosdev:dev`) available, use it to build a local EOS checkout in a container.

The following example assumes your EOS checkout is at `~/shared/eos`. Adjust the host path and image tag for your environment:

```bash
podman run --rm -dit --name eos-build \
  -v ~/shared/eos:/shared/eos:z localhost/eosdev:dev
```

Connect to the container:

```bash
podman exec -it eos-build /bin/bash
```

Then follow the EOS build instructions, for example the RPM build below. The mounted checkout keeps source edits and build outputs on the host.

## Build EOS RPMs

Inside the development container:

```bash
dnf install -y ninja-build
cd /shared/eos
mkdir -p build
cd build
cmake3 .. -Wno-dev
make rpm
```

Re-run the build after source changes. Consult the EOS development guide for revision-specific build options or dependency requirements. When finished with the development container, run `podman stop eos-build` on the host; the mounted checkout and build outputs remain.

## Build an EOS deployment image

The [eos-docker repository](https://gitlab.cern.ch/eos/eos-docker) supplies deployment Dockerfiles. Its EL9 build expects an `el-9_artifacts` directory in the build context. On the host, arrange the EOS build output and Dockerfile checkout under the same parent directory:

```bash
cd ~/shared
ln -s eos/build el-9_artifacts
git clone https://gitlab.cern.ch/eos/eos-docker.git
podman build -f eos-docker/Dockerfile_el9 \
  --build-arg EOS_CODENAME=diopside \
  -t localhost/eos-ci-local:dev .
```

These commands assume the checkout and build paths used above. If `el-9_artifacts` already exists, check that it points to the intended build output. Use the Dockerfile and codename appropriate to your EOS version; check the repository's instructions if its expected artifact layout changes.

#### Going one step further: RPM-free Dockerfile

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

## Deploy with CTA

Set the EOS image explicitly, replacing the example repository and tag:

```bash
cta-dev deploy --eos-image-repository example/eos --eos-image-tag my-tag
```

This redeploys the development instance. See [cta-dev deployment options](../../tools-and-environment/cta-dev.md#deployment-options) for lifecycle behaviour and [EOS overrides](../../tools-and-environment/cta-dev.md#eos) for configuration values.

The Kubernetes runtime must be able to pull the image from its registry or already have it loaded on the nodes that run EOS. An image in Podman's local storage alone is not available to Kubernetes.

For a local image tagged `localhost/eos-ci-local:dev`, load it into the runtime used by your development cluster. For k3s:

```bash
podman save localhost/eos-ci-local:dev | sudo /usr/local/bin/k3s ctr images import --local -
```

For minikube:

```bash
podman save localhost/eos-ci-local:dev | minikube image load --overwrite -
```

Then select that same image reference:

```bash
cta-dev deploy --eos-image-repository localhost/eos-ci-local --eos-image-tag dev
```

## Validate the integration

Check that the pods start successfully and run the client suite against the deployed instance:

```bash
cta-dev test client
```

This suite exercises a broad range of CTA workflows and takes a few minutes. Add or run tests for the behaviour changed by your EOS revision; use [System Tests](../../testing/system-tests.md) for test selection, setup, and writing tests.

For failures, inspect the relevant EOS and CTA logs using [Working with Development Pods](../../tools-and-environment/development-pods.md) and [Debugging](../../tools-and-environment/debugging.md). Record the CTA and EOS revisions, configuration overrides, and failing operation so the result can be reproduced.
