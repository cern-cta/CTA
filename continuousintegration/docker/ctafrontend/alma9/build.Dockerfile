FROM gitlab-registry.cern.ch/linuxsupport/alma9-base

LABEL maintainer="Jorge Camarero Vera"
LABEL maintainer_mail="jorge.camarero@cern.ch"

ENV USER="develop"

COPY ./etc/yum.repos.d/ /etc/yum.repos.d/
COPY ./etc/yum.repos.d/ /etc/yum.repos.d/

RUN set -ex; \
    yum update -y; \
    yum -y install epel-release almalinux-release-devel; \
    yum -y install git wget gcc gcc-c++ cmake3 make rpm-build yum-utils; \
    yum clean all;

WORKDIR /Project

RUN set -ex; \
    git clone https://gitlab.cern.ch/cta/CTA.git; \
    cd CTA; \
    git switch 499-compile-and-running-cta-in-alma9-linux; \
    git submodule update --init --recursive

COPY ./cc7toalma9.sh /Project/CTA/continuousintegration/docker/ctafrontend/alma9/cc7toalma9.sh
COPY ./code.patch /Project/CTA/continuousintegration/docker/ctafrontend/alma9/code.patch

RUN set -ex; \
    cd CTA;

RUN set -ex; \
    mkdir /Project/build_srpm; \
    cd /Project/build_srpm; \
    cmake3 -DPackageOnly:Bool=true ../CTA; \
    make cta_srpm; \
    yum-builddep -y /Project/build_srpm/RPM/SRPMS/*;

RUN set -ex; \
    mkdir /Project/build; \
    cd /Project/build; \
    cmake3 ../CTA; \
    make cta_rpm -j7;
