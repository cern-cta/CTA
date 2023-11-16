FROM cern/alma9-base

LABEL maintainer="Jorge Camarero Vera"
LABEL maintainer_mail="jorge.camarero@cern.ch"

ENV USER="develop"

COPY ./repos/ /etc/yum.repos.d/

RUN set -ex; \
    yum update -y; \
    yum -y install epel-release almalinux-release-devel; \
    yum -y install git wget gcc gcc-c++ cmake3 make rpm-build yum-utils; \
    yum clean all;

RUN set -ex; \
    wget https://download.oracle.com/otn_software/linux/instantclient/1919000/oracle-instantclient19.19-basic-19.19.0.0.0-1.el9.x86_64.rpm; \
    wget https://download.oracle.com/otn_software/linux/instantclient/1919000/oracle-instantclient19.19-devel-19.19.0.0.0-1.el9.x86_64.rpm; \
    yum install -y oracle-instantclient19.19-basic-19.19.0.0.0-1.el9.x86_64.rpm; \
    yum install -y oracle-instantclient19.19-devel-19.19.0.0.0-1.el9.x86_64.rpm;

WORKDIR /Project

RUN set -ex; \
    git clone https://gitlab.cern.ch/cta/CTA.git; \
    cd CTA; \
    git switch 499-compile-and-running-cta-in-alma9-linux; \
    git submodule update --init --recursive

RUN set -ex; \
    cd CTA; \
    sed -i 's/protobuf3/protobuf/g' cta.spec.in; \
    sed -i 's/instantclient19.3/instantclient19.19/g' cta.spec.in; \
    sed -i 's/radosVersion 2:15.2.15/radosVersion 2:16.2.4/g' cta.spec.in; \
    sed -i 's/, grpc-static//' cta.spec.in; \
    sed -i 's/protobuf3/protobuf/g' cmake/FindProtobuf3.cmake; \
    sed -i 's/lib64\/protobuf/lib64/g' cmake/FindProtobuf3.cmake; \
    sed -i 's/protoc3/protoc/g' cmake/FindProtobuf3.cmake; \
    sed -i 's/include\/protobuf/include/g' cmake/FindProtobuf3.cmake; \
    sed -i 's/19.3/19.19/g' cmake/Findoracle-instantclient.cmake; \
    sed -i 's/EosCtaGrpc gpr/EosCtaGrpc gpr absl_synchronization/g' eos_cta/CMakeLists.txt;

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
    make -j7;
