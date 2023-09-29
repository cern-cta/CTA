FROM cern/alma9-base AS buildstage

LABEL maintainer="Jorge Camarero Vera"
LABEL maintainer_mail="jcamarerov@gmail.com"

USER root

# Set the working directory
WORKDIR /build

COPY spec/gtest-devel-1.12.0.spec /build/

RUN set -ex; \
    yum -y install epel-release; \
    yum -y install git wget gcc gcc-c++ cmake3 make rpm-build yum-utils; \
    yum update -y;

# Spdlog
RUN set -ex; \
    mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}; \
    export SPECFILE="gtest-devel-1.12.0.spec"; \
    export PACKAGE="gtest-devel"; \
    export BUILDARCH="x86_64"; \
    export VERSION="1.12.0"; \
    export RELEASE=1; \
    export RPMTOPDIR=$(rpm --eval '%{_topdir}'); \
    export BUILTSRPM=$RPMTOPDIR/SRPMS/$PACKAGE-$VERSION-$RELEASE.src.rpm; \
    export BUILTRPM=$RPMTOPDIR/RPMS/$BUILDARCH/$PACKAGE-$VERSION-$RELEASE.$BUILDARCH.rpm; \
    export DEST="../build"; \
    wget https://github.com/google/googletest/archive/refs/tags/v1.12.0.tar.gz -P $RPMTOPDIR/SOURCES; \
	cp /build/$SPECFILE $RPMTOPDIR/SPECS/; \
	rpmbuild -ba $RPMTOPDIR/SPECS/$SPECFILE; \
	rpm -ivh --nodeps $RPMTOPDIR/RPMS/$BUILDARCH/$PACKAGE-$VERSION-$RELEASE.$BUILDARCH.rpm;

FROM cern/alma9-base

LABEL maintainer="Jorge Camarero Vera"
LABEL maintainer_mail="jorge.camarero@cern.ch"

COPY --from=buildstage /root/rpmbuild/RPMS/x86_64/gtest-devel-1.12.0-1.x86_64.rpm /

ENV USER="develop"

COPY ./repos/ /etc/yum.repos.d/

RUN set -ex; \
    yum -y install epel-release almalinux-release-devel; \
    yum -y install git wget gcc gcc-c++ cmake3 make rpm-build yum-utils; \
    yum install -y /gtest-devel-1.12.0-1.x86_64.rpm; \
    yum update -y;

RUN set -ex; \
    wget https://download.oracle.com/otn_software/linux/instantclient/1919000/oracle-instantclient19.19-basic-19.19.0.0.0-1.el9.x86_64.rpm; \
    wget https://download.oracle.com/otn_software/linux/instantclient/1919000/oracle-instantclient19.19-devel-19.19.0.0.0-1.el9.x86_64.rpm; \
    yum install -y oracle-instantclient19.19-basic-19.19.0.0.0-1.el9.x86_64.rpm; \
    yum install -y oracle-instantclient19.19-devel-19.19.0.0.0-1.el9.x86_64.rpm;

WORKDIR /Project

RUN set -ex; \
    git clone https://gitlab.cern.ch/cta/CTA.git; \
    cd CTA; \
    git submodule update --init --recursive

RUN set -ex; \
    cd CTA; \
    sed -i 's/protobuf3/protobuf/g' cta.spec.in; \
    sed -i 's/instantclient19.3/instantclient19.19/g' cta.spec.in; \
    sed -i 's/15.2.15/16.2.4/g' cta.spec.in; \
    sed -i 's/, grpc-static//' cta.spec.in; \
    sed -i 's/protobuf3/protobuf/g' cmake/FindProtobuf3.cmake; \
    sed -i 's/lib64\/protobuf/lib64/g' cmake/FindProtobuf3.cmake; \
    sed -i 's/protoc3/protoc/g' cmake/FindProtobuf3.cmake; \
    sed -i 's/include\/protobuf/include/g' cmake/FindProtobuf3.cmake;

RUN set -ex; \
    mkdir /Project/build_srpm; \
    cd /Project/build_srpm; \
    cmake3 -DPackageOnly:Bool=true ../CTA; \
    make cta_srpm; \
    yum-builddep -y /Project/build_srpm/RPM/SRPMS/*;

RUN set -ex; \
    cd CTA; \
    sed -i 's/19.3/19.19/g' cmake/Findoracle-instantclient.cmake; \
    # Modify ABI to 1, because protobuf for alma9 is compiled with ABI=1; \
    sed -i 's/ABI=0/ABI=1/g' CMakeLists.txt;

# RUN set -ex; \
#     mkdir /Project/build; \
#     cd /Project/build; \
#     cmake3 ../CTA; \
#     make;
