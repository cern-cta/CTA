FROM cern/alma9-base

LABEL maintainer="Jorge Camarero Vera"
LABEL maintainer_mail="jorge.camarero@cern.ch"

ENV USER="develop"

COPY ./repos/ /etc/yum.repos.d/

RUN set -ex; \
    yum update -y; \
    yum -y install git; \
    

WORKDIR /Project

RUN set -ex; \
    git clone https://gitlab.cern.ch/cta/CTA.git; \
    cd CTA; \
    git submodule update --init --recursive


