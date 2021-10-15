FROM schemacrawler/schemacrawler

USER root
RUN apk add --update sqlite
COPY mermaid.py /opt/

USER schcrwlr
WORKDIR /home/schcrwlr

MAINTAINER Richard Bachmann <richard.bachmann@cern.ch>
