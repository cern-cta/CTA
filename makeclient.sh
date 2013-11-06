#!/bin/sh

# Build and install the clients only.
# The version is extracted by configure and used later on. Used by
# Teamcity for the client builds to have all steps in the same environment
# and preserve the version env variables.

. ./configure
make client
make installclient $1
