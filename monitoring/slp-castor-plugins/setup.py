#!/usr/bin/python

import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "slp-castor-plugins",
    version = "0.1.8",
    author = "Benjamin Fiorini",
    author_email = "benjamin.fiorini@cern.ch",
    description = ("Simple log producer - Simple producer for messaging, tailing log file. Plugins pack for CASTOR"),
    license = "GPL",
    keywords = "producer messaging log file tail",
    url = "http://castor.web.cern.ch/",
    long_description=read('README'),
    data_files=[('/etc/simple-log-producer/plugins', ['castor_base_file_parser.py']),
                ('/etc/simple-log-producer/plugins', ['castor_rsyslogfile_body_parser.py']),
                ('/etc/simple-log-producer/plugins', ['castor_rsyslogfile_header_parser.py']),
                ('/etc/simple-log-producer/plugins', ['castor_rsyslogfile_oldheader_parser.py']) ],
)
