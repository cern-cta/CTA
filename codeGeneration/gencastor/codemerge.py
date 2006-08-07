#!/usr/bin/python
#
# An internal script used by gencastor to merge specific code for some special cases in the generated one.
# Currently used to merge the clone() method for some classes that need a custom implementation.
#
# @author Castor dev team, castor-dev@cern.ch
#
# @(#)$RCSfile: codemerge.py,v $ $Revision: 1.1 $ $Release$ $Date: 2006/08/07 14:44:59 $ $Author: itglp $
#

import sys
import os

if(len(sys.argv) != 4):
  # we don't want to expose this to users
  print "codemerge: for internal use only, see genCastor"
  sys.exit(1)

source = open(sys.argv[1], 'r')
tobeadded = open(sys.argv[2], 'r')
dest = open(sys.argv[3], 'w')
header = tobeadded.readline()

# at the beginning, dest = source
currentline = ''
while currentline != header:
  currentline = source.readline()
  dest.write(currentline)

# append now the tobeadded part
dest.write(tobeadded.read())
source.readline()
source.readline()

# append the rest of source
dest.write(source.read())

source.close()
tobeadded.close()
dest.close()

