#!/usr/bin/python
from os import sys

if len(sys.argv) != 2:
	print 'Usage:', sys.argv[0], 'castorversion > /tmp/afsInstallScript ; source /tmp/afsInstallScript'
	sys.exit(1)

castordir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/CERN/savannah/'
afsdir = '/afs/cern.ch/sw/lcg/external/castor/'
version = sys.argv[1]
compatversion = '2.0.2-1'
packages = ['ns-client', 'doc', 'rtcopy-client', 'upv-client', 'commands', 'stager-client', 'vdqm-client', 'rfio-server', 'lib', 'devel', 'stager-clientold', 'vmgr-client', 'rtcopy-messages', 'tape-client', 'rfio-client', 'scriptlets']

platformConversion = {'SLC3' : 'slc3', 'SLC4' : 'slc4'}
archConversion = {'i386' : 'ia32', 'ia64': 'ia64', 'x86_64' : 'amd64'}
gccVersion = {'SLC3' : 'gcc323', 'SLC4' : 'gcc34'}

for plat in ('SLC3', 'SLC4'):
    for arch in ('i386', 'ia64', 'x86_64'):
        archdir = afsdir + version + '/' + platformConversion[plat] + '_' + archConversion[arch] + '_' + gccVersion[plat]
        rpmdir = castordir + 'CASTOR.pkg/' + version + '/' + plat + '/' + arch + '/'
        print 'mkdir -p ' + archdir + ';cd ' + archdir
        for p in packages:
          print 'rpm2cpio ' + rpmdir + 'castor-' + p + '-' + version + '.' + arch + '.rpm | cpio -idmv'
	print 'rpm2cpio ' + castordir + 'CASTOR-compat.pkg/castor-lib-compat-' + compatversion + '.' + platformConversion[plat] + '.' + arch + '.rpm | cpio -idmv'
