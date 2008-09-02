#!/usr/bin/python
from os import sys

if len(sys.argv) != 2:
	print 'Usage:', sys.argv[0], 'castorversion > /tmp/afsInstallScript ; source /tmp/afsInstallScript'
	sys.exit(1)

castordir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/CERN/savannah/'
afsdir = '/afs/cern.ch/sw/lcg/external/castor/'
version = sys.argv[1]
packages = ['ns-client', 'doc', 'rtcopy-client', 'upv-client', 'stager-client', 'vdqm-client', 'rfio-server', 'lib', 'devel', 'stager-clientold', 'vmgr-client', 'rtcopy-messages', 'tape-client', 'rfio-client']

platformConversion = {'SLC4' : 'slc4'}
archConversion = {'i386' : 'ia32', 'x86_64' : 'amd64'}
gccVersion = {'SLC4' : 'gcc34'}

for plat in ('SLC4',):
    for arch in ('i386', 'x86_64'):
        archdir = afsdir + version + '/' + platformConversion[plat] + '_' + archConversion[arch] + '_' + gccVersion[plat]
        rpmdir = castordir + 'CASTOR.pkg/' + version + '/' + plat + '/' + arch + '/'
        print 'mkdir -p ' + archdir + ';cd ' + archdir
        for p in packages:
          print 'rpm2cpio ' + rpmdir + 'castor-' + p + '-' + version + '.' + arch + '.rpm | cpio -idmv'
