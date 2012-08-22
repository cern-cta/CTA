#!/usr/bin/python
from os import sys

if len(sys.argv) != 2:
	print 'Usage:', sys.argv[0], 'castorversion > /tmp/afsInstallScript ; source /tmp/afsInstallScript'
	sys.exit(1)

castordir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/CERN/savannah/'
afsdir = '/afs/cern.ch/sw/lcg/external/castor/'
version = sys.argv[1]
packages = ['ns-client', 'upv-client', 'stager-client', 'vdqm2-client', 'rfio-server', 'lib', 'devel', 'vmgr-client', 'rfio-client']

platformConversion = {'SL4' : 'slc4', 'SL5' : 'slc5'}
archConversion = {'i386' : 'i686', 'x86_64' : 'x86_64'}
gccVersions = {'SL5' : ['gcc41-opt', 'gcc34-opt', 'gcc43-opt']}

def buildSFTdir(plat, arch):
    return [afsdir + version + '/' + archConversion[arch] + '-' + platformConversion[plat] + '-' + gccversion for gccversion in gccVersions[plat]]

for plat in ('SL5',):
    for arch in ('x86_64',):
        archdirs = buildSFTdir(plat, arch)
        rpmdir = castordir + 'CASTOR.pkg/' + version[:version.rfind('-')] + '-\\*/' + version + '/' + plat + '/' + arch + '/'
        print 'mkdir -p ' + archdirs[0] + ';cd ' + archdirs[0]
        for p in packages:
            print 'rpm2cpio ' + rpmdir + 'castor-' + p + '-' + version + '.' + arch + '.rpm | cpio -idmv'
        for d in archdirs[1:]:
            print 'ln -s ' + archdirs[0] + " " + d
