#!/usr/bin/python
from os import sys

if len(sys.argv) != 2:
	print 'Usage:', sys.argv[0], 'castorversion > /tmp/afsInstallScript ; source /tmp/afsInstallScript'
	sys.exit(1)

castordir = '/afs/cern.ch/project/cndoc/wwwds/HSM/CASTOR/DIST/CERN/savannah/'
afsdir = '/afs/cern.ch/sw/lcg/external/castor/'
version = sys.argv[1]
packages = ['ns-client', 'upv-client', 'stager-client', 'vdqm2-client', 'rfio-server', 'lib', 'devel', 'vmgr-client', 'tape-client', 'rfio-client']

platformConversion = {'SL4' : 'slc4', 'SL5' : 'slc5'}
oldArchConversion = {'i386' : 'ia32', 'x86_64' : 'amd64'}
archConversion = {'i386' : 'i686', 'x86_64' : 'x86_64'}
oldGccVersion = {'SL4' : 'gcc34'}
gccVersions = {'SL5' : ['gcc41-opt', 'gcc34-opt', 'gcc43-opt']}

def buildSFTdir(plat, arch):
    if plat == 'SL4':   # old conventions
        return [afsdir + version + '/' + platformConversion[plat] + '_' + oldArchConversion[arch] + '_' + oldGccVersion[plat]]
    else:
        base = afsdir + version + '/' + archConversion[arch] + '-' + platformConversion[plat] + '-'
        ret = []
        for v in gccVersions[plat]:
            ret.append(base + v)
        return ret

for plat in ('SL4', 'SL5'):
    for arch in ('i386', 'x86_64'):
        archdirs = buildSFTdir(plat, arch)
        rpmdir = castordir + 'CASTOR.pkg/' + version + '/' + plat + '/' + arch + '/'
        print 'mkdir -p ' + archdirs[0] + ';cd ' + archdirs[0]
        for p in packages:
            print 'rpm2cpio ' + rpmdir + 'castor-' + p + '-' + version + '.' + arch + '.rpm | cpio -idmv'
        for d in archdirs[1:]:
            print 'ln -s ' + archdirs[0] + " " + d
