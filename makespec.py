#!/usr/bin/python

'''Generate the spec file from a Debian style package description collection'''

import re, os, sys
from debian import deb822

# regexp distinghuising architecture dependent files
# these are the ones containing /usr/lib/ except for /usr/lib/perl and /usr/lib/python
archDepRegExp = re.compile('/usr/lib/(?!(perl|python))')

# translation of requires that are SLC version dependent
osDepRequireTranslator = {
    'libuuid-devel' : '''# For uuid/uuid.h
%if 0%{?rhel} >= 6
Build-Depends: libuuid-devel
%else
Build-Depends: e2fsprogs-devel
%endif'''
}

def handleDebianEntry(pkg, entry, specEntry=None, modifier=lambda x: x):
  if None == specEntry:
    specEntry = entry + ': '
  if entry in pkg:
    print '%s%s' % (specEntry, modifier(pkg[entry]))


def integrateDebianFile(pkg, fileExtension, header=None, lineModifier=lambda x: x):
  fileName = 'debian/%s.%s' % (pkg['Package'], fileExtension)
  if os.path.exists(fileName):
    if header:
      print header
    for entry in open(fileName).readlines():
      print lineModifier(entry[:-1])

def handleInstallFiles(pkg):
  installFileName = 'debian/%s.install.perm' % pkg['Package']
  if os.path.exists(installFileName):
    # get knowledge of config files and insert appropriate %config keyword
    confFileName = 'debian/%s.conffiles' % pkg['Package']
    confFiles = []
    if os.path.exists(confFileName):
      confFiles = [fileEntry.strip() for fileEntry in open(confFileName).readlines()]
    # retrieve list of files, and insert %config part when needed
    entries = set([])
    for entry in open(installFileName).readlines():
      if entry.strip() == '': continue
      attr, fileName = entry.split()
      fileName = '/' + fileName
      if fileName in confFiles:
        attr += ' %config(noreplace)'
      entries.add((attr, fileName))
    # distinguish architecture dependent files from regular ones
    archDepEntries = set([entry for entry in entries if archDepRegExp.search(entry[1])])
    entries = entries - archDepEntries
    # create entries for arch independent files
    for attr, fileName in entries:
      print attr, fileName
    # create arch dependent entries
    if archDepEntries:
      print '%ifarch x86_64'
      for attr, fileName in archDepEntries:
        print attr, fileName.replace('/usr/lib/', '/usr/lib64/')
      print '%else'
      for attr, fileName in archDepEntries:
        print attr, fileName
      print '%endif'

def translateRequires(requires):
  reqlist = [req.strip() for req in requires.split(',')]
  easyreqlist = []
  osdepreqlist = []
  for req in reqlist:
    if req in osDepRequireTranslator:
      osdepreqlist.append(req)
    else:
      easyreqlist.append(req)
  res = ', '.join(easyreqlist)
  if osdepreqlist:
    for req in osdepreqlist:
      res += '\n' + osDepRequireTranslator[req]
  return res
      
# include header
print open('castor.spec.in.head').read()

# Append all sub-packages to CASTOR.spec
for pkg in deb822.Packages.iter_paragraphs(open('debian/control')):
    # skip first header
    if 'Package' not in pkg: continue
    # if not a client package, compile only in the server case (the default)
    if ('XBS-Group' not in pkg) or (pkg['XBS-Group'] != 'Client'):
        print '%if %compile_server'
    # package header
    print '%%package -n %s' % pkg['Package']
    print 'Summary: Cern Advanced mass STORage'
    print 'Group: Application/Castor'
    # deal with entries of the control file
    handleDebianEntry(pkg, 'Depends', 'Requires: ')
    handleDebianEntry(pkg, 'Build-Depends', 'BuildRequires: ', translateRequires)
    handleDebianEntry(pkg, 'Provides')
    handleDebianEntry(pkg, 'Conflicts')
    handleDebianEntry(pkg, 'XBS-Obsoletes', 'Obsoletes: ')
    handleDebianEntry(pkg, 'Description', '%%description -n %s\n' % pkg['Package'])
    # header of file related part
    print '%%files -n %s' % pkg['Package']
    print '%defattr(-,root,root)'
    integrateDebianFile(pkg, 'manpages', None,
                        lambda x: '%%attr(0644,root,bin) %%doc %s' % x.replace('debian/castor', ''))
    def convertAttr(attr):   # could be a lambda once slc5 is gone
      if attr.find('%attr') < 0:
        return '%%attr(-,root,bin) %%dir /%s' % attr
      else:
        return attr.replace(') ', ') %dir /')
    integrateDebianFile(pkg, 'dirs', None, convertAttr)
    handleInstallFiles(pkg)
    # deal with scripts
    integrateDebianFile(pkg, 'postinst', '%%post -n %s' % pkg['Package'])
    integrateDebianFile(pkg, 'preun', '%%preun -n %s' % pkg['Package'])
    integrateDebianFile(pkg, 'pre', '%%pre -n %s' % pkg['Package'])
    integrateDebianFile(pkg, 'postun', '%%postun -n %s' % pkg['Package'])
    if ('XBS-Group' not in pkg) or (pkg['XBS-Group'] != 'Client'):
        print '%endif' # end of client compilation if
    print
