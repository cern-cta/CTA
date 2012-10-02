#!/usr/bin/python
#/******************************************************************************
# *                      makeMagicnumbers
# *
# * This file is part of the Castor project.
# * See http://castor.web.cern.ch/castor
# *
# * Copyright (C) 2003  CERN
# * This program is free software; you can redistribute it and/or
# * modify it under the terms of the GNU General Public License
# * as published by the Free Software Foundation; either version 2
# * of the License, or (at your option) any later version.
# * This program is distributed in the hope that it will be useful,
# * but WITHOUT ANY WARRANTY; without even the implied warranty of
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# * GNU General Public License for more details.
# * You should have received a copy of the GNU General Public License
# * along with this program; if not, write to the Free Software
# * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''creates a text file gathering all "magic numbers" of CASTOR'''

import os, sys, subprocess, stat

try:
    root = os.environ['CASTOR_ROOT']
except KeyError:
    print "ERROR: The environment variable CASTOR_ROOT is not set"
    sys.exit(1)

try:
    os.unlink("MagicNumbers")
except OSError:
    # cool, it was already not there
    pass

print "Extracting Object statuses..."
g = open("MagicNumbers", 'w')
constants = {}

content = open('/'.join([root, 'castor', 'db', 'oracleConstants.sql'])).readlines()
# go through all packages
curline = 0
while curline < len(content):
    # go to next package
    while curline < len(content) and content[curline].find('PACKAGE') < 0: curline += 1
    # get name and keep only '*const'
    if curline == len(content) : break
    l = content[curline].split()
    index = l.index('PACKAGE')
    name = l[index+1].lower()
    curline += 1
    if not name.endswith('const'):
        continue
    # parse lines until the end of package and add to dictionnary of constants
    while curline < len(content) and content[curline].lower().find('end ' + name) < 0:
        l = content[curline].split()
        if len(l) in (5,6) and l[1].upper() == 'CONSTANT' and l[2].upper() == 'PLS_INTEGER' and l[3] == ':=':
            obj, state = l[0].split('_',1)
            if obj not in constants: constants[obj] = {}
            value = l[4].split(';')[0]
            constants[obj][int(value)] = state
        curline += 1

        
for obj in constants:
    g.write("     %s\n\n" % obj)
    l = constants[obj].keys()
    l.sort()
    for n in l:
        g.write("%3d %s_%s\n" % (n, obj, constants[obj][n]))
    g.write("\n")

def execshellcmds(cmd):
    tmpfile = os.tempnam()
    print 'using ' + tmpfile
    f = open(tmpfile, 'w')
    f.write(cmd)
    f.close()
    os.chmod(tmpfile, stat.S_IRWXU)
    output = subprocess.Popen(tmpfile, stdout=subprocess.PIPE).stdout.read()
    #os.unlink(tmpfile)
    return output

g.write(execshellcmds('''#!/usr/bin/tcsh
foreach f ( `find $CASTOR_ROOT/castor/stager $CASTOR_ROOT/castor/monitoring $CASTOR_ROOT/castor/vdqm -name '*.hpp' | grep -v DlfMessage` )
  set output=`grep -H '_' $f | grep '=' | grep -v 'm_' | grep -v "if" | grep -v 'static' | grep -v 'throw' | sed 's/\/\/.*$//' | grep -v '(' | sed 's/^.*\/\([a-zA-Z]*\).hpp:/\\1/'`
  if !("$output" == "") then
    echo $output | sed 's/^ *\([^ ]*\).*$/    \\1\\n/' | sed 's/StatusCodes*//' | awk '{print "    ", toupper($1);}'
    echo $output | sed 's/,/\\n/g' | sed 's/ *[^ ]* *\([^ ]*\) = \([0-9]*\).*$/\\2  \\1/'
    echo
  endif
end
'''))

print "Parsing SRM2 constants..."
g.write(execshellcmds('''#!/usr/bin/tcsh
if $?SRM_ROOT then
  echo "\\n    SRM2 OBJECTS\\n"
  grep OBJ_ $SRM_ROOT/srm/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\\2  \\1/'
  echo

  foreach f ( `find $SRM_ROOT/srm -name '*Status.hpp'` )
    set output=`grep '_' $f | grep '=' | grep -v 'm_' | grep -v "if" | sed 's/\/\/.*$//'`
    if !("$output" == "") then
      echo $output | sed 's/^\([^_]*\).*$/    \\1\\n/'
      echo $output | sed 's/,/\\n/g' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\\2  \\1/'
      echo
    endif
  end
endif
'''))
  
print "Parsing Castor constants..."
g.write("     OBJECTS\n\n")
g.write(execshellcmds('''#!/usr/bin/tcsh
grep OBJ_ $CASTOR_ROOT/castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\\2  \\1/'
'''))

g.write("\n     SERVICES\n\n")
g.write(execshellcmds('''#!/usr/bin/tcsh
grep SVC_ $CASTOR_ROOT/castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\\2  \\1/'
'''))

g.write("\n     REPRESENTATIONS\n\n")
g.write(execshellcmds('''#!/usr/bin/tcsh
grep REP_ $CASTOR_ROOT/castor/Constants.hpp | grep '=' | sed 's/ *\([^ ]*\) = \([0-9]*\).*$/\\2  \\1/'
'''))
g.write("\n")
g.close()

subprocess.Popen('a2ps --toc= --columns=4 -f 7.5 -c -o MagicNumbers.ps MagicNumbers', shell=True)
