#!/usr/bin/python

#******************************************************************************
#                      config/pycompile.py
#
# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# @(#)$RCSfile: pycompile.py,v $ $Revision: 1.4 $ $Release$ $Date: 2007/11/29 16:17:01 $ $Author: waldron $
#
# @author Castor Dev team, castor-dev@cern.ch
#******************************************************************************

# modules
import sys
import os
import getopt
import distutils.sysconfig

#
# usage
#
def usage():
    print 'Usage: %s [options]\n' % (os.path.basename(sys.argv[0]))
    print '-h, --help      Display this help and exit'
    print '    --cflags    Display pre-processor and compile flags'
    print '    --libs      Display the link flag\n'
    print '    --inc       Display inclusion path\n'
    print 'Report bugs to <castor-support@cern.ch>'

#
# main
#
def main(argv):

    # default values
    cflags = 0
    libs   = 0
    inc    = 0
    flags  = ''

    # process command line arguments
    try:
	opts, args = getopt.getopt(argv, 'hcli', ['help', 'cflags', 'libs', 'inc'])
    except getopt.GetoptError:
	usage()
	sys.exit(2)

    # process options
    try:
	for opt, arg in opts:
	    if opt in ('-h', '--help'):
		usage()
		sys.exit()
	    elif opt in ('-c', '--cflags'):
		cflags = 1;
	    elif opt in ('-l', '--libs'):
		libs = 1;
	    elif opt in ('-i', '--inc'):
		inc = 1;
    except ValueError:
	sys.exit(1)

    # construct the flags to use
    sysconfig = distutils.sysconfig.get_config_vars()

    if cflags:
	flags = '%s' % (sysconfig['CFLAGS'])
    if inc:
	flags += '-I%s -L%s/config ' % (sysconfig['INCLUDEPY'], sysconfig['BINLIBDEST'])
    if libs:
        flags += '-L%s -lpython%s %s %s' % (sysconfig['LIBDIR'], sysconfig['VERSION'], sysconfig['LIBS'], sysconfig['LIBM'])

	# for some version of python the shared objects to link against need 
	# to be defined explicitly
	if 'INSTSONAME' not in sysconfig:
	    dyn    = sysconfig['DESTSHARED']
	    flags += ' %s/mathmodule.so %s/cmathmodule.so %s/timemodule.so %s/cryptmodule.so' % (dyn, dyn, dyn, dyn);

    # the flags
    print flags
    sys.exit(0)


if __name__ == '__main__':
    main(sys.argv[1:])
