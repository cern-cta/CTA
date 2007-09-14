#!/usr/bin/python

# modules
import sys
import os
import getopt
import distutils.sysconfig

#
# usage
#
def usage():
    print '-h, --help      Display this help and exit'
    print '    --cflags    Display pre-processor and compile flags'
    print '    --libs      Display the link flag'

#
# main
#
def main(argv):

    # default values
    cflags = 0
    libs   = 0
    flags  = ''

    # process command line arguments
    try:
	opts, args = getopt.getopt(argv, 'hcl', ['help', 'cflags', 'libs'])
    except getopt.GetoptError:
	usage()
	sys.exit(2)

    # process options
    try:
	for opt, arg in opts:
	    if opt in ('-h', '--help'):
		usage()
		sys.exit()
	    elif opt in ('c', '--cflags'):
		cflags = 1;
	    elif opt in ('l', '--libs'):
		libs = 1;
    except ValueError:
	sys.exit(1)

    # construct the flags to use
    sysconfig = distutils.sysconfig.get_config_vars()

    if cflags:
        flags = '%s -I%s -L%s ' % (sysconfig['CFLAGS'], sysconfig['INCLUDEPY'], sysconfig['LIBPL'])
    if libs:
        flags += '-lpython%s %s %s' % (sysconfig['VERSION'], sysconfig['LIBS'], sysconfig['LIBM'])

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