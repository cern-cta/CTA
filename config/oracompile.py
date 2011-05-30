#!/usr/bin/python

#******************************************************************************
#                      config/oracompile.py
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
# @author Castor Dev team, castor-dev@cern.ch
#******************************************************************************

# Modules
import platform
import string
import sys
import os
import re
import getopt

# Constants
ORACLE_DIRS = ("/usr/__lib__/oracle/__version__/__client__",  "/afs/cern.ch/project/oracle/@sys/__version__")
LIBDIRS     = ("lib", "lib64")
VERSIONS    = ("11.2", "10.2.0.3", "10203")
CLIENTS     = ("client", "client64")

#------------------------------------------------------------------------------
# Usage
#------------------------------------------------------------------------------
def usage():
    print "Usage: %s [options]\n" % (os.path.basename(sys.argv[0]))
    print "-h, --help               Display this help and exit"
    print "    --with-precomp       Return an environment which has Pro*C"
    print "    --home               Return the value for ORACLE_HOME"
    print "    --cppflags           Return the C pre processor flags"
    print "    --procinc            Return the include= arguments for Pro*C"
    print "    --libdir             Return the location of ORACLE libraries"
    print "    --bindir             Return the directory with Pro*C is found"
    print "    --version=ORAVERSION The required oracle version"
    print "    --quiet              Suppress all warnings\n"
    print "Report bugs to <castor-support@cern.ch>"

#------------------------------------------------------------------------------
# GetOracleEnv
#------------------------------------------------------------------------------
def getOracleEnv(withPreCompiler, oracleVersions):

    """
    Function to automatically determine the ORACLE environment required to
    compile CASTOR. The logic is far from perfect and makes many assumptions
    with regards to directory layouts.

    On success the function will return a dict structure with the keys:
    home, bin, libdir, cppflags and procinc set appropriately. On error the
    returned structure will keys with no value.
    """

    # Try to determine the base directory to be used taking into consideration
    # the supported ORACLE versions.
    dirpath = None
    found = False
    for directory in ORACLE_DIRS:
        for libdir in LIBDIRS:
            for version in oracleVersions:
                for client in CLIENTS:
                    if not found:
                        # Check for a sub directory in the base matching the required ORACLE
                        # version.
                        dirpathcand = directory.replace("__lib__", libdir)
                        dirpathcand = dirpathcand.replace("__version__", version)
                        dirpathcand = dirpathcand.replace("__client__", client)
                        if not os.path.isdir(dirpathcand):
                            # directory does not exist, next candidate please
                            continue
                        # We have a valid directory lets check that the proc (Pro*C) binary
                        # exists. This is a simple check that the layout is as expected.
                        procpath = os.sep.join([dirpathcand, "bin", "proc"])
                        if not os.path.isfile(procpath) and withPreCompiler:
                            continue
                        # We have a valid base directory
                        dirpath = dirpathcand
                        found = True

    # Set default values for the return argument.
    rtn = dict()
    rtn['home']     = ""
    rtn['cppflags'] = ""
    rtn['procinc']  = ""
    rtn['libdir']   = ""
    rtn['bindir']   = ""

    if not dirpath:
        return rtn

    # Construct the return argument.
    rtn['home']     = dirpath
    rtn['bindir']   = os.path.join(dirpath, "bin")

    # The values for libdir, cppflags and procinc are different between OIC
    # and /afs/ so we set the values accordingly.
    if dirpath.startswith("/afs/"):

        # libdir on /afs/ makes no distinction between lib and lib64.
        rtn['libdir'] = os.path.join(dirpath, "lib")

        # Add precomp/public and rdbms/public as header file inclusion search
        # paths to the preprocessor compiler options.
        for directory in ("precomp/public", "rdbms/public"):
            filePath = os.path.join(dirpath, directory)
            rtn['cppflags'] += " " + filePath
            rtn['procinc']  += " " + filePath
        rtn['cppflags'] = rtn['cppflags'].strip()
        rtn['procinc']  = rtn['procinc'].strip()
    else:
        rtn['libdir']   = os.path.join(dirpath, libdir)

        # The location of header files in OIC is not in the same base as
        # home.
        filePath = re.sub("^/usr/" + libdir, "/usr/include", dirpath)
        rtn['cppflags'] = filePath
        rtn['procinc']  = filePath

    return rtn

#------------------------------------------------------------------------------
# PrefixCompilerOption
#------------------------------------------------------------------------------
def prefixCompilerOption(string, compilerOption):
    rtnValue = []
    for value in string.split(" "):
        rtnValue.append(compilerOption + value)
    return ' '.join(rtnValue)

#------------------------------------------------------------------------------
# Main Thread
#------------------------------------------------------------------------------

# Process command line arguments.
try:
    opts, args = getopt.getopt(sys.argv[1:], "h",
                               ["help", "with-precomp", "home", "cppflags",
                                "procinc", "libdir", "bindir", "quiet", "version="])
except getopt.GetoptError:
    usage()
    sys.exit(2)

# Defaults.
withPreCompiler = False
oracleVersions  = VERSIONS
quietMode       = False

# Determine if we are in quiet mode.
envOptions = os.getenv('ORACOMPILE_OPTIONS')
if envOptions:
    if "--quiet" in envOptions:
        quietMode = True

# Check to see what ORACLE version the user requires and whether the ORACLE
# installation must contain a valid ORACLE pre compiler (Pro*C).
for opt, arg in opts:
    if opt == "--with-precomp":
        withPreCompiler = True
    if opt == "--version":
        oracleVersions = [str(arg)]

# Construct the return value based on the command line arguments.
oraEnv   = getOracleEnv(withPreCompiler, oracleVersions)
rtnValue = []
for opt, arg in opts:
    if opt == "-h" or opt == "--help":
        usage()
        exit()
    if opt == "--home":
        rtnValue.append(oraEnv['home'])
    if opt == "--cppflags":
        rtnValue.append(prefixCompilerOption(oraEnv['cppflags'], "-I"))
    if opt == "--procinc":
        rtnValue.append(prefixCompilerOption(oraEnv['procinc'], "include="))
    if opt == "--libdir":
        rtnValue.append(oraEnv['libdir'])
    if opt == "--bindir":
        rtnValue.append(oraEnv['bindir'])
    if opt == "--quiet":
        quietMode = True

# Check that we have a valid installation.
if oraEnv['home'] == "" or (withPreCompiler and oraEnv['procinc'] == ""):
    if not quietMode:
        print >> sys.stderr, "Unable to find a valid ORACLE installation"
    sys.exit(1)

print ' '.join(rtnValue)
