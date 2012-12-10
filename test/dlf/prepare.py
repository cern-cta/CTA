#/******************************************************************************
# *                   prepare.py
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
# * @(#)$RCSfile: prepare.py,v $ $Revision: 1.2 $ $Release$ $Date: 2009/03/03 10:43:49 $ $Author: waldron $
# *
# * script gathering a consistent set of logs from a given castor
# * instance so that the logging activity of the instance can be replayed
# * by the dlftest program.
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import sys, os, time

# list of log files per facility, and whether it is a server log file
logfiles = { 'RequestHandler' : ('rhserver', 'log', True),
             'TapeErrorHandler' : ('rtcpclientd', 'TapeErrorHandler', True),
             'Stager' : ('stager', 'log', True),
             'Scheduler' : ('scheduler', 'log', True),
             'Job' : ('job', 'log', False),
             'DiskCopy' : ('job', 'diskcopy', False),
             'GC' : ('gc', 'log', False),
             'TransferManager' : ('transfermanagerd', 'log', True),
             'TapeGateway' : ('tapegatewayd', 'log', True)
             }

def usage():
    print sys.argv[0] + " <instance> [date YYMMDD]"

# check arguments
if len(sys.argv) != 2 and len(sys.argv) != 3 :
    print "wrong number of arguments"
    usage()
    sys.exit(1)
instance = sys.argv[1]
lognb = 0
if len(sys.argv) == 3:
    try:
        time.strptime(sys.argv[2],"%y%m%d")
        logday = sys.argv[2]
    except ValueError:
        print "second argument should be a date in TTMMDD format"
        usage()
        sys.exit(1)
else:
    logday = time.strftime("%y%m%d",time.localtime())

# get list of machines to deal with
cmd = 'wassh -c c2' + instance + ' --list'
f = os.popen(cmd)
machines = f.read().split()
if (f.close() != None):
    print "Failed running : " + cmd
    print "Exiting"
    sys.exit(1)
smachines = []
dmachines = []
for m in machines:
    if m[0:2] == 'c2':
        smachines.append(m)
    else:
        dmachines.append(m)

# get the files
for fac in logfiles.keys():
    print "Gathering log files for " + fac + ' facility...'
    d = 'fac_'+fac
    try:
        os.mkdir(d)
    except OSError:
        None
    fulld = os.getcwd() + '/' + d
    log = logfiles[fac][0] + '/' + logfiles[fac][1]
    if logfiles[fac][2]:
        ms = smachines
    else:
        ms = dmachines
    for m in ms:
        # find the file on the machine for that facility is there is one
        cmd = 'ssh -q -o "StrictHostKeyChecking no" ' + m + ' "ls --time-style=+dateis%y%m%d /var/spool/' + log + '* -l -t -r | grep dateis' + logday + ' | tail -1"'
        f, g, h = os.popen3(cmd)
        errors = h.read()
        if len(errors) > 0:
            if errors.find("No match") == -1:
                print "  failed to find logs on machine " + m
                print output
            continue
        # get the file
        output = g.readlines()
        if len(output) == 0:
            continue
        output = output[0].split()
        filename = output[len(output)-1]
        cmd = 'scp -o "StrictHostKeyChecking no" ' + m + ":" + filename + " " + fulld + '/' + m + '.' + os.path.basename(filename)
        print "  transfering " + d + '/' +  m + '.' + os.path.basename(filename)
        f, g, h = os.popen3(cmd)
        output = h.read()
        if len(output) > 0:
            print "  Transfer failed :"
            print output
    for f in os.listdir(fulld):
        if f.endswith('.gz'):
            cmd = 'gunzip ' + fulld + '/' + f
            h = os.popen(cmd)
            h.read()
            if (h.close() != None):
                print "failed to unzip '" + f + "'"
            
