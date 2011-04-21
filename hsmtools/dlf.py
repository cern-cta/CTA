#/******************************************************************************
# *                   dlf.py
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
# * @(#)$RCSfile: castor_tools.py,v $ $Revision: 1.9 $ $Release$ $Date: 2009/03/23 15:47:41 $ $Author: sponcec3 $
# *
# * this class implements the distributed logging facility of CASTOR for the python
# * tools. It basically uses syslog to send well formatted messages to the DLF server
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

import syslog
import castor_tools
import thread

# a little, useful enum type, with parameterized base value
def enum(*args, **kwds):
    try:
        base = kwds['base']
    except KeyError:
        base = 0
    enums = dict(zip(args, range(base, base+len(args))))
    return type('Enum', (), enums)

_messages = {}
_priorities = { syslog.LOG_EMERG:   "Emerg",
                syslog.LOG_ALERT:   "Alert",
                syslog.LOG_CRIT:    "Crit",
                syslog.LOG_ERR:     "Error",
                syslog.LOG_WARNING: "Warn",
                syslog.LOG_NOTICE:  "Notice",
                syslog.LOG_INFO:    "Info",
                syslog.LOG_DEBUG:   "Debug" }
_logmask = 0xff
_initialized = False

# Constants
_LOG_PRIMASK = 0x07

def init(facility):
    '''Initializes the distributed logging facility. Sets the facility name and the mask'''
    # Default logmask to include all messages up to and including INFO
    global _logmask
    syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_INFO))
    # Find out which logmask should be used from castor.conf
    config = castor_tools.castorConf()
    smask = config.getValue(facility, "LogMask", "LOG_INFO")
    # Set the log mask
    try:
        syslog.setlogmask(
            syslog.LOG_UPTO(getattr(syslog, smask)))
        _logmask = syslog.setlogmask(0)
    except AttributeError:
        # Unknown mask name
        raise AttributeError('Invalid Argument: ' + smask + ' for ' + facility + '/LogMask option in castor.conf')
    # Open syslog
    syslog.openlog(facility, syslog.LOG_PID, syslog.LOG_LOCAL3)
    _initialized = True

def shutdown():
    '''Close/shutdown the distributed logging facility interface'''
    if not _initialized:
        return
    # Close the syslog API
    syslog.closelog()
    # Reset internal variables
    _logmask = 0xff
    _messages = {}
    _initialize = False

def addmessages(msgs):
    '''Add new messages to the set of known messages'''
    # add messages given to the list of known ones (or overwrite any existing one)
    # and specifiy that we did not yet send them to the server
    # messages should be passed as a dictionary with the key being the message
    # number and the value being the message itself
    for msgnb in msgs:
        _messages[msgnb] = [msgs[msgnb], False]

def writep(priority, msgnb, **params):
    '''Writes a log message with the given priority.
    Parameters can be passed as a dictionnary of name/value.
    Some parameter names will trigger a specific interpretation :
      - reqid : will be treated as the UUID of the ongoing request
      - fileid : will be treated as a pair(nshost, fileid)
    '''
    # check if API has been initialized
    if not _initialized:
        return
    # ignore messages whose priority is not of interest
    global _logmask
    if syslog.LOG_MASK(priority & _LOG_PRIMASK) & _logmask == 0:
        return
    # check whether this is the first log of this message
    if not _messages[msgnb][1]:
        # then send the message to the server
        syslog.syslog(syslog.LOG_INFO | syslog.LOG_LOCAL2, 'MSGNO=%d MSGTEXT="%s"' % (msgnb, _messages[msgnb][0]))
        _messages[msgnb][1] = True
    # build the message raw text
    # note that the thread id is not the actual linux thread id but the "fake" python one
    # note also that we cut it to 5 digits (potentially creating colisions) so that it does
    # not exceed the length expected by som eother components (e.g. syslog)
    rawmsg = 'LVL=%s TID=%d MSG="%s" ' % (_priorities[priority], thread.get_ident()%10000, _messages[msgnb][0])
    if params.has_key('fileid'):
        rawmsg += 'NSHOSTNAME=%s NSFILEID=%d ' % (params['fileid'])
    if params.has_key('reqid'):
        rawmsg += 'REQID=%s ' % (params['reqid'])
    if params.has_key('subreqid'):
        rawmsg += 'SUBREQID=%s ' % (params['subreqid']) 
    for param in params:
        if param in ['reqid', 'fileid', 'subreqid']:
            continue
        value = params[param]
        if value == None:
            continue

        # Integers
        try:
            rawmsg += '%s=%s ' % (param[0:20], int(value))
            continue
        except ValueError:
            pass

        # Floats
        try:
            rawmsg += '%s=%s ' % (param[0:20], float(value))
            continue
        except ValueError:
            pass

        # Strings
        value = value.replace('\n', ' ') # remove newlines
        value = value.replace('\t', ' ') # remove tabs
        value = value.replace('"', '\'') # escape double quotes
        value = value.strip("'") # remove trailing and leading signal quotes
        rawmsg += '%s="%s" ' % (param[0:20], value[0:1024])

    syslog.syslog(priority, rawmsg.rstrip())

def write(msgnb, **params):
    '''Writes a log message with the LOG_INFO priority.
    See writep for more details'''
    writep(syslog.LOG_INFO, msgnb, **params)

def writedebug(msgnb, **params):
    '''Writes a log message with the LOG_DEBUG priority.
    See writep for more details'''
    writep(syslog.LOG_DEBUG, msgnb, **params)

def writeerr(msgnb, **params):
    '''Writes a log message with the LOG_ERR priority.
    See writep for more details'''
    writep(syslog.LOG_ERR, msgnb, **params)

def writewarning(msgnb, **params):
    '''Writes a log message with the LOG_WARNING priority.
    See writep for more details'''
    writep(syslog.LOG_WARNING, msgnb, **params)

def writenotice(msgnb, **params):
    '''Writes a log message with the LOG_NOTICE priority.
    See writep for more details'''
    writep(syslog.LOG_NOTICE, msgnb, **params)

def writeemerg(msgnb, **params):
    '''Writes a log message with the LOG_EMERG priority.
    See writep for more details'''
    writep(syslog.LOG_EMERG, msgnb, **params)
