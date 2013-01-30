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
# * This class implements the distributed logging facility of CASTOR for
# * the python tools. It basically uses syslog to send well formatted
# * messages to the DLF server
# *
# * @author Castor Dev team, castor-dev@cern.ch
# *****************************************************************************/

'''
This class implements the distributed logging facility of CASTOR for
the python tools. It basically uses syslog to send well formatted
messages to the DLF server
'''

#import syslog
import castor_tools
import thread
import traceback
import sys
import logging
from logging.handlers import SysLogHandler as syslog

def enum(*args, **kwds):
    '''a little, useful enum type, with parameterized base value'''
    try:
        base = kwds['base']
    except KeyError:
        base = 0
    enums = dict(zip(args, range(base, base+len(args))))
    return type('Enum', (), enums)


_messages = {}

_priorities = { 
        syslog.LOG_EMERG:   "Emerg",
        syslog.LOG_ALERT:   "Alert",
        syslog.LOG_CRIT:    "Crit",
        syslog.LOG_ERR:     "Error",
        syslog.LOG_WARNING: "Warn",
        syslog.LOG_NOTICE:  "Notice",
        syslog.LOG_INFO:    "Info",
        syslog.LOG_DEBUG:   "Debug" 
        }

_initialized = False

logger = None
_handler = None
_logmask = None


class CastorFormater(logging.Formatter):
    '''Logs formatter
    NB: timestamp and hostname are handled by rsyslog'''
    def format(self, record):
        return "".join([record.name, "[", str(record.process), "]: ", 
                        record.msg])


def init(facility):
    '''Initializes the distributed logging facility. 
    Sets the facility name and the mask'''
    # Default logmask to include all messages up to and including INFO
    global _logmask, _initialized, logger, _handler
    _logmask = syslog.LOG_INFO
    # Find out which logmask should be used from castor.conf
    config = castor_tools.castorConf()
    smask = config.getValue("LogMask", facility, "LOG_INFO")
    # Set the log mask
    try:
        _logmask = getattr(syslog, smask)
    except AttributeError:
        # Unknown mask name
        raise AttributeError('Invalid Argument: ' + smask + ' for ' + 
                              facility + '/LogMask option in castor.conf')
    # Create logger
    logger = logging.getLogger(facility)
    # NB : we take msg up to DEBUG, because the effective level is 
    # handled by _logmask
    logger.setLevel(logging.DEBUG)
    # Open syslog
    _handler = syslog(address='/dev/log', facility=syslog.LOG_LOCAL3)
    _handler.setFormatter(CastorFormater())
    logger.addHandler(_handler)
    _initialized = True

def shutdown():
    '''Close/shutdown the distributed logging facility interface'''
    global _logmask, _initialized, _messages
    if not _initialized:
        return
    # close sysloghandler
    _handler.close()
    # Reset internal variables
    _logmask = None
    _messages = {}
    _initialized = False

def addmessages(msgs):
    '''Add new messages to the set of known messages'''
    # add messages given to the list of known ones
    # (or overwrite any existing one)
    for msgnb in msgs:
        _messages[msgnb] = msgs[msgnb]

def _writep(priority, msgnb, **params):
    '''Writes a log message with the given priority.
    Parameters can be passed as a dictionary of name/value.
    Some parameter names will trigger a specific interpretation :
      - reqid : will be treated as the UUID of the ongoing request
      - fileid : will be treated as a pair(nshost, fileid)
    If called within an exception handler and priority is LOG_ERR or above,
    all details about the exception will be logged.
    '''
    # check if API has been initialized
    if not _initialized:
        return None
    # ignore messages whose priority is not of interest
    if priority > _logmask:
        return None
    # for LOG_ERR and above, check if we're in an exception context
    if priority <= syslog.LOG_ERR:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        if exc_type != None:
            params['TraceBack'] = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            if hasattr(exc_value, '_remote_tb'):
                params['RemoteTraceBack'] = str(exc_value._remote_tb[0])
    # build the message raw text
    # note that the thread id is not the actual linux thread id but the 
    # "fake" python one
    # note also that we cut it to 5 digits (potentially creating colisions)
    # so that it does not exceed the length expected by som eother components
    # (e.g. syslog)
    rawmsg = 'LVL=%s TID=%d MSG="%s" ' % (_priorities[priority], 
                                          thread.get_ident()%10000, 
                                          _messages[msgnb])
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

    return rawmsg.rstrip()

def write(msgnb, **params):
    '''Writes a log message with the LOG_INFO priority.
    See _writep for more details'''
    rawmsg = _writep(syslog.LOG_INFO, msgnb, **params)
    if rawmsg:
        logger.info(rawmsg)

def writedebug(msgnb, **params):
    '''Writes a log message with the LOG_DEBUG priority.
    See _writep for more details'''
    rawmsg = _writep(syslog.LOG_DEBUG, msgnb, **params)
    if rawmsg:
        logger.debug(rawmsg)

def writeerr(msgnb, **params):
    '''Writes a log message with the LOG_ERR priority.
    See _writep for more details'''
    rawmsg = _writep(syslog.LOG_ERR, msgnb, **params)
    if rawmsg:
        logger.error(rawmsg)

def writewarning(msgnb, **params):
    '''Writes a log message with the LOG_WARNING priority.
    See _writep for more details'''
    rawmsg = _writep(syslog.LOG_WARNING, msgnb, **params)
    if rawmsg:
        logger.warning(rawmsg)

def writenotice(msgnb, **params):
    '''Writes a log message with the LOG_NOTICE priority.
    See _writep for more details'''
    rawmsg = _writep(syslog.LOG_NOTICE, msgnb, **params)
    if rawmsg:
        logger.warning(rawmsg)

def writeemerg(msgnb, **params):
    '''Writes a log message with the LOG_EMERG priority.
    See _writep for more details'''
    rawmsg = _writep(syslog.LOG_EMERG, msgnb, **params)
    if rawmsg:
        logger.critical(rawmsg)
