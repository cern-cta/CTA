#!/bin/python

# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import datetime
import getpass
import logging
import logging.config
import os
import re
import socket
import subprocess
import sys
import time

from cta.exceptions import UserError

class Gc:
  def get_env_mgmhost(self):
    if "EOS_MGM_URL" in self.env:
      mgm_url = self.env["EOS_MGM_URL"]
      if mgm_url:
        return re.sub("^x?root://", "", mgm_url)

  def get_sysconfig_file_mgmhost(self, sysconfig_file):
    for line in sysconfig_file:
      mgmhostline = re.match("^EOS_MGM_HOST=.*", line)
      if mgmhostline:
        splitmgmhostline = mgmhostline.group(0).split('=')
        if 2 == len(splitmgmhostline):
          return splitmgmhostline[1]

  def get_syconfig_mgmhost(self):
    if os.path.isfile("/etc/sysconfig/eos_env"):
      sysconfig_file = open("/etc/sysconfig/eos_env", "r")
      return self.get_sysconfig_file_mgmhost(sysconfig_file)

  def setmgmhost(self):
    if self.mgmhost:
      return

    self.mgmhost = self.get_env_mgmhost()
    if self.mgmhost:
      return

    self.mgmhost = self.get_syconfig_mgmhost()
    if self.mgmhost:
      return

    raise Exception("Failed to determine the MGM host")

  def configureDummyLogging(self):
    config = {
      'version': 1,
      'disable_existing_loggers': False,
      'loggers': {
        'gc' : {
          'level': 'INFO'
        }
      }
    }
    logging.config.dictConfig(config)

  def configureFileBasedLogging(self):
    if None == self.logfilepath:
      raise Exception("Cannot configure file based logging because the log file path has not been set")

    loggingdir = os.path.dirname(self.logfilepath)
    if not os.path.isdir(loggingdir):
      raise UserError("The logging directory {} does is not a directory or does not exist".format(loggingdir))
    if not os.access(loggingdir, os.W_OK):
      raise UserError("The logging directory {} cannot be written to by {}".format(loggingdir, self.programname))

    config = {
      'version': 1,
      'disable_existing_loggers': False,
      'formatters': { 
        'stdout': { 
            'format': '%(asctime)s.%(msecs)03d000 %(levelname)s ' + self.programname +
              ': LVL="%(levelname)s" PID="%(process)d" TID="%(process)d" MSG="%(message)s"',
            'datefmt': '%Y/%m/%d %H:%M:%S'
        }
      },
      'handlers': { 
        'logfile': { 
          'level': 'INFO',
          'formatter': 'stdout',
          'class': 'logging.handlers.TimedRotatingFileHandler',
          'filename' : self.logfilepath,
          'when' : 'midnight'
        }
      },
      'loggers': {
        'gc' : {
          'handlers': ['logfile'],
          'level': 'INFO'
        }
      }
    }
    # Failing to configure the logging system is usually a user error
    try:
      logging.config.dictConfig(config)
    except Exception as err:
      raise UserError(err)

  def configureLogging(self):
    if None == self.logfilepath:
      self.configureDummyLogging()
    else:
      self.configureFileBasedLogging()

  def __init__(self, programname, env, minfreebytes, gcagesecs, cmdline_mgmhost = None,
    logfilepath = '/var/log/eos/fst/cta-fst-gcd.log'):
    self.programname = programname
    self.env = env
    self.minfreebytes = minfreebytes
    self.gcagesecs = gcagesecs
    self.cmdline_mgmhost = cmdline_mgmhost
    self.logfilepath = logfilepath
    self.fqdn = socket.getfqdn()
    self.mgmhost = cmdline_mgmhost
    self.localfilesystempaths = []

  def eosfsls(self, mgmhost):
    mgmurl = "root://{}".format(mgmhost)
    cmd = "eos -r 0 0 {} fs ls -m".format(mgmurl)
    env = os.environ.copy()
    env["XrdSecPROTOCOL"] = "sss"
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    stdout,stderr = process.communicate()
    if 0 != process.returncode:
      raise Exception(
        "\n"
        "Failed to to execute: {}\n"
        "Return code         : {}\n"
        "Return code strerror: {}\n"
        "Standard error      : {}".format(cmd, process.returncode, os.strerror(process.returncode), stderr))
  
    result = []
    lines = stdout.splitlines();
    for l in lines:
      linedict = {}
      pairs = l.split()
      for p in pairs:
        splitpair = p.split('=')
        if 2 == len(splitpair):
          linedict[splitpair[0]] = splitpair[1]
      if linedict:
        result.append(linedict)

    return result

  def eosstagerrm(self, mgmhost, fxid):
    logger = logging.getLogger('gc')
    mgmurl = "root://{}".format(mgmhost)
    cmd = "eos {} stagerrm fxid:{}".format(mgmurl, fxid)
    env = os.environ.copy()
    env["XrdSecPROTOCOL"] = "sss"
    process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    stdout,stderr = process.communicate()
    if 0 == process.returncode:
      logger.info("Executed {}".format(cmd))

  def processfile(self, subdir, fstfile):
    statvfs = os.statvfs(subdir)
    spaceshouldefreed = statvfs.f_frsize * statvfs.f_bavail > self.minfreebytes

    if spaceshouldefreed:
      fullpath = os.path.join(subdir,fstfile)
      statinfo = os.stat(fullpath)
      now = time.time()
      agesecs = now - statinfo.st_ctime
      if agesecs > self.gcagesecs:
        self.eosstagerrm(self.mgmhost, fstfile)

  def processfssubdir(self, subdir):
    logger = logging.getLogger('gc')
    fstfiles = [f for f in os.listdir(subdir)
      if re.match('^[0-9A-Fa-f]{8}$', f) and os.path.isfile(os.path.join(subdir, f))]
    for fstfile in fstfiles:
      self.processfile(subdir, fstfile)

  def processfs(self, path):
    fssubdirs = [os.path.join(path, f) for f in os.listdir(path)
      if re.match('^[0-9A-Fa-f]{8}$', f) and os.path.isdir(os.path.join(path, f))]
    for fssubdir in fssubdirs:
      self.processfssubdir(fssubdir)

  def logfilesystempaths(self):
    logger = logging.getLogger('gc')
    logger.info('Number of local file systems is {}'.format(len(self.localfilesystempaths)))
    i = 0
    for path in self.localfilesystempaths:
      logger.info('Local file system {}: {}'.format(i, path))
      i = i + 1

  def processallfs(self):
    filesystems = self.eosfsls(self.mgmhost)
    newlocalfilesystempaths = [fs["path"] for fs in filesystems if "path" in fs and "host" in fs and self.fqdn == fs["host"]]
    if newlocalfilesystempaths != self.localfilesystempaths:
      self.localfilesystempaths = newlocalfilesystempaths
      self.logfilesystempaths();
    for path in self.localfilesystempaths:
      self.processfs(path)

  def logconf(self):
    logger = logging.getLogger('gc')
    logger.info("config minfreebytes={}".format(self.minfreebytes))
    logger.info("config gcagesecs={}".format(self.gcagesecs))
    logger.info("config mgmhost={}".format(self.mgmhost))
    logger.info("config logfilepath={}".format(self.logfilepath))

  def run(self):
    username = getpass.getuser()
    if 'daemon' != username:
      raise UserError('{} must be executed as user daemon and not user {}'.format(self.programname, username))

    self.setmgmhost()

    self.configureLogging()
    logger = logging.getLogger('gc')
    logger.info('{} started'.format(self.programname))
    logger.info('The fqdn of this machine is {}'.format(self.fqdn))
    self.logconf()

    minperiod = 300 # In seconds

    while True:
      before = time.time()
      self.processallfs()
      after = time.time()
      period = after - before
      if period < minperiod:
        sleeptime = minperiod - period
        logger.debug('Sleeping {} seconds'.format(sleeptime))
        time.sleep(sleeptime)

def main():
  programname = 'cta-fst-gcd'

  parser = argparse.ArgumentParser()
  parser.add_argument("-f", "--minfreebytes", help="The minimum amount of free space in bytes", type=int, default=10*1000*1000*1000)
  parser.add_argument("-a", "--gcagesecs", help="The age in seconds that a file must have in order to be considered for garbage collection", type=int, default=2*60*60)
  parser.add_argument("-m", "--mgmhost", help="The EOS MGM host")
  args = parser.parse_args()

  gc = Gc(programname, os.environ, args.minfreebytes, args.mgmhost)

  try:
    gc.run()
  except UserError as err:
    print "User error: {}".format(err)

if __name__ == '__main__':
  main()
