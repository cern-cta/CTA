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
import ConfigParser
import datetime
import distutils
import errno
import getpass
import logging
import logging.config
import os
import re
import socket
import subprocess
import sys
import time

class UserError(Exception):
  pass

class NoMgmHost(UserError):
  pass

class FileSizeAndCtime:
  def __init__(self):
    self.sizebytes = 0
    self.ctime = 0

class RealDisk:
  '''Class to faciliate unit-testing by wrapping the disk storage system.'''

  def __init__(self, log):
    self.log = log

  def listdir(self, path):
    return os.listdir(path)

  def isdir(self, path):
    return os.path.isdir(path)

  def isfile(self, path):
    return os.path.isfile(path)

  def getfilesizeandctime(self, path):
    try:
      statinfo = os.stat(path)

      filesizeandctime = FileSizeAndCtime()
      filesizeandctime.sizebytes = statinfo.st_size
      filesizeandctime.ctime = statinfo.st_ctime

      return filesizeandctime
    except Exception as err:
      # The file may have been deleted by the FST just before the stat
      if err.errno == errno.ENOENT:
        return None
      else:
        raise Exception("Failed to stat file: path={}: {}".format(path, err))

  def getfreebytes(self, path):
    statvfs = None
    try:
      statvfs = os.statvfs(path)
      return statvfs.f_frsize * statvfs.f_bavail
    except Exception as err:
      self.log.error("Failed to stat VFS for free space: path={}".format(path))

class RealEos:
  '''Class to faciliate unit-testing by wrapping the EOS storage system.'''

  def __init__(self, log, mgmhost, xrdsecssskt):
    self.log = log
    self.mgmhost = mgmhost
    self.xrdsecssskt = xrdsecssskt

  def fsls(self):
    mgmurl = "root://{}".format(self.mgmhost)
    cmd = "eos -r 0 0 {} fs ls -m".format(mgmurl)
    env = os.environ.copy()
    env["XrdSecPROTOCOL"] = "sss"
    env["XrdSecSSSKT"] = self.xrdsecssskt
    process = None
    try:
      process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    except Exception as err:
      raise Exception("Failed to execute '{}': {}".format(cmd, err))
    stdout,stderr = process.communicate()

    result = []

    if 0 != process.returncode:
      returncodestr = os.strerror(process.returncode)
      stderrstr = stderr.replace('\n', ' ').replace('\r', '').strip()
      self.log.error("Failed to execute {}: returncode={} returncodestr='{}' stderr='{}'"
        .format(cmd, process.returncode, returncodestr, stderrstr))
    else:
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

  def stagerrm(self, fxid):
    mgmurl = "root://{}".format(self.mgmhost)
    cmd = "eos {} stagerrm fxid:{}".format(mgmurl, fxid)
    env = os.environ.copy()
    env["XrdSecPROTOCOL"] = "sss"
    env["XrdSecSSSKT"] = self.xrdsecssskt
    process = None
    try:
      process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    except Exception as err:
      raise Exception("Failed to execute '{}': {}".format(cmd, err))
    stdout,stderr = process.communicate()

    if 0 != process.returncode:
      raise Exception("'{}' returned non zero: returncode={}".format(cmd, process.returncode))

class SpaceTracker:
  '''Calculates the amount of effective free space in the file system of a given
  file or directory by querying the OS and taking into account the pending
  stagerrm requests to the EOS MGM.'''

  def __init__(self, disk, queryperiodsecs, path):
    self.disk = disk
    self.queryperiodsecs = queryperiodsecs
    self.path = path
    self.freebytes = None
    self.lastquerytimestamp = None

  def stagerrmqueued(self, filesizebytes):
    self.freebytes = self.freebytes + filesizebytes

  def getfreebytes(self):
    now = time.time()

    if self.freebytes:
      elapsed = now - self.lastquerytimestamp

      if elapsed > self.queryperiodsecs:
        self.lastquerytimestamp = now
        self.freebytes = self.disk.getfreebytes(self.path)

    else: # not self.freebytes:
      self.lastquerytimestamp = now
      self.freebytes = self.disk.getfreebytes(self.path)

    return self.freebytes

class SpaceTrackers:
  '''Container and factory of SpaceTracker objects.  There is one SpaceTracker
  per filesystem mount-point being tracked.'''

  def __init__(self, log, disk, queryperiodsecs):
    self.log = log
    self.disk = disk
    self.queryperiodsecs = queryperiodsecs
    self.mntpointtotracker = {}

  def gettracker(self, path):
    mntpoint = self.getmntpoint(path)

    if not mntpoint in self.mntpointtotracker:
      self.mntpointtotracker[mntpoint] = SpaceTracker(self.disk, self.queryperiodsecs, mntpoint)
      self.log.info("Tracking storage space of mount point: mntpoint={}".format(mntpoint))

    return self.mntpointtotracker[mntpoint]

  def getnbtrackers(self):
    return len(self.mntpointtotracker)

  def getmntpoint(self, path):
    realpath = os.path.realpath(path)
    while not os.path.ismount(realpath):
      realpath = os.path.dirname(realpath)
    return realpath

class DummyLoggingHandler(logging.StreamHandler):
   '''Dummy logging handler that does nothing'''

   def __init__(self):
     logging.StreamHandler.__init__(self)

   def emit(self, record):
     pass

class GcConfig:
  '''The configuration of a cta-fst-gcd daemon.'''

  def __init__(self):
    self.logfile = ''
    self.mgmhost = ''
    self.minfreebytes = 0
    self.gcagesecs = 0
    self.queryperiodsecs = 0
    self.mainloopperiodsecs = 0
    self.xrdsecssskt = ''

class Gc:
  '''Implements the cta-fst-gcd daemon that runs on an EOS FST and garbage
  collects EOS disk copies that have been safely stored to tape.

  The cta-fst-gcd daemon scans across every single EOS disk file on an FST.
  A file is garbage collected if:

  * The amount of free space on the corresponding file system is
    considered too low.

  * The file is considered old enough to be garbage collected.

  The cta-fst-gcd daemon garbage collects an EOS disk file by extracting the
  hexadecimal EOS file identifier from the local disk filename and then
  running eos stagerm fxid:<fid-hex>.'''

  def __init__(self, log, fqdn, disk, eos, config):
    self.log = log
    self.fqdn = fqdn
    self.disk = disk
    self.eos = eos
    self.config = config

    self.localfilesystempaths = []
    self.spacetrackers = SpaceTrackers(self.log, disk, self.config.queryperiodsecs)

    self.log.info("Config: logfile={}".format(self.config.logfile))
    self.log.info("Config: mgmhost={}".format(self.config.mgmhost))
    self.log.info("Config: minfreebytes={}".format(self.config.minfreebytes))
    self.log.info("Config: gcagesecs={}".format(self.config.gcagesecs))
    self.log.info("Config: queryperiodsecs={}". format(self.config.queryperiodsecs))
    self.log.info("Config: mainloopperiodsecs={}". format(self.config.mainloopperiodsecs))
    self.log.info("Config: xrdsecssskt={}".format(self.config.xrdsecssskt))

  def processfile(self, subdir, fstfile):
    spacetracker = self.spacetrackers.gettracker(subdir)
    totalfreebytes = spacetracker.getfreebytes()
    shouldfreespace = totalfreebytes < self.config.minfreebytes

    if shouldfreespace:
      fullpath = os.path.join(subdir,fstfile)

      filesizeandctime = None
      try:
        filesizeandctime = self.disk.getfilesizeandctime(fullpath)
      except Exception as err:
        self.log.error(err)

      if filesizeandctime:
        now = time.time()
        agesecs = now - filesizeandctime.ctime
        if agesecs > self.config.gcagesecs:
          try:
            bytesrequiredbefore = self.config.minfreebytes - totalfreebytes
            self.eos.stagerrm(fstfile)
            spacetracker.stagerrmqueued(filesizeandctime.sizebytes)
            self.log.info("stagerrm: subdir={}, fxid={}, bytesrequiredbefore={}, filesizebytes={}"
              .format(subdir, fstfile, bytesrequiredbefore, filesizeandctime.sizebytes))
          except Exception as err:
            self.log.error(err)

  def processfssubdir(self, subdir):
    fstfiles = [f for f in self.disk.listdir(subdir)
      if re.match('^[0-9A-Fa-f]{8}$', f) and self.disk.isfile(os.path.join(subdir, f))]
    for fstfile in fstfiles:
      self.processfile(subdir, fstfile)

  def processfs(self, path):
    fssubdirs = [os.path.join(path, f) for f in self.disk.listdir(path)
      if re.match('^[0-9A-Fa-f]{8}$', f) and self.disk.isdir(os.path.join(path, f))]
    for fssubdir in fssubdirs:
      self.processfssubdir(fssubdir)

  def logfilesystempaths(self):
    self.log.info('Number of local file systems is {}'.format(len(self.localfilesystempaths)))
    i = 0
    for path in self.localfilesystempaths:
      self.log.info('Local file system {}: path={}'.format(i, path))
      i = i + 1

  def processallfs(self):
    # Get the paths of the EOS filesystems local to this FST and log them if
    # they have changed
    filesystems = None
    try:
      filesystems = self.eos.fsls()
    except Exception as err:
      self.log.error("Failed to determine the EOS filesystems local to this FST: {}".format(err))

    if not filesystems:
      return

    newlocalfilesystempaths = [fs["path"] for fs in filesystems if "path" in fs and "host" in fs and self.fqdn == fs["host"]]
    if newlocalfilesystempaths != self.localfilesystempaths:
      self.localfilesystempaths = newlocalfilesystempaths
      self.logfilesystempaths();

    for path in self.localfilesystempaths:
      self.processfs(path)

  def run(self, runonlyonce = False):
    continueMainLoop = True
    while continueMainLoop:
      before = time.time()
      self.processallfs()
      after = time.time()
      period = after - before
      if period < self.config.mainloopperiodsecs:
        sleeptime = self.config.mainloopperiodsecs - period
        self.log.debug('Sleeping {} seconds'.format(sleeptime))
        time.sleep(sleeptime)
      if runonlyonce:
        continueMainLoop = False

def main():
  programname = 'cta-fst-gcd'

  username = getpass.getuser()
  if 'daemon' != username:
    raise UserError('{} must be executed as user daemon and not user {}'.format(programname, username))

  parser = argparse.ArgumentParser()
  parser.add_argument("-c", "--config", default="/etc/cta/{}.conf".format(programname), help="Configuration file path")
  args = parser.parse_args()

  config = parseconf(args.config)
  hostname = socket.gethostname()
  fqdn = socket.getfqdn()

  log = getlogger(hostname, programname, config.logfile)
  log.info('{} started'.format(programname))
  log.info('The fqdn of this machine is {}'.format(fqdn))

  eos = RealEos(log, config.mgmhost, config.xrdsecssskt)
  disk = RealDisk(log)
  gc = Gc(log, fqdn, disk, eos, config)
  gc.run()

def parseconf(conffile):
  if not os.path.isfile(conffile):
    raise UserError("The configuration file {} is not a directory or does not exist".format(conffile))
  if not os.access(conffile, os.R_OK):
    raise UserError("Cannot access for reading the configuration file {}".format(conffile))

  conffp = open(conffile)

  parser = ConfigParser.ConfigParser()
  parser.readfp(conffp)

  try:
    config = GcConfig()
    config.logfile = parser.get('main', 'logfile')
    config.mgmhost = parser.get('main', 'mgmhost')
    config.minfreebytes = parser.getint('main', 'minfreebytes')
    config.gcagesecs = parser.getint('main', 'gcagesecs')
    config.queryperiodsecs = parser.getint('main', 'queryperiodsecs')
    config.mainloopperiodsecs = parser.getint('main', 'mainloopperiodsecs')
    config.xrdsecssskt = parser.get('main', 'xrdsecssskt')
    return config
  except ConfigParser.Error as err:
    raise UserError("Error in configuration file {}: {}".format(conffile, err))

def getlogger(hostname, programname, logpath):
  config = {}

  logfmt = '%(asctime)s.%(msecs)03d000 ' + hostname + ' %(levelname)s ' + programname + ':LVL="%(levelname)s" PID="%(process)d" TID="%(process)d" MSG="%(message)s"'
  logdatefmt = '%Y/%m/%d %H:%M:%S'
  logformatter = logging.Formatter(fmt = logfmt, datefmt = logdatefmt)

  loghandler = None

  loggingdir = os.path.dirname(logpath)
  if not os.path.isdir(loggingdir):
    raise UserError("The logging directory {} is not a directory or does not exist".format(loggingdir))
  if not os.access(loggingdir, os.W_OK):
    raise UserError("The logging directory {} cannot be written to".format(loggingdir))

  loghandler = logging.handlers.TimedRotatingFileHandler(filename = logpath, when = 'midnight')
  loghandler.setLevel(logging.INFO)
  loghandler.setFormatter(logformatter)

  logger = logging.getLogger('gc')
  logger.setLevel(logging.INFO)
  logger.addHandler(loghandler)

  return logger

try:
  if __name__ == '__main__':
    main()
except UserError as err:
  print(err)
except KeyboardInterrupt:
  pass
