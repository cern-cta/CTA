#!/usr/bin/env python3

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright(C) 2015-2023 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

import argparse
import configparser
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

class EvictError(Exception):
  pass

class VersionError(Exception):
  pass

class AttrsetError(Exception):
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

  def get_file_size_and_ctime(self, path):
    try:
      statinfo = os.stat(path)

      file_size_and_ctime = FileSizeAndCtime()
      file_size_and_ctime.sizebytes = statinfo.st_size
      file_size_and_ctime.ctime = statinfo.st_ctime

      return file_size_and_ctime
    except Exception as err:
      # The file may have been deleted by the FST just before the stat
      if err.errno == errno.ENOENT:
        return None
      else:
        raise Exception('Failed to stat file: path={}: {}'.format(path, err))

  def get_free_bytes(self, path):
    statvfs = None
    try:
      statvfs = os.statvfs(path)
      return statvfs.f_frsize * statvfs.f_bavail
    except Exception as err:
      self.log.error('get_free_bytes: Failed to stat VFS for free space: path={}'.format(path))

class RealEos:
  '''Class to faciliate unit-testing by wrapping the EOS storage system.'''

  def __init__(self, log, mgm_host, xrdsecssskt):
    self.log = log
    self.mgm_host = mgm_host
    self.xrdsecssskt = xrdsecssskt

  def fsls(self):
    mgmurl = 'root://{}'.format(self.mgm_host)
    cmd = 'eos -r 0 0 {} fs ls -m'.format(mgmurl)
    env = os.environ.copy()
    env['XrdSecPROTOCOL'] = 'sss'
    env['XrdSecSSSKT'] = self.xrdsecssskt
    process = None
    try:
      process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, universal_newlines=True)
    except Exception as err:
      raise Exception('Failed to execute \'{}\'": {}'.format(cmd, err))
    stdout,stderr = process.communicate()

    result = []

    if 0 != process.returncode:
      returncodestr = os.strerror(process.returncode)
      stderrstr = stderr.replace('\n', ' ').replace('\r', '').strip()
      self.log.error('fsls: Failed to execute {}: returncode={} returncodestr=\'{}\' stderr=\'{}\''
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
    mgmurl = 'root://{}'.format(self.mgm_host)
    cmd = 'eos {} stagerrm fxid:{}'.format(mgmurl, fxid)
    env = os.environ.copy()
    env['XrdSecPROTOCOL'] = 'sss'
    env['XrdSecSSSKT'] = self.xrdsecssskt
    process = None
    try:
      process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, universal_newlines=True)
    except Exception as err:
      raise Exception('Failed to execute \'{}\': {}'.format(cmd, err))
    stdout,stderr = process.communicate()

    if 0 != process.returncode:
      raise EvictError('\'{}\' returned non zero: returncode={}'.format(cmd, process.returncode))

  def evict(self, fsid, fxid):
    mgmurl = 'root://{}'.format(self.mgm_host)
    cmd = 'eos {} evict --force --fsid {} fxid:{}'.format(mgmurl, fsid, fxid)
    env = os.environ.copy()
    env['XrdSecPROTOCOL'] = 'sss'
    env['XrdSecSSSKT'] = self.xrdsecssskt
    process = None
    try:
      process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    except Exception as err:
      raise Exception('Failed to execute \'{}\': {}'.format(cmd, err))
    stdout,stderr = process.communicate()

    if 0 != process.returncode:
      raise EvictError('\'{}\' returned non zero: returncode={}'.format(cmd, process.returncode))

  def getMinMajorVersion(self):
    mgmurl = 'root://{}'.format(self.mgm_host)
    cmd = 'eos {} version'.format(mgmurl)
    env = os.environ.copy()
    env['XrdSecPROTOCOL'] = 'sss'
    env['XrdSecSSSKT'] = self.xrdsecssskt
    process = None
    try:
      process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    except Exception as err:
      raise Exception('Failed to execute \'{}\': {}'.format(cmd, err))
    stdout,stderr = process.communicate()

    versions = {}

    if 0 != process.returncode:
      raise VersionError('\'{}\' returned non zero: returncode={}'.format(cmd, process.returncode))
    else:
      entries = stdout.split();
      for e in entries:
        splitpair = e.split('=')
        if 2 == len(splitpair):
          versions[splitpair[0]] = splitpair[1]

    server_major_version = int(versions['EOS_SERVER_VERSION'].split('.')[0])
    client_major_version = int(versions['EOS_CLIENT_VERSION'].split('.')[0])
    min_major_version = min(server_major_version, client_major_version)

    if min_major_version not in (4, 5):
      raise VersionError('EOS major version \'{}\' is not valid'.format(min_major_version))

    return min_major_version

  def attrset(self, name, value, fxid):
    mgmurl = 'root://{}'.format(self.mgm_host)
    args = ['eos', '-r', '0', '0', mgmurl, 'attr', 'set', '{}={}'.format(name, value), 'fxid:{}'.format(fxid)]
    env = os.environ.copy()
    env['XrdSecPROTOCOL'] = 'sss'
    env['XrdSecSSSKT'] = self.xrdsecssskt
    process = None
    try:
      process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, universal_newlines=True)
    except Exception as err:
      raise Exception('Failed to execute \'{}\': {}'.format(' '.join(args), err))
    stdout,stderr = process.communicate()

    if 0 != process.returncode:
      raise AttrsetError('\'{}\' returned non zero: returncode={}'.format(' '.join(args), process.returncode))

class SpaceTracker:
  '''Calculates the amount of effective free space in the file system of a given
  file or directory by querying the OS and taking into account the pending
  stagerrm requests to the EOS MGM.'''

  def __init__(self, disk, query_period_secs, path):
    self.disk = disk
    self.query_period_secs = query_period_secs
    self.path = path
    self.freebytes = None
    self.lastquerytimestamp = None

  def stagerrm_queued(self, file_size_bytes):
    self.freebytes = self.freebytes + file_size_bytes

  def get_free_bytes(self):
    now = time.time()

    if self.freebytes:
      elapsed = now - self.lastquerytimestamp

      if elapsed > self.query_period_secs:
        self.lastquerytimestamp = now
        self.freebytes = self.disk.get_free_bytes(self.path)

    else: # not self.freebytes:
      self.lastquerytimestamp = now
      self.freebytes = self.disk.get_free_bytes(self.path)

    return self.freebytes

class SpaceTrackers:
  '''Container and factory of SpaceTracker objects.  There is one SpaceTracker
  per filesystem mount-point being tracked.'''

  def __init__(self, log, disk, query_period_secs):
    self.log = log
    self.disk = disk
    self.query_period_secs = query_period_secs
    self.mnt_point_to_tracker = {}

  def get_tracker(self, path):
    mntpoint = self.get_mnt_point(path)

    if not mntpoint in self.mnt_point_to_tracker:
      self.mnt_point_to_tracker[mntpoint] = SpaceTracker(self.disk, self.query_period_secs, mntpoint)
      self.log.info('Tracking storage space of mount point: mntpoint={}'.format(mntpoint))

    return self.mnt_point_to_tracker[mntpoint]

  def get_nb_trackers(self):
    return len(self.mnt_point_to_tracker)

  def get_mnt_point(self, path):
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
    self.log_file = ''
    self.mgm_host = ''
    self.eos_spaces = []
    self.eos_space_to_min_free_bytes = {}
    self.gc_age_secs = 0
    self.query_period_secs = 0
    self.main_loop_period_secs = 0
    self.xrdsecssskt = ''

class PathAndFsidAndEosSpace:

  def __init__(self, path, fsid, eos_space):
    self.path = path
    self.fsid = fsid
    self.eos_space = eos_space

class MissingColonError(UserError):
  pass

class TooManyColonsError(UserError):
  pass

class MinFreeBytesNotSetError(UserError):
  pass

class MinFreeBytesError(UserError):
  pass

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
    self.eos_version = 4 # Version 4 by default
    self.config = config

    self.local_file_system_paths = []
    self.space_trackers = SpaceTrackers(self.log, disk, self.config.query_period_secs)

    self.log.info('Config: log_file={}'.format(self.config.log_file))
    self.log.info('Config: mgm_host={}'.format(self.config.mgm_host))
    self.log.info('Config: eos_spaces={}'.format(' '.join(self.config.eos_spaces)))
    self.log.info('Config: eos_space_to_min_free_bytes={}'.format(config.eos_space_to_min_free_bytes_str))
    self.log.info('Config: gc_age_secs={}'.format(self.config.gc_age_secs))
    self.log.info('Config: absolute_max_age_secs={}'.format(self.config.absolute_max_age_secs))
    self.log.info('Config: query_period_secs={}'. format(self.config.query_period_secs))
    self.log.info('Config: main_loop_period_secs={}'. format(self.config.main_loop_period_secs))
    self.log.info('Config: xrdsecssskt={}'.format(self.config.xrdsecssskt))

  @staticmethod
  def parse_conf(conf_fp):
    try:
      parser = configparser.ConfigParser(inline_comment_prefixes=";")
      parser.readfp(conf_fp)
      config = GcConfig()
      config.log_file = parser.get('main', 'log_file')
      config.mgm_host = parser.get('main', 'mgm_host')
      config.eos_spaces = parser.get('main', 'eos_spaces').split()
      config.eos_space_to_min_free_bytes_str = parser.get('main', 'eos_space_to_min_free_bytes')
      config.eos_space_to_min_free_bytes = Gc.parse_eos_space_to_min_free_bytes(config.eos_space_to_min_free_bytes_str)
      config.gc_age_secs = parser.getint('main', 'gc_age_secs')
      config.absolute_max_age_secs = parser.getint('main', 'absolute_max_age_secs')
      config.query_period_secs = parser.getint('main', 'query_period_secs')
      config.main_loop_period_secs = parser.getint('main', 'main_loop_period_secs')
      config.xrdsecssskt = parser.get('main', 'xrdsecssskt')

      for eos_space in config.eos_spaces:
        # config.eos_space_to_min_free_bytes is a dictionary mapping EOS space
        # name to minimum number of free bytes
        if eos_space not in config.eos_space_to_min_free_bytes:
          raise MinFreeBytesNotSetError(
           'Error in eos_space_to_min_free_bytes value: ' \
           'Minimum number of free bytes has not been specified for EOS space {}: ' \
           'value=\'{}\''.format(eos_space, config.eos_space_to_min_free_bytes_str))

      return config
    except configparser.Error as err:
      raise UserError(err)

  @staticmethod
  def parse_eos_space_to_min_free_bytes(str):
    # Dictionary mapping EOS space name to minimum number of free bytes
    eos_space_to_min_free_bytes = {}

    for maplet_str in str.split():
      maplet = maplet_str.split(':')
      if 1 >= len(maplet):
        raise MissingColonError('Syntax error in eos_space_to_min_free_bytes value' \
         ': Invalid maplet: Missing colon: maplet=\'{}\'"'.format(maplet_str))
      if 2 < len(maplet):
        raise TooManyColonsError('Syntax error in eos_space_to_min_free_bytes value: Invalid maplet' \
          ': Too many colons: maplet=\'{}\''.format(maplet_str))

      eos_space = maplet[0]
      min_free_bytes_str = maplet[1]

      min_free_bytes_int = 0
      try:
        min_free_bytes_int = int(min_free_bytes_str)
      except ValueError as err:
        raise MinFreeBytesError('Minimum number of free bytes is not a valid integer: eos_space={} value={}: {}'.
          format(eos_space, min_free_bytes_str, err))

      if 0 > min_free_bytes_int:
        raise MinFreeBytesError('Minimum number of free bytes is negative: eos_space={} value={}'.
          format(eos_space, min_free_bytes_int))

      eos_space_to_min_free_bytes[eos_space] = min_free_bytes_int

    return eos_space_to_min_free_bytes

  def process_all_fs(self):
    # Get the paths of the EOS file systems local to this FST and log them if
    # they have changed
    file_systems = None
    try:
      file_systems = self.eos.fsls()
    except Exception as err:
      self.log.error('process_all_fs: Failed to determine the EOS file systems local to this FST: {}'.format(err))

    if not file_systems:
      return

    self.check_eos_version()

    new_local_file_system_paths = [
      PathAndFsidAndEosSpace(fs['path'], fs['id'], self.get_space_from_schedgroup(fs['schedgroup']))
      for fs in file_systems if
        'path' in fs and
        'id' in fs and
        'host' in fs and self.fqdn == fs['host'] and
        'schedgroup' in fs and self.schedgroup_matches_eos_spaces(fs['schedgroup'])
    ]
    if new_local_file_system_paths != self.local_file_system_paths:
      self.local_file_system_paths = new_local_file_system_paths
      self.log_file_system_paths()

    for path_and_eos_space in self.local_file_system_paths:
      self.process_fs(path_and_eos_space.path, path_and_eos_space.fsid, path_and_eos_space.eos_space)

  def check_eos_version(self):
    # Check if the EOS version has changed
    try:
      new_eos_version = self.getMinMajorVersion()
    except Exception as err:
      self.log.error('process_eos_version: Failed to determine the EOS major version: {}'.format(err))
      return

    if new_eos_version != self.eos_version:
      self.eos_version = new_eos_version
      self.log.info('EOS major version switched to {}'.format(new_eos_version))

  def log_file_system_paths(self):
    self.log.info('Number of local file systems is {}'.format(len(self.local_file_system_paths)))
    i = 0
    for path_and_eos_space in self.local_file_system_paths:
      self.log.info('Local file system {}: path={} eos_space={}'.format(i, path_and_eos_space.path, path_and_eos_space.eos_space))
      i = i + 1

  def process_fs(self, path, fsid, eos_space):
    fsfiles = []
    try:
      fsfiles = self.disk.listdir(path)
    except Exception as err:
      self.log.error('process_fs: Failed to list contents of filesystem: path={}: {}'.format(path, err))
      return

    sub_dirs = []
    try:
      sub_dirs = [os.path.join(path, f) for f in fsfiles
        if re.match('^[0-9A-Fa-f]{8}$', f) and self.disk.isdir(os.path.join(path, f))]
    except Exception as err:
      self.log.error('process_fs: Failed to determine sub directories of filesystem: path={}: {}'.format(path, err))
      return

    for sub_dir in sub_dirs:
      self.process_fs_sub_dir(sub_dir, fsid, eos_space)

  def schedgroup_matches_eos_spaces(self, schedgroup):
    for eos_space in self.config.eos_spaces:
      if re.match('^{}\.'.format(eos_space), schedgroup):
        return True
    return False

  def get_space_from_schedgroup(self, schedgroup):
    return re.match('^([^.]+)', schedgroup).group(0)

  def process_fs_sub_dir(self, sub_dir, fsid, eos_space):
    sub_dir_files = []
    try:
      sub_dir_files = self.disk.listdir(sub_dir)
    except Exception as err:
      self.log.error('process_fs_sub_dir: Failed to list contents of sub directory: sub_dir={}: {}'.format(sub_dir, err))

    fst_files = [f for f in sub_dir_files if re.match('^[0-9A-Fa-f]{8,}$', f) and self.disk.isfile(os.path.join(sub_dir, f))]
    for fst_file in fst_files:
      self.process_file(sub_dir, fst_file, fsid, eos_space)

  def process_file(self, sub_dir, fst_file, fsid, eos_space):
    fullpath = os.path.join(sub_dir, fst_file)
    file_size_and_ctime = None
    try:
      file_size_and_ctime = self.disk.get_file_size_and_ctime(fullpath)
    except Exception as err:
      self.log.error('process_file: Error calling self.disk.get_file_size_and_ctime(): {}'.format(err))

    if not file_size_and_ctime:
      return

    if eos_space not in self.config.eos_space_to_min_free_bytes:
      return

    now = time.time()
    age_secs = now - file_size_and_ctime.ctime
    absolute_max_age_reached = age_secs > self.config.absolute_max_age_secs
    gc_age_reached = age_secs > self.config.gc_age_secs
    space_tracker = self.space_trackers.get_tracker(sub_dir)
    total_free_bytes = space_tracker.get_free_bytes()
    should_free_space = total_free_bytes < self.config.eos_space_to_min_free_bytes[eos_space]

    if absolute_max_age_reached or (should_free_space and gc_age_reached):
      try:
        bytes_required_before = 0
        if self.config.eos_space_to_min_free_bytes[eos_space] > total_free_bytes:
          bytes_required_before = self.config.eos_space_to_min_free_bytes[eos_space] - total_free_bytes
        if self.eos_version == 5:
          self.eos.evict(fsid, fst_file)
        else:
          self.eos.stagerrm(fst_file)
        space_tracker.stagerrm_queued(file_size_and_ctime.sizebytes)
        self.log.info('stagerrm: ' \
          'sub_dir={}, ' \
          'fxid={}, ' \
          'bytes_required_before={}, ' \
          'file_size_bytes={}, ' \
          'absolute_max_age_reached={}, ' \
          'should_free_space={}, ' \
          'gc_age_reached={}'
          .format(sub_dir, fst_file, bytes_required_before, file_size_and_ctime.sizebytes, absolute_max_age_reached,
            should_free_space, gc_age_reached))
        nowstr = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S.%f')
        attrname = 'sys.retrieve.error'
        attrvalue = 'Garbage collected at {}'.format(nowstr)
        self.eos.attrset(attrname, attrvalue, fst_file)
      except EvictError as err:
        pass
      except Exception as err:
        self.log.error('process_file: {}'.format(err))

  def run(self, run_only_once = False):
    continue_main_loop = True
    while continue_main_loop:
      before = time.time()
      self.process_all_fs()
      after = time.time()
      period = after - before
      if period < self.config.main_loop_period_secs:
        sleeptime = self.config.main_loop_period_secs - period
        self.log.debug('Sleeping {} seconds'.format(sleeptime))
        time.sleep(sleeptime)
      if run_only_once:
        continue_main_loop = False

def main():
  programname = 'cta-fst-gcd'

  username = getpass.getuser()
  if 'daemon' != username:
    raise UserError('{} must be executed as user daemon and not user {}'.format(programname, username))

  parser = argparse.ArgumentParser()
  parser.add_argument('-c', '--config', default='/etc/cta/{}.conf'.format(programname), help='Configuration file path')
  parser.add_argument('-s', '--stdout', action='store_true', help='Print logs to standard output, overriding configuration file path')
  args = parser.parse_args()

  if not os.path.isfile(args.config):
    raise UserError('The configuration file {} is not a directory or does not exist'.format(args.config))
  if not os.access(args.config, os.R_OK):
    raise UserError('Cannot access for reading the configuration file {}'.format(args.config))

  conf_fp = open(args.config)

  try:
    config = Gc.parse_conf(conf_fp)
  except UserError as err:
    raise UserError('Error parsing configuration file {}: {}'.format(args.config, err))

  hostname = socket.gethostname()
  fqdn = socket.getfqdn()

  if args.stdout == True:
    # If --stdout has been given on the command line, use False as the log path to set output to stdout
    log = get_logger(hostname, programname, False)
  else:
    log = get_logger(hostname, programname, config.log_file)

  log.info('{} started'.format(programname))
  log.info('The fqdn of this machine is {}'.format(fqdn))

  eos = RealEos(log, config.mgm_host, config.xrdsecssskt)
  disk = RealDisk(log)
  gc = Gc(log, fqdn, disk, eos, config)
  gc.run()

def get_logger(hostname, programname, logpath):
  config = {}

  log_fmt = '%(asctime)s.%(msecs)03d000 ' + hostname + ' %(levelname)s ' + programname + \
    ':LVL="%(levelname)s" PID="%(process)d" TID="%(process)d" MSG="%(message)s"'
  log_date_fmt = '%Y/%m/%d %H:%M:%S'
  log_formatter = logging.Formatter(fmt = log_fmt, datefmt = log_date_fmt)

  log_handler = None

  if logpath == False:
    log_handler = logging.StreamHandler(stream = sys.stdout)
  else:
    logging_dir = os.path.dirname(logpath)
    if not os.path.isdir(logging_dir):
      raise UserError('The logging directory {} is not a directory or does not exist'.format(logging_dir))
    if not os.access(logging_dir, os.W_OK):
      raise UserError('The logging directory {} cannot be written to'.format(logging_dir))
    log_handler = logging.handlers.TimedRotatingFileHandler(filename = logpath, when = 'midnight')

  log_handler = logging.handlers.TimedRotatingFileHandler(filename = logpath, when = 'midnight')
  log_handler.setLevel(logging.INFO)
  log_handler.setFormatter(log_formatter)

  logger = logging.getLogger('gc')
  logger.setLevel(logging.INFO)
  logger.addHandler(log_handler)

  return logger

try:
  if __name__ == '__main__':
    main()
except UserError as err:
  print(err)
except KeyboardInterrupt:
  pass
