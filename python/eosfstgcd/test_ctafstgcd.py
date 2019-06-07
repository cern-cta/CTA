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

import cStringIO
import ctafstgcd
import os
import sys
import time
import unittest

class DummyLogger:
  def debug(self, msg):
    pass

  def info(self, msg):
    pass

  def warning(self, msg):
    pass

  def error(self, msg):
    pass

  def critical(self, msg):
    pass

class MockTreeNode:
  def __init__(self, name, children = []):
    self.name = name
    self.children = children

class MockDisk:
  def __init__(self, mocktree, freebytes, filesizeandctime):
    self.mocktree = mocktree
    self.freebytes = freebytes
    self.filesizeandctime = filesizeandctime

    self.nblistdir = 0
    self.nbisdir = 0
    self.nbisfile = 0
    self.nbgetfilesizeandctime = 0
    self.nbgetfreebytes = 0

  def listdir(self, path):
    self.nblistdir = self.nblistdir + 1

    pathlist = self.pathtolist(path)

    currentnodelist = [self.mocktree]
    resultnodelist = None
    for nodename in pathlist:
      matching = filter(lambda node: node.name == nodename, currentnodelist)
      if len(matching) == 0:
        raise Exception("Cannot list {}".format(path))
      if len(matching) > 1:
        raise Exception("More than mock directory with the same name: name={}".format(matching[0].name))
      if len(matching) == 1:
        resultnodelist = matching[0].children
        currentnodelist = resultnodelist

    resultfilelist = []
    for node in resultnodelist:
      resultfilelist.append(node.name)

    return resultfilelist

  def isdir(self, path):
    self.nbisdir = self.nbisdir + 1
    return True

  def isfile(self, path):
    self.nbisfile = self.nbisfile + 1
    return True

  def getfilesizeandctime(self, path):
    self.nbgetfilesizeandctime = self.nbgetfilesizeandctime + 1
    return self.filesizeandctime

  def getfreebytes(self, path):
    self.nbgetfreebytes = self.nbgetfreebytes + 1
    return self.freebytes

  def pathtolist(self, path):
    result = []
    while path != '' and path != '/':
      basename = os.path.basename(path)
      if basename != '':
        result.insert(0, basename)
      path = os.path.dirname(path)

    result.insert(0, '/')

    return result

class MockEos:
  def __init__(self, filesystems = []):
    self.filesystems = filesystems
    self.nbfsls = 0
    self.nbstagerrm = 0

  def fsls(self):
    self.nbfsls = self.nbfsls + 1
    return self.filesystems

  def stagerrm(self, fxid, subdir):
    self.nbstagerrm = self.nbstagerrm + 1

class RealDiskCase(unittest.TestCase):
  def setUp(self):
    self.log = DummyLogger()

  def test_getfreebytes(self):
    path = "/"
    disksystem = ctafstgcd.RealDisk(self.log)
    freebytes = disksystem.getfreebytes(path)
    self.assertTrue(freebytes != None)

class RealEosCase(unittest.TestCase):
  def setUp(self):
    self.log = DummyLogger()
    self.mgmhost = 'mgmhost'
    self.xrdsecssskt = 'xrdsecssskt'

  def test_constructor(self):
    eos = ctafstgcd.RealEos(self.log, self.mgmhost, self.xrdsecssskt)

class SpaceTrackerCase(unittest.TestCase):
  def setUp(self):
    self.mocktree = []
    self.freebytes = 10000
    self.filectime = ctafstgcd.FileSizeAndCtime()
    self.filectime.sizebytes = 1000
    self.filectime.ctime = time.time()

  def test_getfreebytes(self):
    disksystem = MockDisk(self.mocktree, self.freebytes, self.filectime)
    queryperiodsecs = 0
    path = "/"
    spacetracker = ctafstgcd.SpaceTracker(disksystem, queryperiodsecs, path)
    self.assertEqual(self.freebytes, spacetracker.getfreebytes())
    self.assertEqual(1, disksystem.nbgetfreebytes)

class SpaceTrackersCase(unittest.TestCase):
  def setUp(self):
    self.log = DummyLogger()
    self.mocktree = []
    self.freebytes = 10000
    self.filectime = ctafstgcd.FileSizeAndCtime()
    self.filectime.sizebytes = 1000
    self.filectime.ctime = time.time()
    self.fqdn = "a.b.c"

  def test_getmntpoint_root(self):
    queryperiodsecs = 0
    path = "/"
    disksystem = MockDisk(self.mocktree, self.freebytes, self.filectime)
    spacetrackers = ctafstgcd.SpaceTrackers(self.log, disksystem, queryperiodsecs)
    self.assertEqual(path, spacetrackers.getmntpoint(path))

  def test_gettracker_root_once(self):
    queryperiodsecs = 0
    path = "/"
    disksystem = MockDisk(self.mocktree, self.freebytes, self.filectime)
    spacetrackers = ctafstgcd.SpaceTrackers(self.log, disksystem, queryperiodsecs)

    self.assertEqual(0, spacetrackers.getnbtrackers())
    spacetracker = spacetrackers.gettracker(path)
    self.assertEqual(1, spacetrackers.getnbtrackers())
    self.assertEqual(path, spacetracker.path)

  def test_gettracker_root_twice(self):
    disksystem = MockDisk(self.mocktree, self.freebytes, self.filectime)
    queryperiodsecs = 0
    path = "/"
    spacetrackers = ctafstgcd.SpaceTrackers(self.log, disksystem, queryperiodsecs)

    self.assertEqual(0, spacetrackers.getnbtrackers())
    firsttracker = spacetrackers.gettracker(path)
    self.assertEqual(1, spacetrackers.getnbtrackers())
    self.assertEqual(path, firsttracker.path)

    secondtracker = spacetrackers.gettracker(path)
    self.assertEqual(1, spacetrackers.getnbtrackers())
    self.assertEqual(path, secondtracker.path)

    self.assertEqual(firsttracker, secondtracker)

class GcTestCase(unittest.TestCase):
  def setUp(self):
    self.log = DummyLogger()
    self.mocktree = MockTreeNode("/")
    self.freebytes = 10000
    self.filesizeandctime = ctafstgcd.FileSizeAndCtime()
    self.filesizeandctime.sizebytes = 1000
    self.filesizeandctime.ctime = time.time()
    self.fqdn = "a.b.c"

    self.config = ctafstgcd.GcConfig()
    self.config.logfile = 'logfile'
    self.config.mgmhost = 'mgmhost'
    self.config.minfreebytes = 0
    self.config.gcagesecs = 1000
    self.config.queryperiodsecs = 0
    self.config.mainloopperiodsecs = 0
    self.config.xrdsecssskt = ''

  def tearDown(self):
    pass

  def test_constructor(self):
    disk = MockDisk(self.mocktree, self.freebytes, self.filesizeandctime)
    eos = MockEos()

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

  def test_run_only_once_no_fs(self):
    disk = MockDisk(self.mocktree, self.freebytes, self.filesizeandctime)
    eos = MockEos()

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(1, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

  def test_run_only_once_one_fs(self):
    mockfs = MockTreeNode("filesystem1")
    mocktree = MockTreeNode("/", [mockfs])
    disk = MockDisk(mocktree, self.freebytes, self.filesizeandctime)

    filesystem1 = {
      "path" : "/filesystem1",
      "host" : self.fqdn
    }
    filesystems = [filesystem1]
    eos = MockEos(filesystems)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(1, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(1, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

  def test_run_only_once_one_fs_one_subdir(self):
    mocksubdir = MockTreeNode("12345678")
    mockfs = MockTreeNode("filesystem1", [mocksubdir])
    mocktree = MockTreeNode("/", [mockfs])
    disk = MockDisk(mocktree, self.freebytes, self.filesizeandctime)

    filesystem1 = {
      "path" : "/filesystem1",
      "host" : self.fqdn
    }
    filesystems = [filesystem1]
    eos = MockEos(filesystems)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nblistdir)
    self.assertEqual(1, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(1, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

  def test_run_only_once_one_fs_one_subdir_one_file_free_space(self):
    mockfile = MockTreeNode("90abcdef")
    mocksubdir = MockTreeNode("12345678", [mockfile])
    mockfs = MockTreeNode("filesystem1", [mocksubdir])
    mocktree = MockTreeNode("/", [mockfs])
    disk = MockDisk(mocktree, self.freebytes, self.filesizeandctime)

    filesystem1 = {
      "path" : "/filesystem1",
      "host" : self.fqdn
    }
    filesystems = [filesystem1]
    eos = MockEos(filesystems)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nblistdir)
    self.assertEqual(1, disk.nbisdir)
    self.assertEqual(1, disk.nbisfile)
    self.assertEqual(1, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(1, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

  def test_run_only_once_one_fs_one_subdir_one_file_no_free_space_young_file(self):
    mockfile = MockTreeNode("90abcdef")
    mocksubdir = MockTreeNode("12345678", [mockfile])
    mockfs = MockTreeNode("filesystem1", [mocksubdir])
    mocktree = MockTreeNode("/", [mockfs])
    disk = MockDisk(mocktree, self.freebytes, self.filesizeandctime)

    filesystem1 = {
      "path" : "/filesystem1",
      "host" : self.fqdn
    }
    filesystems = [filesystem1]
    eos = MockEos(filesystems)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    self.config.minfreebytes = self.freebytes + 1

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nblistdir)
    self.assertEqual(1, disk.nbisdir)
    self.assertEqual(1, disk.nbisfile)
    self.assertEqual(1, disk.nbgetfreebytes)
    self.assertEqual(1, disk.nbgetfilesizeandctime)
    self.assertEqual(1, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

  def test_run_only_once_one_fs_one_subdir_one_file_no_free_space_old_file(self):
    mockfile = MockTreeNode("90abcdef")
    mocksubdir = MockTreeNode("12345678", [mockfile])
    mockfs = MockTreeNode("filesystem1", [mocksubdir])
    mocktree = MockTreeNode("/", [mockfs])
    filesizeandctime = ctafstgcd.FileSizeAndCtime()
    filesizeandctime.sizebytes = 1000
    filesizeandctime.ctime = time.time() - self.config.gcagesecs - 1
    disk = MockDisk(mocktree, self.freebytes, filesizeandctime)

    filesystem1 = {
      "path" : "/filesystem1",
      "host" : self.fqdn
    }
    filesystems = [filesystem1]
    eos = MockEos(filesystems)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    self.config.minfreebytes = self.freebytes + 1

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nblistdir)
    self.assertEqual(0, disk.nbisdir)
    self.assertEqual(0, disk.nbisfile)
    self.assertEqual(0, disk.nbgetfreebytes)
    self.assertEqual(0, disk.nbgetfilesizeandctime)
    self.assertEqual(0, eos.nbfsls)
    self.assertEqual(0, eos.nbstagerrm)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nblistdir)
    self.assertEqual(1, disk.nbisdir)
    self.assertEqual(1, disk.nbisfile)
    self.assertEqual(1, disk.nbgetfreebytes)
    self.assertEqual(1, disk.nbgetfilesizeandctime)
    self.assertEqual(1, eos.nbfsls)
    self.assertEqual(1, eos.nbstagerrm)

if __name__ == '__main__':
  suites = []
  suites.append(unittest.TestLoader().loadTestsFromTestCase(RealDiskCase))
  suites.append(unittest.TestLoader().loadTestsFromTestCase(RealEosCase))
  suites.append(unittest.TestLoader().loadTestsFromTestCase(SpaceTrackerCase))
  suites.append(unittest.TestLoader().loadTestsFromTestCase(SpaceTrackersCase))
  suites.append(unittest.TestLoader().loadTestsFromTestCase(GcTestCase))

  suite = unittest.TestSuite(suites)

  result = unittest.TextTestRunner(verbosity=2).run(suite)
  if len(result.failures) == 0:
    sys.exit(0)
  else:
    sys.exit(1)
