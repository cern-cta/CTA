#!/bin/python

# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright(C) 2015-2022 CERN
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

import cStringIO
import ctafstgcd
import io
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
  def __init__(self, mock_tree, free_bytes, file_size_and_ctime):
    self.mock_tree = mock_tree
    self.free_bytes = free_bytes
    self.file_size_and_ctime = file_size_and_ctime

    self.nb_listdir = 0
    self.nb_isdir = 0
    self.nb_isfile = 0
    self.nb_get_file_size_and_ctime = 0
    self.nb_get_free_bytes = 0

  def listdir(self, path):
    print "listdir path={}".format(path)
    self.nb_listdir = self.nb_listdir + 1

    path_list = self.path_to_list(path)

    current_node_list = [self.mock_tree]
    result_node_list = None
    for node_name in path_list:
      matching = filter(lambda node: node.name == node_name, current_node_list)
      if len(matching) == 0:
        raise Exception("Cannot list {}".format(path))
      if len(matching) > 1:
        raise Exception("More than mock directory with the same name: name={}".format(matching[0].name))
      if len(matching) == 1:
        result_node_list = matching[0].children
        current_node_list = result_node_list

    resultfilelist = []
    for node in result_node_list:
      resultfilelist.append(node.name)

    return resultfilelist

  def isdir(self, path):
    self.nb_isdir = self.nb_isdir + 1
    return True

  def isfile(self, path):
    self.nb_isfile = self.nb_isfile + 1
    return True

  def get_file_size_and_ctime(self, path):
    self.nb_get_file_size_and_ctime = self.nb_get_file_size_and_ctime + 1
    return self.file_size_and_ctime

  def get_free_bytes(self, path):
    print("get_free_bytes() path={}".format(path))
    self.nb_get_free_bytes = self.nb_get_free_bytes + 1
    return self.free_bytes

  def path_to_list(self, path):
    result = []
    while path != '' and path != '/':
      basename = os.path.basename(path)
      if basename != '':
        result.insert(0, basename)
      path = os.path.dirname(path)

    result.insert(0, '/')

    return result

class MockEos:
  def __init__(self, file_systems = []):
    self.file_systems = file_systems
    self.nb_fsls = 0
    self.nb_stagerrm = 0
    self.nb_attrset = 0

  def fsls(self):
    self.nb_fsls = self.nb_fsls + 1
    return self.file_systems

  def stagerrm(self, fxid):
    self.nb_stagerrm = self.nb_stagerrm + 1

  def attrset(self, name, value, fxid):
    self.nb_attrset = self.nb_attrset + 1

class RealDiskCase(unittest.TestCase):
  def setUp(self):
    self.log = DummyLogger()

  def test_get_free_bytes(self):
    path = "/"
    disksystem = ctafstgcd.RealDisk(self.log)
    free_bytes = disksystem.get_free_bytes(path)
    self.assertTrue(free_bytes != None)

class RealEosCase(unittest.TestCase):
  def setUp(self):
    self.log = DummyLogger()
    self.mgm_host = 'mgm_host'
    self.xrdsecssskt = 'xrdsecssskt'

  def test_constructor(self):
    eos = ctafstgcd.RealEos(self.log, self.mgm_host, self.xrdsecssskt)

class SpaceTrackerCase(unittest.TestCase):
  def setUp(self):
    self.mock_tree = []
    self.free_bytes = 10000
    self.filectime = ctafstgcd.FileSizeAndCtime()
    self.filectime.sizebytes = 1000
    self.filectime.ctime = time.time()

  def test_get_free_bytes(self):
    disksystem = MockDisk(self.mock_tree, self.free_bytes, self.filectime)
    queryperiodsecs = 0
    path = "/"
    spacetracker = ctafstgcd.SpaceTracker(disksystem, queryperiodsecs, path)
    self.assertEqual(self.free_bytes, spacetracker.get_free_bytes())
    self.assertEqual(1, disksystem.nb_get_free_bytes)

class SpaceTrackersCase(unittest.TestCase):
  def setUp(self):
    self.log = DummyLogger()
    self.mock_tree = []
    self.free_bytes = 10000
    self.filectime = ctafstgcd.FileSizeAndCtime()
    self.filectime.sizebytes = 1000
    self.filectime.ctime = time.time()
    self.fqdn = "a.b.c"

  def test_get_mnt_point_root(self):
    queryperiodsecs = 0
    path = "/"
    disksystem = MockDisk(self.mock_tree, self.free_bytes, self.filectime)
    spacetrackers = ctafstgcd.SpaceTrackers(self.log, disksystem, queryperiodsecs)
    self.assertEqual(path, spacetrackers.get_mnt_point(path))

  def test_get_tracker_root_once(self):
    queryperiodsecs = 0
    path = "/"
    disksystem = MockDisk(self.mock_tree, self.free_bytes, self.filectime)
    spacetrackers = ctafstgcd.SpaceTrackers(self.log, disksystem, queryperiodsecs)

    self.assertEqual(0, spacetrackers.get_nb_trackers())
    spacetracker = spacetrackers.get_tracker(path)
    self.assertEqual(1, spacetrackers.get_nb_trackers())
    self.assertEqual(path, spacetracker.path)

  def test_get_tracker_root_twice(self):
    disksystem = MockDisk(self.mock_tree, self.free_bytes, self.filectime)
    queryperiodsecs = 0
    path = "/"
    spacetrackers = ctafstgcd.SpaceTrackers(self.log, disksystem, queryperiodsecs)

    self.assertEqual(0, spacetrackers.get_nb_trackers())
    firsttracker = spacetrackers.get_tracker(path)
    self.assertEqual(1, spacetrackers.get_nb_trackers())
    self.assertEqual(path, firsttracker.path)

    secondtracker = spacetrackers.get_tracker(path)
    self.assertEqual(1, spacetrackers.get_nb_trackers())
    self.assertEqual(path, secondtracker.path)

    self.assertEqual(firsttracker, secondtracker)

class GcTestCase(unittest.TestCase):
  def setUp(self):
    self.log = DummyLogger()
    self.mock_tree = MockTreeNode("/")
    self.free_bytes = 10000
    self.file_size_and_ctime = ctafstgcd.FileSizeAndCtime()
    self.file_size_and_ctime.sizebytes = 1000
    self.file_size_and_ctime.ctime = time.time()
    self.fqdn = "a.b.c"

    self.config_str = \
"""[main]
log_file = log_file
mgm_host = mgm_host
eos_spaces = spinner
eos_space_to_min_free_bytes = spinner:0
gc_age_secs = 1000
absolute_max_age_secs = 604800
query_period_secs = 0
main_loop_period_secs = 0
xrdsecssskt = xrdsecssskt"""

    self.config = ctafstgcd.Gc.parse_conf(io.BytesIO(self.config_str))

  def tearDown(self):
    pass

  def test_parse_conf_default_test_conf(self):
    config = ctafstgcd.Gc.parse_conf(io.BytesIO(self.config_str))

    self.assertEqual('log_file', config.log_file)
    self.assertEqual('mgm_host', config.mgm_host)
    self.assertEqual(1, len(config.eos_spaces))
    self.assertEqual('spinner', config.eos_spaces[0])
    self.assertEqual(1, len(config.eos_space_to_min_free_bytes))
    self.assertTrue('spinner' in config.eos_space_to_min_free_bytes)
    self.assertEqual(0, config.eos_space_to_min_free_bytes['spinner'])
    self.assertEqual(604800, config.absolute_max_age_secs)
    self.assertEqual(0, config.query_period_secs)
    self.assertEqual(0, config.main_loop_period_secs)
    self.assertEqual('xrdsecssskt', config.xrdsecssskt)

  def test_parse_conf_multiple_eos_spaces(self):
    config_str = \
"""[main]
log_file = log_file
mgm_host = mgm_host
eos_spaces = space_one space_two space_three
eos_space_to_min_free_bytes = space_one:1 space_two:2 space_three:3
gc_age_secs = 1000
absolute_max_age_secs = 604800
query_period_secs = 0
main_loop_period_secs = 0
xrdsecssskt = xrdsecssskt"""
    config = ctafstgcd.Gc.parse_conf(io.BytesIO(config_str))

    self.assertEqual('log_file', config.log_file)
    self.assertEqual('mgm_host', config.mgm_host)
    self.assertEqual(3, len(config.eos_spaces))
    self.assertTrue('space_one' in config.eos_spaces)
    self.assertTrue('space_two' in config.eos_spaces)
    self.assertTrue('space_three' in config.eos_spaces)
    self.assertEqual(3, len(config.eos_space_to_min_free_bytes))
    self.assertTrue('space_one' in config.eos_space_to_min_free_bytes)
    self.assertTrue('space_two' in config.eos_space_to_min_free_bytes)
    self.assertTrue('space_three' in config.eos_space_to_min_free_bytes)
    self.assertEqual(1, config.eos_space_to_min_free_bytes['space_one'])
    self.assertEqual(2, config.eos_space_to_min_free_bytes['space_two'])
    self.assertEqual(3, config.eos_space_to_min_free_bytes['space_three'])
    self.assertEqual(604800, config.absolute_max_age_secs)
    self.assertEqual(0, config.query_period_secs)
    self.assertEqual(0, config.main_loop_period_secs)
    self.assertEqual('xrdsecssskt', config.xrdsecssskt)

  def test_parse_conf_eos_space_to_min_free_bytes_missing_colon(self):
    config_str = \
"""[main]
log_file = log_file
mgm_host = mgm_host
eos_spaces = spinner
eos_space_to_min_free_bytes = spinner0
gc_age_secs = 1000
absolute_max_age_secs = 604800
query_period_secs = 0
main_loop_period_secs = 0
xrdsecssskt = xrdsecssskt"""
    with self.assertRaises(ctafstgcd.MissingColonError):
      ctafstgcd.Gc.parse_conf(io.BytesIO(config_str))

  def test_parse_conf_eos_space_to_min_free_bytes_too_many_colons(self):
    config_str = \
"""[main]
log_file = log_file
mgm_host = mgm_host
eos_spaces = spinner
eos_space_to_min_free_bytes = spinner:0:1
gc_age_secs = 1000
absolute_max_age_secs = 604800
query_period_secs = 0
main_loop_period_secs = 0
xrdsecssskt = xrdsecssskt"""
    with self.assertRaises(ctafstgcd.TooManyColonsError):
      ctafstgcd.Gc.parse_conf(io.BytesIO(config_str))

  def test_parse_conf_min_free_bytes_not_a_valid_integer(self):
    config_str = \
"""[main]
log_file = log_file
mgm_host = mgm_host
eos_spaces = spinner
eos_space_to_min_free_bytes = spinner:not_a_valid_integer
gc_age_secs = 1000
absolute_max_age_secs = 604800
query_period_secs = 0
main_loop_period_secs = 0
xrdsecssskt = xrdsecssskt"""
    with self.assertRaises(ctafstgcd.MinFreeBytesError):
      ctafstgcd.Gc.parse_conf(io.BytesIO(config_str))

  def test_parse_conf_negative_min_free_bytes(self):
    config_str = \
"""[main]
log_file = log_file
mgm_host = mgm_host
eos_spaces = spinner
eos_space_to_min_free_bytes = spinner:-1
gc_age_secs = 1000
absolute_max_age_secs = 604800
query_period_secs = 0
main_loop_period_secs = 0
xrdsecssskt = xrdsecssskt"""
    with self.assertRaises(ctafstgcd.MinFreeBytesError):
      ctafstgcd.Gc.parse_conf(io.BytesIO(config_str))

  def test_parse_conf_no_mapping_from_eos_space_to_min_free_bytes(self):
    config_str = \
"""[main]
log_file = log_file
mgm_host = mgm_host
eos_spaces = space_one space_two
eos_space_to_min_free_bytes = space_one:0
gc_age_secs = 1000
absolute_max_age_secs = 604800
query_period_secs = 0
main_loop_period_secs = 0
xrdsecssskt = xrdsecssskt"""
    with self.assertRaises(ctafstgcd.MinFreeBytesNotSetError):
      ctafstgcd.Gc.parse_conf(io.BytesIO(config_str))

  def test_constructor(self):
    disk = MockDisk(self.mock_tree, self.free_bytes, self.file_size_and_ctime)
    eos = MockEos()

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

  def test_run_only_once_no_fs(self):
    disk = MockDisk(self.mock_tree, self.free_bytes, self.file_size_and_ctime)
    eos = MockEos()

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

  def test_run_only_once_one_fs(self):
    mock_fs = MockTreeNode('filesystem1')
    mock_tree = MockTreeNode('/', [mock_fs])
    disk = MockDisk(mock_tree, self.free_bytes, self.file_size_and_ctime)

    filesystem1 = {
      'path' : '/filesystem1',
      'host' : self.fqdn,
      'schedgroup' : 'spinner.0'
    }
    file_systems = [filesystem1]
    eos = MockEos(file_systems)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(1, disk.nb_listdir) # ls of fs
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

  def test_run_only_once_one_fs_one_sub_dir_no_free_space(self):
    mock_sub_dir = MockTreeNode('12345678')
    mock_fs = MockTreeNode('filesystem1', [mock_sub_dir])
    mock_tree = MockTreeNode('/', [mock_fs])
    disk = MockDisk(mock_tree, self.free_bytes, self.file_size_and_ctime)

    filesystem1 = {
      'path' : '/filesystem1',
      'host' : self.fqdn,
      'schedgroup' : 'spinner.0'
    }
    file_systems = [filesystem1]
    eos = MockEos(file_systems)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    self.config.eos_space_to_min_free_bytes['spinner'] = self.free_bytes + 1

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nb_listdir) # ls of fs and sub_dir
    self.assertEqual(1, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

  def test_run_only_once_one_fs_one_sub_dir_free_space(self):
    mock_sub_dir = MockTreeNode('12345678')
    mock_fs = MockTreeNode('filesystem1', [mock_sub_dir])
    mock_tree = MockTreeNode('/', [mock_fs])
    disk = MockDisk(mock_tree, self.free_bytes, self.file_size_and_ctime)

    filesystem1 = {
      'path' : '/filesystem1',
      'host' : self.fqdn,
      'schedgroup' : 'spinner.0'
    }
    file_systems = [filesystem1]
    eos = MockEos(file_systems)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nb_listdir)
    self.assertEqual(1, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes) # sub_dir
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

  def test_run_only_once_one_fs_one_sub_dir_one_file_free_space(self):
    mock_file = MockTreeNode('90abcdef')
    mock_sub_dir = MockTreeNode('12345678', [mock_file])
    mock_fs = MockTreeNode('filesystem1', [mock_sub_dir])
    mock_tree = MockTreeNode('/', [mock_fs])
    disk = MockDisk(mock_tree, self.free_bytes, self.file_size_and_ctime)

    filesystem1 = {
      'path' : '/filesystem1',
      'host' : self.fqdn,
      'schedgroup' : 'spinner.0'
    }
    file_systems = [filesystem1]
    eos = MockEos(file_systems)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nb_listdir)
    self.assertEqual(1, disk.nb_isdir)
    self.assertEqual(1, disk.nb_isfile)
    self.assertEqual(1, disk.nb_get_free_bytes)
    self.assertEqual(1, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

  def test_run_only_once_one_fs_one_sub_dir_one_file_no_free_space_young_file(self):
    mock_file = MockTreeNode('90abcdef')
    mock_sub_dir = MockTreeNode('12345678', [mock_file])
    mock_fs = MockTreeNode('filesystem1', [mock_sub_dir])
    mock_tree = MockTreeNode('/', [mock_fs])
    disk = MockDisk(mock_tree, self.free_bytes, self.file_size_and_ctime)

    filesystem1 = {
      'path' : '/filesystem1',
      'host' : self.fqdn,
      'schedgroup' : 'spinner.0'
    }
    file_systems = [filesystem1]
    eos = MockEos(file_systems)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    self.config.eos_space_to_min_free_bytes['spinner'] = self.free_bytes + 1

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nb_listdir)
    self.assertEqual(1, disk.nb_isdir)
    self.assertEqual(1, disk.nb_isfile)
    self.assertEqual(1, disk.nb_get_free_bytes) # file
    self.assertEqual(1, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

  def test_run_only_once_one_fs_one_sub_dir_one_file_no_free_space_old_file(self):
    mock_file = MockTreeNode('90abcdef')
    mock_sub_dir = MockTreeNode('12345678', [mock_file])
    mock_fs = MockTreeNode('filesystem1', [mock_sub_dir])
    mock_tree = MockTreeNode('/', [mock_fs])
    file_size_and_ctime = ctafstgcd.FileSizeAndCtime()
    file_size_and_ctime.sizebytes = 1000
    file_size_and_ctime.ctime = time.time() - self.config.gc_age_secs - 1
    disk = MockDisk(mock_tree, self.free_bytes, file_size_and_ctime)

    filesystem1 = {
      'path' : '/filesystem1',
      'host' : self.fqdn,
      'schedgroup' : 'spinner.0'
    }
    file_systems = [filesystem1]
    eos = MockEos(file_systems)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    self.config.eos_space_to_min_free_bytes['spinner'] = self.free_bytes + 1

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nb_listdir)
    self.assertEqual(1, disk.nb_isdir)
    self.assertEqual(1, disk.nb_isfile)
    self.assertEqual(1, disk.nb_get_free_bytes) # file
    self.assertEqual(1, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(1, eos.nb_stagerrm)
    self.assertEqual(1, eos.nb_attrset)

  def test_run_only_once_one_fs_one_sub_dir_one_file_free_space_absolutely_old_file(self):
    mock_file = MockTreeNode('90abcdef')
    mock_sub_dir = MockTreeNode('12345678', [mock_file])
    mock_fs = MockTreeNode('filesystem1', [mock_sub_dir])
    mock_tree = MockTreeNode('/', [mock_fs])
    file_size_and_ctime = ctafstgcd.FileSizeAndCtime()
    file_size_and_ctime.sizebytes = 1000
    file_size_and_ctime.ctime = time.time() - self.config.absolute_max_age_secs - 1
    disk = MockDisk(mock_tree, self.free_bytes, file_size_and_ctime)

    filesystem1 = {
      'path' : '/filesystem1',
      'host' : self.fqdn,
      'schedgroup' : 'spinner.0'
    }
    file_systems = [filesystem1]
    eos = MockEos(file_systems)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    self.config.eos_space_to_min_free_bytes['spinner'] = self.free_bytes + 1

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nb_listdir)
    self.assertEqual(1, disk.nb_isdir)
    self.assertEqual(1, disk.nb_isfile)
    self.assertEqual(1, disk.nb_get_free_bytes) # file
    self.assertEqual(1, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(1, eos.nb_stagerrm)
    self.assertEqual(1, eos.nb_attrset)

  def test_run_only_once_one_fs_one_sub_dir_one_file_free_space_old_file(self):
    mock_file = MockTreeNode('90abcdef')
    mock_sub_dir = MockTreeNode('12345678', [mock_file])
    mock_fs = MockTreeNode('filesystem1', [mock_sub_dir])
    mock_tree = MockTreeNode('/', [mock_fs])
    file_size_and_ctime = ctafstgcd.FileSizeAndCtime()
    file_size_and_ctime.sizebytes = 1000
    file_size_and_ctime.ctime = time.time() - self.config.gc_age_secs - 1
    disk = MockDisk(mock_tree, self.free_bytes, file_size_and_ctime)

    filesystem1 = {
      'path' : '/filesystem1',
      'host' : self.fqdn,
      'schedgroup' : 'spinner.0'
    }
    file_systems = [filesystem1]
    eos = MockEos(file_systems)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    self.config.eos_space_to_min_free_bytes['spinner'] = self.free_bytes

    gc = ctafstgcd.Gc(self.log, self.fqdn, disk, eos, self.config)

    self.assertEqual(0, disk.nb_listdir)
    self.assertEqual(0, disk.nb_isdir)
    self.assertEqual(0, disk.nb_isfile)
    self.assertEqual(0, disk.nb_get_free_bytes)
    self.assertEqual(0, disk.nb_get_file_size_and_ctime)
    self.assertEqual(0, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

    runonlyonce = True
    gc.run(runonlyonce)

    self.assertEqual(2, disk.nb_listdir)
    self.assertEqual(1, disk.nb_isdir)
    self.assertEqual(1, disk.nb_isfile)
    self.assertEqual(1, disk.nb_get_free_bytes)
    self.assertEqual(1, disk.nb_get_file_size_and_ctime)
    self.assertEqual(1, eos.nb_fsls)
    self.assertEqual(0, eos.nb_stagerrm)
    self.assertEqual(0, eos.nb_attrset)

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
