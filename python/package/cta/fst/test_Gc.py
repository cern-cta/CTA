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
import unittest

from cta.fst.gc import Gc

class GcTestCase(unittest.TestCase):
  def setUp(self):
    self.programname = "test"
    self.minfreebytes = 1
    self.gcagesecs = 1
    self.cmdline_mgmhost = None
    self.logfilepath = None

  def tearDown(self):
    pass

  def test_get_env_mgmhost_hostname(self):
    hostname = "hostname";
    env = {"EOS_MGM_URL" : hostname}
    gc = Gc(self.programname, env, self.minfreebytes, self.gcagesecs, self.cmdline_mgmhost, self.logfilepath)
    self.assertEqual(hostname, gc.get_env_mgmhost())

  def test_get_env_mgmhost_root_hostname(self):
    hostname = "hostname";
    env = {"EOS_MGM_URL" : "root://" + hostname}
    gc = Gc(self.programname, env, self.minfreebytes, self.gcagesecs, self.cmdline_mgmhost, self.logfilepath)
    self.assertEqual(hostname, gc.get_env_mgmhost())

  def test_get_env_mgmhost_xroot_hostname(self):
    hostname = "hostname";
    env = {"EOS_MGM_URL" : "xroot://" + hostname}
    gc = Gc(self.programname, env, self.minfreebytes, self.gcagesecs, self.cmdline_mgmhost, self.logfilepath)
    self.assertEqual(hostname, gc.get_env_mgmhost())

  def test_get_env_mgmhost_none(self):
    env = {}
    gc = Gc(self.programname, env, self.minfreebytes, self.gcagesecs, self.cmdline_mgmhost, self.logfilepath)
    self.assertEqual(None, gc.get_env_mgmhost())

  def test_get_sysconfig_file_mgmhost(self):
    hostname = "hostname";
    sysconfig_file = cStringIO.StringIO()
    sysconfig_file.write("EOS_MGM_HOST=" + hostname + "\n")
    sysconfig_file.seek(0)
    env = {}
    gc = Gc(self.programname, env, self.minfreebytes, self.gcagesecs, self.cmdline_mgmhost, self.logfilepath)
    self.assertEqual(hostname, gc.get_sysconfig_file_mgmhost(sysconfig_file))

  def test_get_sysconfig_file_mgmhost_empty(self):
    sysconfig_file = cStringIO.StringIO()
    env = {}
    gc = Gc(self.programname, env, self.minfreebytes, self.gcagesecs, self.cmdline_mgmhost, self.logfilepath)
    self.assertEqual(None, gc.get_sysconfig_file_mgmhost(sysconfig_file))
