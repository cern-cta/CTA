/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <gtest/gtest.h>

#include "common/ConfigurationFile.hpp"
#include "tests/TempFile.hpp"

namespace unitTests {

TEST(cta_Daemon, ConfigurationFile) {
  TempFile tf;
  tf.stringFill("# My test config file\n"
  "cat1 key1 val1\n"
  "cat1 #key2 val2\n"
  "cat1 key3 #val3\n");
  cta::ConfigurationFile cf(tf.path());
  ASSERT_EQ(1, cf.entries.size());
  ASSERT_NO_THROW(cf.entries.at("cat1").at("key1"));
  ASSERT_EQ("val1", cf.entries.at("cat1").at("key1").value);
  ASSERT_EQ(2, cf.entries.at("cat1").at("key1").line);
}

} // namespace unitTests
