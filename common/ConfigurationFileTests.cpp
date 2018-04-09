/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
