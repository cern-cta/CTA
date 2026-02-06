/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ConfigurationFile.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>

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

}  // namespace unitTests
