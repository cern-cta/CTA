// ----------------------------------------------------------------------
// File: Utils/RegexTest.cc
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <gmock/gmock-cardinalities.h>
#include "Regex.hh"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::_;

TEST(Regex, BasicFunctionality) {
  Tape::Utils::regex re("a(b)");
  std::vector<std::string> ret1, ret2;
  ret1 = re.exec("1abc");
  ret2 = re.exec("xyz");

  ASSERT_EQ(ret1.size(), 2);
  ASSERT_EQ(ret1[0], "ab");
  ASSERT_EQ(ret1[1], "b");
  ASSERT_EQ(ret2.size(), 0);
}

TEST(Regex, OperationalTest) {
  Tape::Utils::regex re("^scsi_tape:(st[[:digit:]]+)$");
  std::vector<std::string> ret1, ret2, ret3;
  ret1 = re.exec("scsi_tape:st1");
  ret2 = re.exec("scsi_tape:st124");
  ret3 = re.exec("scsi_tape:st1a");

  ASSERT_EQ(ret1.size(), 2);
  ASSERT_EQ(ret1[0], "scsi_tape:st1");
  ASSERT_EQ(ret1[1], "st1");
  ASSERT_EQ(ret2.size(), 2);
  ASSERT_EQ(ret2[0], "scsi_tape:st124");
  ASSERT_EQ(ret2[1], "st124");
  ASSERT_EQ(ret3.size(), 0);
}
