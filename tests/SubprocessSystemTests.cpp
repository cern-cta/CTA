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

#include "Subprocess.hpp"

#include <gtest/gtest.h>

namespace systemTests {  
TEST(SuprocessHelper, basicTests) {
  Subprocess sp("echo", std::list<std::string>({"echo", "Hello,", "world."}));
  sp.wait();
  ASSERT_EQ("Hello, world.\n", sp.stdout());
  ASSERT_EQ("", sp.stderr());
  ASSERT_EQ(0, sp.exitValue());
  Subprocess sp2("cat", std::list<std::string>({"cat", "/no/such/file"}));
  sp2.wait();
  ASSERT_EQ("", sp2.stdout());
  ASSERT_NE(std::string::npos, sp2.stderr().find("/no/such/file"));
  ASSERT_EQ(1, sp2.exitValue());
  Subprocess sp3("/no/such/file", std::list<std::string>({"/no/such/file"}));
  sp3.wait();
  ASSERT_EQ("", sp3.stdout());
  ASSERT_NE(std::string::npos, sp3.stderr().find("In Subprocess::Subprocess execv failed"));
  ASSERT_EQ(1, sp3.exitValue());
}
}