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

#include "tests/Subprocess.hpp"

#include <gtest/gtest.h>

namespace systemTests {  
TEST(cta_taped, InvocationTests) {
  // Do we get help with -h or --help?
  Subprocess spHelpShort("cta-taped", std::list<std::string>({"cta-taped", "-h"}));
  spHelpShort.wait();
  ASSERT_NE(std::string::npos, spHelpShort.stdout().find("Usage"));
  ASSERT_TRUE(spHelpShort.stderr().empty());
  ASSERT_EQ(EXIT_SUCCESS, spHelpShort.exitValue());
  Subprocess spHelpLong("cta-taped", std::list<std::string>({"cta-taped", "--help"}));
  spHelpLong.wait();
  ASSERT_NE(std::string::npos, spHelpLong.stdout().find("Usage: cta-taped [options]"));
  ASSERT_TRUE(spHelpLong.stderr().empty());
  ASSERT_EQ(EXIT_SUCCESS, spHelpLong.exitValue());
  
  // Does the tape server complain about absence of drive configuration?
  Subprocess spNoDrive("cta-taped", std::list<std::string>({"cta-taped", "-f", "-s"}));
  spNoDrive.wait();
  ASSERT_NE(std::string::npos, spNoDrive.stdout().find("MSG=\"Aborting\" Message=\"No drive found in configuration\""));
  ASSERT_TRUE(spNoDrive.stderr().empty());
  ASSERT_EQ(EXIT_FAILURE, spNoDrive.exitValue());
}
}