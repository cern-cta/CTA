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
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>

namespace systemTests {  
TEST(cta_taped, InvocationTests) {
  {  
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
  }
  
  {
    // Do we get proper complaint when the configuration file is not there?
    Subprocess spNoConfigFile("cta-taped", std::list<std::string>({"cta-taped", "-f", "-s", "-c", "/no/such/file"}));
    spNoConfigFile.wait();
    ASSERT_NE(std::string::npos, spNoConfigFile.stdout().find("Failed to open configuration file"));
    ASSERT_TRUE(spNoConfigFile.stderr().empty());
    ASSERT_EQ(EXIT_FAILURE, spNoConfigFile.exitValue());
  }
  
  // Does the tape server complain about absence of drive configuration?
  {
    // We provide le daemon with an existing (but almost empty) configuration
    // file. The mandatory fields are present.
    unitTests::TempFile tf;
    tf.stringAppend(
      "#A good enough configuration file for taped\n"
      "general ObjectStoreURL vfsObjectStore:///tmp/dir\n"
      "general FileCatalogURL sqliteFileCatalog:///tmp/dir2\n");
    Subprocess spNoDrive("cta-taped", std::list<std::string>({"cta-taped", "-f", "-s", "-c", tf.path()}));
    spNoDrive.wait();
    ASSERT_NE(std::string::npos, spNoDrive.stdout().find("MSG=\"Aborting\" Message=\"No drive found in configuration\""));
    ASSERT_TRUE(spNoDrive.stderr().empty());
    ASSERT_EQ(EXIT_FAILURE, spNoDrive.exitValue());
  }
}
}