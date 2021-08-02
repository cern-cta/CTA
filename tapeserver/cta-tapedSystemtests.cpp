/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/threading/SubProcess.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>

namespace systemTests {
TEST(cta_taped, InvocationTests) {
  {
    // Do we get help with -h or --help?
    cta::threading::SubProcess spHelpShort("cta-taped", std::list<std::string>({"cta-taped", "-h"}));
    spHelpShort.wait();
    ASSERT_NE(std::string::npos, spHelpShort.stdout().find("Usage"));
    ASSERT_TRUE(spHelpShort.stderr().empty());
    ASSERT_EQ(EXIT_SUCCESS, spHelpShort.exitValue());
    cta::threading::SubProcess spHelpLong("cta-taped", std::list<std::string>({"cta-taped", "--help"}));
    spHelpLong.wait();
    ASSERT_NE(std::string::npos, spHelpLong.stdout().find("Usage: cta-taped [options]"));
    ASSERT_TRUE(spHelpLong.stderr().empty());
    ASSERT_EQ(EXIT_SUCCESS, spHelpLong.exitValue());
  }

  {
    // Do we get proper complaint when the configuration file is not there?
    cta::threading::SubProcess spNoConfigFile("cta-taped", std::list<std::string>({"cta-taped", "-f", "-s", "-c", "/no/such/file"}));
    spNoConfigFile.wait();
    ASSERT_NE(std::string::npos, spNoConfigFile.stdout().find("Failed to open configuration file"));
    ASSERT_TRUE(spNoConfigFile.stderr().empty());
    ASSERT_EQ(EXIT_FAILURE, spNoConfigFile.exitValue());
  }

  // Does the tape server complain about absence of drive configuration?
  {
    // We provide le daemon with an existing (but almost empty) configuration
    // file. The mandatory fields are present.
    unitTests::TempFile ctaConf, tpConfig;
    ctaConf.stringAppend(
      "#A good enough configuration file for taped\n"
      "ObjectStore BackendPath vfsObjectStore:///tmp/dir\n"
      "general FileCatalogURL sqliteFileCatalog:///tmp/dir2\n"
      "taped BufferCount 1\n"
      "taped TpConfigPath ");
    ctaConf.stringAppend(tpConfig.path());
    cta::threading::SubProcess spNoDrive("cta-taped", std::list<std::string>({"cta-taped", "-f", "-s", "-c", ctaConf.path()}));
    spNoDrive.wait();
    ASSERT_NE(std::string::npos, spNoDrive.stdout().find("MSG=\"Aborting cta-taped on uncaught exception. Stack trace follows.\" Message=\"No drive found in configuration\""));
    ASSERT_TRUE(spNoDrive.stderr().empty());
    ASSERT_EQ(EXIT_FAILURE, spNoDrive.exitValue());
  }
}
}
