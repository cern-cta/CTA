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

#include "common/threading/SubProcess.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>

namespace systemTests {
TEST(cta_taped, InvocationTests) {
  {
    // Do we get help with -h or --help?
    cta::threading::SubProcess spHelpShort("/usr/bin/cta-taped", std::list<std::string>({"/usr/bin/cta-taped", "-h"}));
    spHelpShort.wait();
    ASSERT_NE(std::string::npos, spHelpShort.stdout().find("Usage"));
    ASSERT_TRUE(spHelpShort.stderr().empty());
    ASSERT_EQ(EXIT_SUCCESS, spHelpShort.exitValue());
    cta::threading::SubProcess spHelpLong("/usr/bin/cta-taped",
      std::list<std::string>({"/usr/bin/cta-taped", "--help"}));
    spHelpLong.wait();
    ASSERT_NE(std::string::npos, spHelpLong.stdout().find("Usage: cta-taped [options]"));
    ASSERT_TRUE(spHelpLong.stderr().empty());
    ASSERT_EQ(EXIT_SUCCESS, spHelpLong.exitValue());
  }

  {
    // Do we get proper complaint when the configuration file is not there?
    cta::threading::SubProcess spNoConfigFile("/usr/bin/cta-taped",
      std::list<std::string>({"/usr/bin/cta-taped", "-f", "-s", "-c", "/no/such/file"}));
    spNoConfigFile.wait();
    ASSERT_NE(std::string::npos, spNoConfigFile.stdout().find("Failed to open configuration file"));
    ASSERT_TRUE(spNoConfigFile.stderr().empty());
    ASSERT_EQ(EXIT_FAILURE, spNoConfigFile.exitValue());
  }

  // Does the tape server complain about absence of a mandatory configuration (DriveName)
  {
    // We provide le daemon with an existing configuration
    // file. The DriveName field is missing purposely.
    unitTests::TempFile ctaConf, driveConfig;
    ctaConf.stringAppend(
      "#A good enough configuration file for taped\n"
      "ObjectStore BackendPath vfsObjectStore:///tmp/dir\n"
      "taped BufferCount 1\n"

      //"taped DriveName dummy-Drive\n"
      "taped DriveLogicalLibrary dummyLL\n"
      "taped DriveDevice /dummy/Device\n"
      "taped DriveControlPath dummyControlPath\n"

      "general InstanceName production\n"
      "general SchedulerBackendName dummyProdUser\n"
      "general ServiceName dummy-service-name\n");

    cta::threading::SubProcess spNoDrive("/usr/bin/cta-taped", 
      std::list<std::string>({"/usr/bin/cta-taped", "-f", "-s", "-c", ctaConf.path()}));
    spNoDrive.wait();
    ASSERT_NE(std::string::npos, spNoDrive.stdout().find("In SourcedParameter::setFromConfigurationFile: mandatory parameter not found: category=taped key=DriveName"));
    ASSERT_TRUE(spNoDrive.stderr().empty());
    ASSERT_EQ(EXIT_FAILURE, spNoDrive.exitValue());
  }
}
}
