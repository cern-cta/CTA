/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/SubProcess.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>

namespace integrationTests {
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
    cta::threading::SubProcess spNoConfigFile(
      "/usr/bin/cta-taped",
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
    ctaConf.stringAppend("#A good enough configuration file for taped\n"
                         "ObjectStore BackendPath vfsObjectStore:///tmp/dir\n"
                         "taped BufferCount 1\n"

                         //"taped DriveName dummy-Drive\n"
                         "taped DriveLogicalLibrary dummyLL\n"
                         "taped DriveDevice /dummy/Device\n"
                         "taped DriveControlPath dummyControlPath\n"

                         "general InstanceName production\n"
                         "general SchedulerBackendName dummyProdUser\n"
                         "general ServiceName dummy-service-name\n");

    cta::threading::SubProcess spNoDrive(
      "/usr/bin/cta-taped",
      std::list<std::string>({"/usr/bin/cta-taped", "-f", "-s", "-c", ctaConf.path()}));
    spNoDrive.wait();
    ASSERT_NE(
      std::string::npos,
      spNoDrive.stdout().find(
        "In SourcedParameter::setFromConfigurationFile: mandatory parameter not found: category=taped key=DriveName"));
    ASSERT_TRUE(spNoDrive.stderr().empty());
    ASSERT_EQ(EXIT_FAILURE, spNoDrive.exitValue());
  }
}
}  // namespace integrationTests
