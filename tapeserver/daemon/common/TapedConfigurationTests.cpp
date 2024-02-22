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

#include "tests/TempFile.hpp"
#include "common/log/StdoutLogger.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"

namespace unitTests {

TEST(cta_Daemon, TapedConfiguration) {
  TempFile incompleteConfFile, completeConfFile;
  incompleteConfFile.stringFill(
  "# My incomplete taped configuration file\n"
  );
  completeConfFile.stringFill(
  "# A good enough configuration file for taped\n"
  "ObjectStore BackendPath vfsObjectStore:///tmp/dir\n"

  "taped DriveName dummy-Drive\n"
  "taped DriveLogicalLibrary dummyLL\n"
  "taped DriveDevice /dummy/Device\n"
  "taped DriveControlPath dummyControlPath\n"

  "general InstanceName production\n"
  "general SchedulerBackendName dummyProdUser\n"
  "general ServiceName dummy-service-name\n");

  ASSERT_THROW(cta::tape::daemon::common::TapedConfiguration::createFromCtaConf(incompleteConfFile.path()),
    cta::SourcedParameter<std::string>::MandatoryParameterNotDefined);
  auto completeConfig = 
    cta::tape::daemon::common::TapedConfiguration::createFromCtaConf(completeConfFile.path());
  ASSERT_EQ(completeConfFile.path()+":2", completeConfig.backendPath.source());
  ASSERT_EQ("vfsObjectStore:///tmp/dir", completeConfig.backendPath.value());
}

TEST(cta_Daemon, TapedConfigurationFull) {
  cta::log::StdoutLogger log("dummy","unitTests");
  TempFile completeConfFile;
  completeConfFile.stringFill(
  "#A good enough configuration file for taped\n"
  "ObjectStore BackendPath vfsObjectStore:///tmp/dir\n"
  "taped CatalogueConfigFile /etc/cta/catalog.conf\n"
  "taped ArchiveFetchBytesFiles 1,2\n"
  "taped ArchiveFlushBytesFiles              3 , 4 \n"
  "taped RetrieveFetchBytesFiles  5,   6\n"
  "taped BufferCount 1  \n"

  "taped DriveName dummy-Drive\n"
  "taped DriveLogicalLibrary dummyLL\n"
  "taped DriveDevice /dummy/Device\n"
  "taped DriveControlPath dummyControlPath\n"

  "general InstanceName production\n"
  "general SchedulerBackendName dummyProdUser\n"
  "general ServiceName dummy-service-name\n");

  // The log parameter can be uncommented to inspect the result on the output.
  auto completeConfig = 
    cta::tape::daemon::common::TapedConfiguration::createFromCtaConf(completeConfFile.path()/*, log*/);
  ASSERT_EQ(completeConfFile.path()+":2", completeConfig.backendPath.source());
  ASSERT_EQ("vfsObjectStore:///tmp/dir", completeConfig.backendPath.value());
  ASSERT_EQ(completeConfFile.path()+":3", completeConfig.fileCatalogConfigFile.source());
  ASSERT_EQ("/etc/cta/catalog.conf", completeConfig.fileCatalogConfigFile.value());
  ASSERT_EQ(1, completeConfig.archiveFetchBytesFiles.value().maxBytes);
  ASSERT_EQ(2, completeConfig.archiveFetchBytesFiles.value().maxFiles);
  ASSERT_EQ(3, completeConfig.archiveFlushBytesFiles.value().maxBytes);
  ASSERT_EQ(4, completeConfig.archiveFlushBytesFiles.value().maxFiles);
  ASSERT_EQ(5, completeConfig.retrieveFetchBytesFiles.value().maxBytes);
  ASSERT_EQ(6, completeConfig.retrieveFetchBytesFiles.value().maxFiles);
}

} // namespace unitTests
