/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <list>
#include <string>

#include "catalogue/Catalogue.hpp"
#include "catalogue/tests/CatalogueTestUtils.hpp"
#include "catalogue/tests/modules/DriveConfigCatalogueTest.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/LogContext.hpp"
#include "common/SourcedParameter.hpp"

namespace unitTests {

cta_catalogue_DriveConfigTest::cta_catalogue_DriveConfigTest()
  : m_dummyLog("dummy", "dummy") {
}

void cta_catalogue_DriveConfigTest::SetUp() {
  cta::log::LogContext dummyLc(m_dummyLog);
  m_catalogue = CatalogueTestUtils::createCatalogue(GetParam(), &dummyLc);
}

void cta_catalogue_DriveConfigTest::TearDown() {
  m_catalogue.reset();
}

TEST_P(cta_catalogue_DriveConfigTest, getTapeDriveConfig) {
  const std::string tapeDriveName = "VDSTK11";

  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};

  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  auto driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  std::string category, value, source;
  std::tie(category, value, source) = driveConfig.value();
  ASSERT_EQ(daemonUserName.category(), category);
  ASSERT_EQ(daemonUserName.value(), value);
  ASSERT_EQ(daemonUserName.source(), source);
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName, daemonUserName.key());
}

TEST_P(cta_catalogue_DriveConfigTest, getAllDrivesConfigs) {
  std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig> tapeDriveConfigs;
  // Create 100 tape drives
  for (size_t i = 0; i < 100; i++) {
    std::stringstream ss;
    ss << "VDSTK" << std::setw(5) << std::setfill('0') << i;

    cta::SourcedParameter<std::string> daemonUserName {
      "taped", "DaemonUserName", "cta", "Compile time default"};
    m_catalogue->DriveConfig()->createTapeDriveConfig(ss.str(), daemonUserName.category(), daemonUserName.key(),
      daemonUserName.value(), daemonUserName.source());
    tapeDriveConfigs.push_back({ss.str(), daemonUserName.category(), daemonUserName.key(), daemonUserName.value(),
      daemonUserName.source()});
    cta::SourcedParameter<std::string> defaultConfig {
      "taped", "defaultConfig", "cta", "Random Default Config for Testing"};
    m_catalogue->DriveConfig()->createTapeDriveConfig(ss.str(), defaultConfig.category(), defaultConfig.key(),
      defaultConfig.value(), defaultConfig.source());
    tapeDriveConfigs.push_back({ss.str(), defaultConfig.category(), defaultConfig.key(), defaultConfig.value(),
      defaultConfig.source()});
  }
  const auto drivesConfigs = m_catalogue->DriveConfig()->getTapeDriveConfigs();
  ASSERT_EQ(tapeDriveConfigs.size(), drivesConfigs.size());
  for (const auto& dc : drivesConfigs) {
    m_catalogue->DriveConfig()->deleteTapeDriveConfig(dc.tapeDriveName, dc.keyName);
  }
}

TEST_P(cta_catalogue_DriveConfigTest, setSourcedParameterWithEmptyValue) {
  const std::string tapeDriveName = "VDSTK11";

  cta::SourcedParameter<std::string> raoLtoOptions {
    "taped", "RAOLTOAlgorithmOptions", "", "Compile time default"
  };

  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName, raoLtoOptions.category(), raoLtoOptions.key(),
    raoLtoOptions.value(), raoLtoOptions.source());
  auto driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, raoLtoOptions.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  std::string category, value, source;
  std::tie(category, value, source) = driveConfig.value();
  ASSERT_EQ(raoLtoOptions.category(), category);
  ASSERT_EQ("", value);
  ASSERT_EQ(raoLtoOptions.source(), source);
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName, raoLtoOptions.key());

  cta::SourcedParameter<std::string> backendPath{
    "ObjectStore", "BackendPath"};

  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName, backendPath.category(), backendPath.key(),
    backendPath.value(), backendPath.source());
  driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, backendPath.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  std::tie(category, value, source) = driveConfig.value();
  ASSERT_EQ(backendPath.category(), category);
  ASSERT_EQ("", value);
  ASSERT_EQ("", source);
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName, backendPath.key());
}

TEST_P(cta_catalogue_DriveConfigTest, failTogetTapeDriveConfig) {
  const std::string tapeDriveName = "VDSTK11";
  const std::string wrongName = "VDSTK56";
  const std::string wrongKey = "wrongKey";
  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};

  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  auto driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(wrongName, daemonUserName.key());
  ASSERT_FALSE(driveConfig);
  driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, wrongKey);
  ASSERT_FALSE(driveConfig);
  driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(wrongName, wrongKey);
  ASSERT_FALSE(driveConfig);
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName, daemonUserName.key());
}

TEST_P(cta_catalogue_DriveConfigTest, failTodeleteTapeDriveConfig) {
  const std::string tapeDriveName = "VDSTK11";
  const std::string wrongName = "VDSTK56";
  const std::string wrongKey = "wrongKey";
  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(wrongName, daemonUserName.key());
  auto driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName, wrongKey);
  driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(wrongName, wrongKey);
  driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig));
  // Good deletion
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName, daemonUserName.key());
  driveConfig = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, daemonUserName.key());
  ASSERT_FALSE(driveConfig);
}

TEST_P(cta_catalogue_DriveConfigTest, multipleDriveConfig) {
  const std::string tapeDriveName1 = "VDSTK11";
  const std::string tapeDriveName2 = "VDSTK12";

  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};
  cta::SourcedParameter<std::string> daemonGroupName {
    "taped", "DaemonGroupName", "tape", "Compile time default"};

  // Combinations of tapeDriveName1/2 and daemonUserName and daemonGroupName
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName1, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName1, daemonGroupName.category(), daemonGroupName.key(),
    daemonGroupName.value(), daemonGroupName.source());
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName2, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName2, daemonGroupName.category(), daemonGroupName.key(),
    daemonGroupName.value(), daemonGroupName.source());
  auto driveConfig1UserName = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName1, daemonUserName.key());
  auto driveConfig2UserName = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName2, daemonUserName.key());
  auto driveConfig1GroupName = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName1, daemonGroupName.key());
  auto driveConfig2GroupName = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName2, daemonGroupName.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig1UserName));
  ASSERT_TRUE(static_cast<bool>(driveConfig2UserName));
  ASSERT_TRUE(static_cast<bool>(driveConfig1GroupName));
  ASSERT_TRUE(static_cast<bool>(driveConfig2GroupName));
  std::string category, value, source;
  std::tie(category, value, source) = driveConfig1UserName.value();
  ASSERT_EQ(daemonUserName.category(), category);
  ASSERT_EQ(daemonUserName.value(), value);
  ASSERT_EQ(daemonUserName.source(), source);
  std::tie(category, value, source) = driveConfig2UserName.value();
  ASSERT_EQ(daemonUserName.category(), category);
  ASSERT_EQ(daemonUserName.value(), value);
  ASSERT_EQ(daemonUserName.source(), source);
  std::tie(category, value, source) = driveConfig1GroupName.value();
  ASSERT_EQ(daemonGroupName.category(), category);
  ASSERT_EQ(daemonGroupName.value(), value);
  ASSERT_EQ(daemonGroupName.source(), source);
  std::tie(category, value, source) = driveConfig2GroupName.value();
  ASSERT_EQ(daemonGroupName.category(), category);
  ASSERT_EQ(daemonGroupName.value(), value);
  ASSERT_EQ(daemonGroupName.source(), source);
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName1, daemonUserName.key());
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName1, daemonGroupName.key());
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName2, daemonUserName.key());
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName2, daemonGroupName.key());
}

TEST_P(cta_catalogue_DriveConfigTest, getNamesAndKeysOfMultipleDriveConfig) {
  const std::string tapeDriveName1 = "VDSTK11";
  const std::string tapeDriveName2 = "VDSTK12";

  cta::SourcedParameter<std::string> daemonUserName {
    "taped", "DaemonUserName", "cta", "Compile time default"};
  cta::SourcedParameter<std::string> daemonGroupName {
    "taped", "DaemonGroupName", "tape", "Compile time default"};

  // Combinations of tapeDriveName1/2 and daemonUserName and daemonGroupName
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName1, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName1, daemonGroupName.category(), daemonGroupName.key(),
    daemonGroupName.value(), daemonGroupName.source());
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName2, daemonUserName.category(), daemonUserName.key(),
    daemonUserName.value(), daemonUserName.source());
  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName2, daemonGroupName.category(), daemonGroupName.key(),
    daemonGroupName.value(), daemonGroupName.source());

  const auto configurationTapeNamesAndKeys = m_catalogue->DriveConfig()->getTapeDriveConfigNamesAndKeys();

  for (const auto& nameAndKey : configurationTapeNamesAndKeys) {
    m_catalogue->DriveConfig()->deleteTapeDriveConfig(nameAndKey.first, nameAndKey.second);
  }
}

TEST_P(cta_catalogue_DriveConfigTest, modifyTapeDriveConfig) {
  const std::string tapeDriveName = "VDSTK11";
  // Both share same key
  cta::SourcedParameter<std::string> daemonUserName1 {
    "taped1", "DaemonUserName", "cta1", "Compile time1 default"};
  cta::SourcedParameter<std::string> daemonUserName2 {
    "taped2", "DaemonUserName", "cta2", "Compile time2 default"};

  m_catalogue->DriveConfig()->createTapeDriveConfig(tapeDriveName, daemonUserName1.category(), daemonUserName1.key(),
    daemonUserName1.value(), daemonUserName1.source());
  const auto driveConfig1 = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, daemonUserName1.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig1));
  std::string category, value, source;
  std::tie(category, value, source) = driveConfig1.value();
  ASSERT_NE(daemonUserName2.category(), category);
  ASSERT_NE(daemonUserName2.value(), value);
  ASSERT_NE(daemonUserName2.source(), source);
  m_catalogue->DriveConfig()->modifyTapeDriveConfig(tapeDriveName, daemonUserName2.category(), daemonUserName2.key(),
    daemonUserName2.value(), daemonUserName2.source());
  const auto driveConfig2 = m_catalogue->DriveConfig()->getTapeDriveConfig(tapeDriveName, daemonUserName1.key());
  ASSERT_TRUE(static_cast<bool>(driveConfig2));
  std::tie(category, value, source) = driveConfig2.value();
  ASSERT_EQ(daemonUserName2.category(), category);
  ASSERT_EQ(daemonUserName2.value(), value);
  ASSERT_EQ(daemonUserName2.source(), source);
  m_catalogue->DriveConfig()->deleteTapeDriveConfig(tapeDriveName, daemonUserName1.key());
}

}  // namespace unitTests
