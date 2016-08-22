/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/log/DummyLogger.hpp"
#include "castor/tape/tapeserver/daemon/Catalogue.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxyDummy.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_CatalogueTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_CatalogueTest, goodDayPopulate) {
  using namespace castor::tape::tapeserver::daemon;

  TpconfigLines lines;
  lines.push_back(TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "manual@SLOT1"));
  lines.push_back(TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "manual@SLOT2"));

  DriveConfigMap driveConfigs;
  ASSERT_NO_THROW(driveConfigs.enterTpconfigLines(lines));

  castor::log::DummyLogger log("unittest");
  ProcessForkerProxyDummy processForker;
  const std::string hostName = "";
  const CatalogueConfig catalogueConfig;
  const int netTimeout = 1;
  castor::tape::System::mockWrapper sysWrapper;
  Catalogue catalogue(netTimeout, log, processForker,
    hostName, catalogueConfig, sysWrapper);
  ASSERT_NO_THROW(catalogue.populate(driveConfigs));
  
  {
    std::list<std::string> fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getUnitNames());
    ASSERT_EQ((std::list<std::string>::size_type)2, fromCatalogue.size());
    ASSERT_EQ(std::string("UNIT1"), fromCatalogue.front());
    fromCatalogue.pop_front();
    ASSERT_EQ(std::string("UNIT2"), fromCatalogue.front());
  }
  
  ///////////////////
  // UNIT1 assertions
  ///////////////////

  const CatalogueDrive &unit1 = catalogue.findDrive("UNIT1");
  const DriveConfig &unit1Config = unit1.getConfig();
  
  ASSERT_EQ(std::string("DGN1"), unit1Config.getLogicalLibrary());
  ASSERT_EQ(std::string("DEV1"), unit1Config.getDevFilename());
  
  ASSERT_EQ(DRIVE_STATE_UP, unit1.getState());
      // TODO: This has to be changed to DRIVE_STATE_DOWN in production which is the default for all tape drives.
      //       Daniele changed this to DRIVE_STATE_UP while doing the full system test ,because the tpconfig command is 
      //       not working properly.
  ASSERT_NO_THROW(unit1Config.getLibrarySlot());
  ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
    unit1Config.getLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("manual@SLOT1"), unit1Config.getLibrarySlot().str());
  
  ///////////////////
  // UNIT2 assertions
  ///////////////////
  
  const CatalogueDrive &unit2 = catalogue.findDrive("UNIT2");
  const DriveConfig &unit2Config = unit2.getConfig();
  
  ASSERT_EQ(std::string("DGN2"), unit2Config.getLogicalLibrary());
  ASSERT_EQ(std::string("DEV2"), unit2Config.getDevFilename());

  ASSERT_EQ(DRIVE_STATE_UP, unit2.getState());
      // TODO: This has to be changed to DRIVE_STATE_DOWN in production which is the default for all tape drives.
      //       Daniele changed this to DRIVE_STATE_UP while doing the full system test ,because the tpconfig command is 
      //       not working properly.
  ASSERT_NO_THROW(unit2Config.getLibrarySlot());
  ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
    unit2Config.getLibrarySlot().getLibraryType());
  ASSERT_EQ(std::string("manual@SLOT2"), unit2Config.getLibrarySlot().str());
}

TEST_F(castor_tape_tapeserver_daemon_CatalogueTest, duplicateUnitName) {
  using namespace castor::tape::tapeserver::daemon;

  TpconfigLines lines;
  lines.push_back(TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "manual@SLOT1"));
  lines.push_back(TpconfigLine(
    "UNIT1", "DGN2", "DEV2", "manual@SLOT2"));

  DriveConfigMap driveConfigs;
  ASSERT_THROW(driveConfigs.enterTpconfigLines(lines),
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_CatalogueTest, 
  getStateOfNonExistingDrive) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  const int netTimeout = 1;
  castor::log::DummyLogger log("unittest");
  ProcessForkerProxyDummy processForker;
  const std::string hostName = "";
  const CatalogueConfig catalogueConfig;
  castor::tape::System::mockWrapper sysWrapper;
  Catalogue catalogue(netTimeout, log, processForker,
    hostName, catalogueConfig, sysWrapper);
  ASSERT_THROW(catalogue.findDrive(unitName), castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_CatalogueTest, getUnitNames) {
  using namespace castor::tape::tapeserver::daemon;
  TpconfigLines lines;
  lines.push_back(TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "manual@SLOT1"));
  lines.push_back(TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "manual@SLOT2"));
  DriveConfigMap driveConfigs;
  ASSERT_NO_THROW(driveConfigs.enterTpconfigLines(lines));

  const int netTimeout = 1;
  castor::log::DummyLogger log("unittest");
  ProcessForkerProxyDummy processForker;
  const std::string hostName = "";
  const CatalogueConfig catalogueConfig;
  castor::tape::System::mockWrapper sysWrapper;
  Catalogue catalogue(netTimeout, log, processForker,
    hostName, catalogueConfig, sysWrapper);
  ASSERT_NO_THROW(catalogue.populate(driveConfigs));

  {
    std::list<std::string> allUnitNames;
    ASSERT_NO_THROW(allUnitNames = catalogue.getUnitNames());
    ASSERT_EQ((std::list<std::string>::size_type)2, allUnitNames.size());
    const std::string firstOfAllUnitNames = allUnitNames.front();
    allUnitNames.pop_front();
    const std::string secondOfAllUnitNames = allUnitNames.front();
    ASSERT_TRUE(firstOfAllUnitNames == "UNIT1" ||
      firstOfAllUnitNames == "UNIT2");
    ASSERT_TRUE(secondOfAllUnitNames == "UNIT1" ||
      secondOfAllUnitNames == "UNIT2");
    ASSERT_TRUE(firstOfAllUnitNames != secondOfAllUnitNames);
  }
}
} // namespace unitTests
