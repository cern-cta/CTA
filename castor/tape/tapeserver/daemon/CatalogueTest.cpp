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

#include "castor/legacymsg/CupvProxyDummy.hpp"
#include "castor/legacymsg/VdqmProxyDummy.hpp"
#include "castor/legacymsg/VmgrProxyDummy.hpp"
#include "castor/log/DummyLogger.hpp"
#include "castor/tape/tapeserver/daemon/Catalogue.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxyDummy.hpp"
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

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "DEN11", "manual@SLOT1", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "DEN12", "manual@SLOT1", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "DEN21", "manual@SLOT2", "DEVTYPE2"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "DEN22", "manual@SLOT2", "DEVTYPE2"));

  castor::tape::utils::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(lines);

  castor::log::DummyLogger log("unittest");
  ProcessForkerProxyDummy processForker;
  const bool isGrantedReturnValue = true;
  castor::legacymsg::CupvProxyDummy cupv(isGrantedReturnValue);
  castor::legacymsg::VdqmProxyDummy vdqm;
  castor::legacymsg::VmgrProxyDummy vmgr;
  const std::string hostName = "";
  const int netTimeout = 1;
  Catalogue catalogue(netTimeout, log, processForker, cupv, vdqm, vmgr,
    hostName);
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
  const castor::tape::utils::DriveConfig &unit1Config = unit1.getConfig();
  
  ASSERT_EQ(std::string("DGN1"), unit1Config.dgn);
  ASSERT_EQ(std::string("DEV1"), unit1Config.devFilename);
  
  {
    std::list<std::string> fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = unit1Config.densities);
    ASSERT_EQ((std::list<std::string>::size_type)2, 
      fromCatalogue.size());
    ASSERT_EQ("DEN11", fromCatalogue.front());
    fromCatalogue.pop_front();
    ASSERT_EQ("DEN12", fromCatalogue.front());
  }
  
  ASSERT_EQ(CatalogueDrive::DRIVE_STATE_DOWN, unit1.getState());
  ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
    unit1Config.librarySlot.getLibraryType());
  ASSERT_EQ(std::string("manual@SLOT1"), unit1Config.librarySlot.str());
  ASSERT_EQ(std::string("DEVTYPE1"), unit1Config.devType);
  
  ///////////////////
  // UNIT2 assertions
  ///////////////////
  
  const CatalogueDrive &unit2 = catalogue.findDrive("UNIT2");
  const castor::tape::utils::DriveConfig &unit2Config = unit2.getConfig();
  
  ASSERT_EQ(std::string("DGN2"), unit2Config.dgn);
  ASSERT_EQ(std::string("DEV2"), unit2Config.devFilename);

  {
    std::list<std::string> fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = unit2Config.densities);
    ASSERT_EQ((std::list<std::string>::size_type)2,
      fromCatalogue.size());
    ASSERT_EQ("DEN21", fromCatalogue.front());
    fromCatalogue.pop_front();
    ASSERT_EQ("DEN22", fromCatalogue.front());
  }

  ASSERT_EQ(CatalogueDrive::DRIVE_STATE_DOWN, unit2.getState());
  ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
    unit2Config.librarySlot.getLibraryType());
  ASSERT_EQ(std::string("manual@SLOT2"), unit2Config.librarySlot.str());
  ASSERT_EQ(std::string("DEVTYPE2"), unit2Config.devType);
}

TEST_F(castor_tape_tapeserver_daemon_CatalogueTest, 
  getStateOfNonExistingDrive) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  const int netTimeout = 1;
  castor::log::DummyLogger log("unittest");
  ProcessForkerProxyDummy processForker;
  const bool isGrantedReturnValue = true;
  castor::legacymsg::CupvProxyDummy cupv(isGrantedReturnValue);
  castor::legacymsg::VdqmProxyDummy vdqm;
  castor::legacymsg::VmgrProxyDummy vmgr;
  const std::string hostName = "";
  Catalogue catalogue(netTimeout, log, processForker, cupv, vdqm, vmgr,
    hostName);
  ASSERT_THROW(catalogue.findDrive(unitName), castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_CatalogueTest, dgnMismatchStart) {
  using namespace castor::tape::tapeserver::daemon;
  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT", "DGN1", "DEV", "DEN", "manual@SLOT", "DEVTYPE"));
  castor::tape::utils::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(lines);

  const int netTimeout = 1;
  castor::log::DummyLogger log("unittest");
  ProcessForkerProxyDummy processForker;
  const bool isGrantedReturnValue = true;
  castor::legacymsg::CupvProxyDummy cupv(isGrantedReturnValue);
  castor::legacymsg::VdqmProxyDummy vdqm;
  castor::legacymsg::VmgrProxyDummy vmgr;
  const std::string hostName = "";
  Catalogue catalogue(netTimeout, log, processForker, cupv, vdqm, vmgr,
    hostName);
  ASSERT_NO_THROW(catalogue.populate(driveConfigs));
  CatalogueDrive &unit = catalogue.findDrive("UNIT");
  ASSERT_EQ(CatalogueDrive::DRIVE_STATE_DOWN, unit.getState());
  ASSERT_NO_THROW(unit.configureUp());
  ASSERT_EQ(CatalogueDrive::DRIVE_STATE_UP, unit.getState());
  castor::legacymsg::RtcpJobRqstMsgBody job;
  job.volReqId = 1111;
  job.clientPort = 2222;
  job.clientEuid = 3333;
  job.clientEgid = 4444;
  castor::utils::copyString(job.clientHost, "CLIENT_HOST");
  castor::utils::copyString(job.dgn, "DGN2");
  castor::utils::copyString(job.driveUnit, "UNIT");
  castor::utils::copyString(job.clientUserName, "USER");
  ASSERT_THROW(unit.receivedVdqmJob(job), castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_CatalogueTest, getUnitNames) {
  using namespace castor::tape::tapeserver::daemon;
  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "DEN1", "manual@SLOT1", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "DEN2", "manual@SLOT2", "DEVTYPE2"));
  castor::tape::utils::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(lines);


  const int netTimeout = 1;
  castor::log::DummyLogger log("unittest");
  ProcessForkerProxyDummy processForker;
  const bool isGrantedReturnValue = true;
  castor::legacymsg::CupvProxyDummy cupv(isGrantedReturnValue);
  castor::legacymsg::VdqmProxyDummy vdqm;
  castor::legacymsg::VmgrProxyDummy vmgr;
  const std::string hostName = "";
  Catalogue catalogue(netTimeout, log, processForker, cupv, vdqm, vmgr,
    hostName);
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
