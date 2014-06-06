/******************************************************************************
 *         castor/tape/tapeserver/daemon/DriveCatalogueTest.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/utils/utils.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_DriveCatalogueTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, goodDayPopulate) {
  using namespace castor::tape::tapeserver::daemon;

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "DEN11", "SLOT1", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "DEN12", "SLOT1", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "DEN21", "SLOT2", "DEVTYPE2"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "DEN22", "SLOT2", "DEVTYPE2"));

  castor::tape::utils::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(lines);

  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.populateCatalogue(driveConfigs));

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

  const DriveCatalogueEntry &unit1 = catalogue.findConstDrive("UNIT1");
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

  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_DOWN, unit1.getState());
  ASSERT_EQ(std::string("SLOT1"), unit1Config.librarySlot);
  ASSERT_EQ(std::string("DEVTYPE1"), unit1Config.devType);

  ///////////////////
  // UNIT2 assertions
  ///////////////////

  const DriveCatalogueEntry &unit2 = catalogue.findConstDrive("UNIT2");
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

  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_DOWN, unit2.getState());
  ASSERT_EQ(std::string("SLOT2"), unit2Config.librarySlot);
  ASSERT_EQ(std::string("DEVTYPE2"), unit2Config.devType);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, 
  getStateOfNonExistingDrive) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.findDrive(unitName), castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, completeFSTN) {
  using namespace castor::tape::tapeserver::daemon;

  // Start with the tape drive in status DOWN
  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT", "DGN", "DEV", "DEN", "SLOT", "DEVTYPE"));
  castor::tape::utils::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(lines);
  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.populateCatalogue(driveConfigs));
  DriveCatalogueEntry &unit = catalogue.findDrive("UNIT");
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_DOWN, unit.getState());

  // Check that the unit can be found and returned as a const reference
  {
    const DriveCatalogueEntry &constUnit = catalogue.findDrive("UNIT");
    ASSERT_EQ(&unit, &constUnit);
  }

  // Configure the tape drive UP
  ASSERT_NO_THROW(unit.configureUp());
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_UP, unit.getState());

  // Check that there are no tape drives waiting for their mount sessions to
  // be forked
  {
    std::list<std::string> unitNames;
    ASSERT_NO_THROW(unitNames = catalogue.getUnitNames(
      DriveCatalogueEntry::DRIVE_STATE_WAITFORKTRANSFER));
    ASSERT_EQ((std::list<std::string>::size_type)0, unitNames.size());
  }

  // Receive a vdqm job
  castor::legacymsg::RtcpJobRqstMsgBody job;
  job.volReqId = 1111;
  job.clientPort = 2222;
  job.clientEuid = 3333;
  job.clientEgid = 4444;
  castor::utils::copyString(job.clientHost, "CLIENT_HOST");
  castor::utils::copyString(job.dgn, "DGN");
  castor::utils::copyString(job.driveUnit, "UNIT");
  castor::utils::copyString(job.clientUserName, "USER");
  ASSERT_NO_THROW(unit.receivedVdqmJob(job));
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_WAITFORKTRANSFER, unit.getState());
  ASSERT_EQ(job.volReqId, unit.getVdqmJob().volReqId);
  ASSERT_EQ(job.clientPort, unit.getVdqmJob().clientPort);
  ASSERT_EQ(job.clientEuid, unit.getVdqmJob().clientEuid);
  ASSERT_EQ(job.clientEgid, unit.getVdqmJob().clientEgid);
  ASSERT_EQ(std::string(job.clientHost),
    std::string(unit.getVdqmJob().clientHost));
  ASSERT_EQ(std::string(job.dgn), std::string(unit.getVdqmJob().dgn));
  ASSERT_EQ(std::string(job.driveUnit),
    std::string(unit.getVdqmJob().driveUnit));
  ASSERT_EQ(std::string(job.clientUserName),
    std::string(unit.getVdqmJob().clientUserName));

  // Check that there is one tape drive waiting for a mount session to be forked
  {
    std::list<std::string> unitNames;
    ASSERT_NO_THROW(unitNames =
      catalogue.getUnitNames(DriveCatalogueEntry::DRIVE_STATE_WAITFORKTRANSFER));
    ASSERT_EQ((std::list<std::string>::size_type)1, unitNames.size());
    ASSERT_EQ(std::string("UNIT"), unitNames.front());
  }

  // Fork the mount session
  const pid_t sessionPid = 1234;
  ASSERT_NO_THROW(unit.forkedMountSession(sessionPid));
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_RUNNING, unit.getState());
  ASSERT_EQ(sessionPid, unit.getSessionPid());

  // Check that there are no longer any tape drives waiting for their mount
  // sessions to be forked
  {
    std::list<std::string> unitNames;
    ASSERT_NO_THROW(unitNames =
      catalogue.getUnitNames(DriveCatalogueEntry::DRIVE_STATE_WAITFORKTRANSFER));
    ASSERT_EQ((std::list<std::string>::size_type)0, unitNames.size());
  }

  // Check that the unit can be found by process ID
  {
    DriveCatalogueEntry &unitFoundByPid = catalogue.findDrive(sessionPid);
    ASSERT_EQ(&unit, &unitFoundByPid);
  }
  {
    const DriveCatalogueEntry &constUnitFoundByPid =
      catalogue.findConstDrive(sessionPid);
    ASSERT_EQ(&unit, &constUnitFoundByPid);
  }

  // Configure the tape drive DOWN whilst the mount session is running
  ASSERT_NO_THROW(unit.configureDown());
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_WAITDOWN, unit.getState());

  // Configure the tape drive back UP whilst the mount session is running
  ASSERT_NO_THROW(unit.configureUp());
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_RUNNING, unit.getState());

  // Complete the tape session successfully
  ASSERT_NO_THROW(unit.sessionSucceeded());
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_UP, unit.getState());

  // Configure the tape drive DOWN
  ASSERT_NO_THROW(unit.configureDown());
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_DOWN, unit.getState());
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, dgnMismatchStart) {
  using namespace castor::tape::tapeserver::daemon;
  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT", "DGN1", "DEV", "DEN", "SLOT", "DEVTYPE"));
  castor::tape::utils::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(lines);

  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.populateCatalogue(driveConfigs));
  DriveCatalogueEntry &unit = catalogue.findDrive("UNIT");
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_DOWN, unit.getState());
  ASSERT_NO_THROW(unit.configureUp());
  ASSERT_EQ(DriveCatalogueEntry::DRIVE_STATE_UP, unit.getState());
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

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, getUnitNames) {
  using namespace castor::tape::tapeserver::daemon;
  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "DEN1", "SLOT1", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "DEN2", "SLOT2", "DEVTYPE2"));
  castor::tape::utils::DriveConfigMap driveConfigs;
  driveConfigs.enterTpconfigLines(lines);

  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.populateCatalogue(driveConfigs));

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

  {
    std::list<std::string> downUnitNames;
    ASSERT_NO_THROW(downUnitNames =
      catalogue.getUnitNames(DriveCatalogueEntry::DRIVE_STATE_DOWN));
    ASSERT_EQ((std::list<std::string>::size_type)2, downUnitNames.size());
  }

  {
    std::list<std::string> upUnitNames;
    ASSERT_NO_THROW(upUnitNames =
      catalogue.getUnitNames(DriveCatalogueEntry::DRIVE_STATE_UP));
    ASSERT_EQ((std::list<std::string>::size_type)0, upUnitNames.size());
  }
}

} // namespace unitTests
