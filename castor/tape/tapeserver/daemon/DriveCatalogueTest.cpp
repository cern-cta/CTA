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

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, driveState2Str) {
  using namespace castor::tape::tapeserver::daemon;

  ASSERT_EQ(std::string("INIT"),
    DriveCatalogue::driveState2Str(DriveCatalogue::DRIVE_STATE_INIT));
  ASSERT_EQ(std::string("DOWN"),
    DriveCatalogue::driveState2Str(DriveCatalogue::DRIVE_STATE_DOWN));
  ASSERT_EQ(std::string("UP"),
    DriveCatalogue::driveState2Str(DriveCatalogue::DRIVE_STATE_UP));
  ASSERT_EQ(std::string("RUNNING"),
    DriveCatalogue::driveState2Str(DriveCatalogue::DRIVE_STATE_RUNNING));
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, goodDayPopulate) {
  using namespace castor::tape::tapeserver::daemon;

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "DEN11", "down", "POSITION1", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV1", "DEN12", "down", "POSITION1", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "DEN21", "up", "POSITION2", "DEVTYPE2"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT2", "DGN2", "DEV2", "DEN22", "up", "POSITION2", "DEVTYPE2"));

  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.populateCatalogue(lines));

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

  {
    std::string fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getDgn("UNIT1"));
    ASSERT_EQ(std::string("DGN1"), fromCatalogue);
  }

  {
    std::string fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getDevFilename("UNIT1"));
    ASSERT_EQ(std::string("DEV1"), fromCatalogue);
  }

  {
    std::list<std::string> fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getDensities("UNIT1"));
    ASSERT_EQ((std::list<std::string>::size_type)2, 
      fromCatalogue.size());
    ASSERT_EQ("DEN11", fromCatalogue.front());
    fromCatalogue.pop_front();
    ASSERT_EQ("DEN12", fromCatalogue.front());
  }

  {
    DriveCatalogue::DriveState fromCatalogue = DriveCatalogue::DRIVE_STATE_INIT;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getState("UNIT1"));
    ASSERT_EQ(DriveCatalogue::DRIVE_STATE_DOWN, fromCatalogue);
  }

  {
    std::string fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getPositionInLibrary("UNIT1"));
    ASSERT_EQ(std::string("POSITION1"), fromCatalogue);
  }

  {
    std::string fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getDevType("UNIT1"));
    ASSERT_EQ(std::string("DEVTYPE1"), fromCatalogue);
  }

  ///////////////////
  // UNIT2 assertions
  ///////////////////

  {
    std::string fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getDgn("UNIT2"));
    ASSERT_EQ(std::string("DGN2"), fromCatalogue);
  }

  {
    std::string fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getDevFilename("UNIT2"));
    ASSERT_EQ(std::string("DEV2"), fromCatalogue);
  }

  {
    std::list<std::string> fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getDensities("UNIT2"));
    ASSERT_EQ((std::list<std::string>::size_type)2,
      fromCatalogue.size());
    ASSERT_EQ("DEN21", fromCatalogue.front());
    fromCatalogue.pop_front();
    ASSERT_EQ("DEN22", fromCatalogue.front());
  }

  {
    DriveCatalogue::DriveState fromCatalogue = DriveCatalogue::DRIVE_STATE_INIT;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getState("UNIT2"));
    ASSERT_EQ(DriveCatalogue::DRIVE_STATE_UP, fromCatalogue);
  }

  {
    std::string fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getPositionInLibrary("UNIT2"));
    ASSERT_EQ(std::string("POSITION2"), fromCatalogue);
  }

  {
    std::string fromCatalogue;
    ASSERT_NO_THROW(fromCatalogue = catalogue.getDevType("UNIT2"));
    ASSERT_EQ(std::string("DEVTYPE2"), fromCatalogue);
  }
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest,
  invalidInitialStatePopulate) {
  using namespace castor::tape::tapeserver::daemon;
  
  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT", "DGN", "DEV", "DEN", "invalid", "POSITION", "DEVTYPE"));
    
  DriveCatalogue catalogue; 
  ASSERT_THROW(catalogue.populateCatalogue(lines),
    castor::exception::Exception);
} 

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest,
  dgnMismatchPopulate) {
  using namespace castor::tape::tapeserver::daemon;

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN1", "DEV", "DEN1", "down", "POSITION", "DEVTYPE"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN2", "DEV", "DEN2", "down", "POSITION", "DEVTYPE"));

  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.populateCatalogue(lines),
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest,
  initialStateMismatchPopulate) {
  using namespace castor::tape::tapeserver::daemon;

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV", "DEN1", "down", "POSITION", "DEVTYPE"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV", "DEN2", "up", "POSITION", "DEVTYPE"));

  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.populateCatalogue(lines),
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest,
  systemDeviceMismatchPopulate) {
  using namespace castor::tape::tapeserver::daemon;

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV1", "DEN1", "down", "POSITION", "DEVTYPE"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV2", "DEN2", "down", "POSITION", "DEVTYPE"));

  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.populateCatalogue(lines),
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest,
  positionInLibraryMismatchPopulate) {
  using namespace castor::tape::tapeserver::daemon;
  
  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV", "DEN1", "down", "POSITION1", "DEVTYPE"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV", "DEN2", "down", "POSITION2", "DEVTYPE"));
    
  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.populateCatalogue(lines),
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest,
  deviceTypeMismatchPopulate) {
  using namespace castor::tape::tapeserver::daemon;

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV", "DEN1", "down", "POSITION", "DEVTYPE1"));
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT1", "DGN", "DEV", "DEN2", "down", "POSITION", "DEVTYPE2"));

  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.populateCatalogue(lines),
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, 
  getStateOfNonExistingDrive) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.getState(unitName), 
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, completeFSTN) {
  using namespace castor::tape::tapeserver::daemon;

  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT", "DGN", "DEV", "DEN", "down", "POSITION", "DEVTYPE"));

  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.populateCatalogue(lines));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_DOWN, catalogue.getState("UNIT"));
  ASSERT_NO_THROW(catalogue.configureUp("UNIT"));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_UP,
    catalogue.getState("UNIT"));
  castor::tape::legacymsg::RtcpJobRqstMsgBody job;
  job.volReqId = 1111;
  job.clientPort = 2222;
  job.clientEuid = 3333;
  job.clientEgid = 4444;
  castor::utils::copyString(job.clientHost, "CLIENT_HOST");
  castor::utils::copyString(job.dgn, "DGN");
  castor::utils::copyString(job.driveUnit, "UNIT");
  castor::utils::copyString(job.clientUserName, "USER");
  ASSERT_NO_THROW(catalogue.tapeSessionStarted("UNIT", job));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_RUNNING,
    catalogue.getState("UNIT"));
  ASSERT_EQ(job.volReqId, catalogue.getJob("UNIT").volReqId);
  ASSERT_EQ(job.clientPort, catalogue.getJob("UNIT").clientPort);
  ASSERT_EQ(job.clientEuid, catalogue.getJob("UNIT").clientEuid);
  ASSERT_EQ(job.clientEgid, catalogue.getJob("UNIT").clientEgid);
  ASSERT_EQ(std::string(job.clientHost),
    std::string(catalogue.getJob("UNIT").clientHost));
  ASSERT_EQ(std::string(job.dgn),
    std::string(catalogue.getJob("UNIT").dgn));
  ASSERT_EQ(std::string(job.driveUnit),
    std::string(catalogue.getJob("UNIT").driveUnit));
  ASSERT_EQ(std::string(job.clientUserName),
    std::string(catalogue.getJob("UNIT").clientUserName));
  ASSERT_NO_THROW(catalogue.tapeSessionSucceeded("UNIT"));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_UP,
    catalogue.getState("UNIT"));
  ASSERT_NO_THROW(catalogue.configureDown("UNIT"));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_DOWN,
    catalogue.getState("UNIT"));
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, dgnMismatchStart) {
  using namespace castor::tape::tapeserver::daemon;
  castor::tape::utils::TpconfigLines lines;
  lines.push_back(castor::tape::utils::TpconfigLine(
    "UNIT", "DGN1", "DEV", "DEN", "down", "POSITION", "DEVTYPE"));

  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.populateCatalogue(lines));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_DOWN, catalogue.getState("UNIT"));
  ASSERT_NO_THROW(catalogue.configureUp("UNIT"));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_UP,
    catalogue.getState("UNIT"));
  castor::tape::legacymsg::RtcpJobRqstMsgBody job;
  job.volReqId = 1111;
  job.clientPort = 2222;
  job.clientEuid = 3333;
  job.clientEgid = 4444;
  castor::utils::copyString(job.clientHost, "CLIENT_HOST");
  castor::utils::copyString(job.dgn, "DGN2");
  castor::utils::copyString(job.driveUnit, "UNIT");
  castor::utils::copyString(job.clientUserName, "USER");
  ASSERT_THROW(catalogue.tapeSessionStarted("UNIT", job),
    castor::exception::Exception);
}

} // namespace unitTests
