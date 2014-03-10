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

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, enterDriveDown) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  const std::string dgn = "DGN";
  const DriveCatalogue::DriveState initialState =
    DriveCatalogue::DRIVE_STATE_DOWN;
  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.enterDrive(unitName, dgn, initialState));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_DOWN,
    catalogue.getState(unitName));
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, enterDriveUp) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  const std::string dgn = "DGN";
  const DriveCatalogue::DriveState initialState =
    DriveCatalogue::DRIVE_STATE_UP;
  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.enterDrive(unitName, dgn, initialState));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_UP,
    catalogue.getState(unitName));
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, enterDriveInit) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  const std::string dgn = "DGN";
  const DriveCatalogue::DriveState initialState =
    DriveCatalogue::DRIVE_STATE_INIT;
  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.enterDrive(unitName, dgn, initialState),
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, enterDriveRunning) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  const std::string dgn = "DGN";
  const DriveCatalogue::DriveState initialState =
    DriveCatalogue::DRIVE_STATE_RUNNING;
  DriveCatalogue catalogue;
  ASSERT_THROW(catalogue.enterDrive(unitName, dgn, initialState),
    castor::exception::Exception);
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, enterSameDriveTwice) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  const std::string dgn = "DGN";
  const DriveCatalogue::DriveState initialState = 
    DriveCatalogue::DRIVE_STATE_DOWN;
  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.enterDrive(unitName, dgn, initialState));
  ASSERT_THROW(catalogue.enterDrive(unitName, dgn, initialState),
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

  const std::string unitName = "DRIVE";
  const std::string dgn = "DGN";
  const DriveCatalogue::DriveState initialState =
    DriveCatalogue::DRIVE_STATE_DOWN;
  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.enterDrive(unitName, dgn, initialState));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_DOWN, catalogue.getState(unitName));
  ASSERT_NO_THROW(catalogue.configureUp(unitName));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_UP,
    catalogue.getState(unitName));
  castor::tape::legacymsg::RtcpJobRqstMsgBody job;
  job.volReqId = 1111;
  job.clientPort = 2222;
  job.clientEuid = 3333;
  job.clientEgid = 4444;
  castor::utils::copyString(job.clientHost, "CLIENT_HOST");
  castor::utils::copyString(job.dgn, "DGN");
  castor::utils::copyString(job.driveUnit, "UNIT");
  castor::utils::copyString(job.clientUserName, "USER");
  ASSERT_NO_THROW(catalogue.tapeSessionStarted(unitName, job));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_RUNNING,
    catalogue.getState(unitName));
  ASSERT_EQ(job.volReqId, catalogue.getJob(unitName).volReqId);
  ASSERT_EQ(job.clientPort, catalogue.getJob(unitName).clientPort);
  ASSERT_EQ(job.clientEuid, catalogue.getJob(unitName).clientEuid);
  ASSERT_EQ(job.clientEgid, catalogue.getJob(unitName).clientEgid);
  ASSERT_EQ(std::string(job.clientHost),
    std::string(catalogue.getJob(unitName).clientHost));
  ASSERT_EQ(std::string(job.dgn),
    std::string(catalogue.getJob(unitName).dgn));
  ASSERT_EQ(std::string(job.driveUnit),
    std::string(catalogue.getJob(unitName).driveUnit));
  ASSERT_EQ(std::string(job.clientUserName),
    std::string(catalogue.getJob(unitName).clientUserName));
  ASSERT_NO_THROW(catalogue.tapeSessionSuceeeded(unitName));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_UP,
    catalogue.getState(unitName));
  ASSERT_NO_THROW(catalogue.configureDown(unitName));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_DOWN,
    catalogue.getState(unitName));
}

TEST_F(castor_tape_tapeserver_daemon_DriveCatalogueTest, dgnMismatch) {
  using namespace castor::tape::tapeserver::daemon;

  const std::string unitName = "DRIVE";
  const std::string dgn = "DGN1";
  const DriveCatalogue::DriveState initialState = 
    DriveCatalogue::DRIVE_STATE_DOWN;
  DriveCatalogue catalogue;
  ASSERT_NO_THROW(catalogue.enterDrive(unitName, dgn, initialState));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_DOWN, catalogue.getState(unitName));
  ASSERT_NO_THROW(catalogue.configureUp(unitName));
  ASSERT_EQ(DriveCatalogue::DRIVE_STATE_UP,
    catalogue.getState(unitName));
  castor::tape::legacymsg::RtcpJobRqstMsgBody job;
  job.volReqId = 1111;
  job.clientPort = 2222;
  job.clientEuid = 3333;
  job.clientEgid = 4444;
  castor::utils::copyString(job.clientHost, "CLIENT_HOST");
  castor::utils::copyString(job.dgn, "DGN2");
  castor::utils::copyString(job.driveUnit, "UNIT");
  castor::utils::copyString(job.clientUserName, "USER");
  ASSERT_THROW(catalogue.tapeSessionStarted(unitName, job),
    castor::exception::Exception);
}

} // namespace unitTests
