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

#include "castor/mediachanger/LibrarySlot.hpp"
#include "castor/mediachanger/LibrarySlotParser.hpp"
#include "castor/mediachanger/MediaChangerFacade.hpp"

#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <sstream>

namespace unitTests {

class castor_mediachanger_MediaChangerFacadeTest : public ::testing::Test {
protected:

  struct MockAcsProxy: public castor::messages::AcsProxy {
    unsigned int nbTimesMountTapeReadOnlyCalled;
    unsigned int nbTimesMountTapeReadWriteCalled;
    unsigned int nbTimesDismountTapeCalled;
    unsigned int nbTimesForceDismountTapeCalled;

    MockAcsProxy():
      nbTimesMountTapeReadOnlyCalled(0),
      nbTimesMountTapeReadWriteCalled(0),
      nbTimesDismountTapeCalled(0),
      nbTimesForceDismountTapeCalled(0) {
    }

    void mountTapeReadOnly(const std::string &vid,
      const castor::mediachanger::AcsLibrarySlot &librarySlot) {
      nbTimesMountTapeReadOnlyCalled++;
    }
    
    void mountTapeReadWrite(const std::string &vid,
      const castor::mediachanger::AcsLibrarySlot &librarySlot) {
      nbTimesMountTapeReadWriteCalled++;
    }

    void dismountTape(const std::string &vid,
      const castor::mediachanger::AcsLibrarySlot &librarySlot) {
      nbTimesDismountTapeCalled++;
    }

    void forceDismountTape(const std::string &vid,
      const castor::mediachanger::AcsLibrarySlot &librarySlot) {
      nbTimesForceDismountTapeCalled++;
    }
  }; // struct MockAcsProxy

  struct MockMmcProxy: public castor::mediachanger::MmcProxy {
    unsigned int nbTimesMountTapeReadOnlyCalled;
    unsigned int nbTimesMountTapeReadWriteCalled;
    unsigned int nbTimesDismountTapeCalled;
    unsigned int nbTimesForceDismountTapeCalled;

    MockMmcProxy():
      nbTimesMountTapeReadOnlyCalled(0),
      nbTimesMountTapeReadWriteCalled(0),
      nbTimesDismountTapeCalled(0),
      nbTimesForceDismountTapeCalled(0) {
    }

    void mountTapeReadOnly(const std::string &vid,
      const castor::mediachanger::ManualLibrarySlot &librarySlot) {
      nbTimesMountTapeReadOnlyCalled++;
    }

    void mountTapeReadWrite(const std::string &vid,
      const castor::mediachanger::ManualLibrarySlot &librarySlot) {
      nbTimesMountTapeReadWriteCalled++;
    }

    void dismountTape(const std::string &vid,
      const castor::mediachanger::ManualLibrarySlot &librarySlot) {
      nbTimesDismountTapeCalled++;
    }

    void forceDismountTape(const std::string &vid,
      const castor::mediachanger::ManualLibrarySlot &librarySlot) {
      nbTimesForceDismountTapeCalled++;
    }
  }; // struct MockMmcProxy

  struct MockRmcProxy: public castor::legacymsg::RmcProxy {
    unsigned int nbTimesMountTapeReadOnlyCalled;
    unsigned int nbTimesMountTapeReadWriteCalled;
    unsigned int nbTimesDismountTapeCalled;
    unsigned int nbTimesForceDismountTapeCalled;

    MockRmcProxy():
      nbTimesMountTapeReadOnlyCalled(0),
      nbTimesMountTapeReadWriteCalled(0),
      nbTimesDismountTapeCalled(0),
      nbTimesForceDismountTapeCalled(0) {
    }

    void mountTapeReadOnly(const std::string &vid,
      const castor::mediachanger::ScsiLibrarySlot &librarySlot) {
      nbTimesMountTapeReadOnlyCalled++;
    }

    void mountTapeReadWrite(const std::string &vid,
      const castor::mediachanger::ScsiLibrarySlot &librarySlot) {
      nbTimesMountTapeReadWriteCalled++;
    }

    void dismountTape(const std::string &vid,
      const castor::mediachanger::ScsiLibrarySlot &librarySlot) {
      nbTimesDismountTapeCalled++;
    }

    void forceDismountTape(const std::string &vid,
      const castor::mediachanger::ScsiLibrarySlot &librarySlot) {
      nbTimesForceDismountTapeCalled++;
    }
  }; // struct MockRmcProxy

  MockAcsProxy m_acs; // ACS
  MockMmcProxy m_mmc; // Manual
  MockRmcProxy m_rmc; // SCSI

  const std::string m_vid;

  const castor::mediachanger::LibrarySlot *m_acsSlot;
  const castor::mediachanger::LibrarySlot *m_mmcSlot;
  const castor::mediachanger::LibrarySlot *m_rmcSlot;

  castor_mediachanger_MediaChangerFacadeTest():
    m_vid("123456"),
    m_acsSlot(castor::mediachanger::LibrarySlotParser::parse("acs1,2,3,4")),
    m_mmcSlot(castor::mediachanger::LibrarySlotParser::parse("manual")),
    m_rmcSlot(castor::mediachanger::LibrarySlotParser::parse("smc1")) {
  }

  ~castor_mediachanger_MediaChangerFacadeTest() {
    delete(m_acsSlot);
    delete(m_mmcSlot);
    delete(m_rmcSlot);
  }

  virtual void SetUp() {
    m_acs.nbTimesMountTapeReadOnlyCalled  = 0;
    m_acs.nbTimesMountTapeReadWriteCalled = 0;
    m_acs.nbTimesDismountTapeCalled       = 0;

    m_mmc.nbTimesMountTapeReadOnlyCalled  = 0;
    m_mmc.nbTimesMountTapeReadWriteCalled = 0;
    m_mmc.nbTimesDismountTapeCalled       = 0;

    m_rmc.nbTimesMountTapeReadWriteCalled = 0;
    m_rmc.nbTimesDismountTapeCalled       = 0;
  }

  virtual void TearDown() {
    SetUp();
  }

}; // class castor_mediachanger_MediaChangerFacadeTest

TEST_F(castor_mediachanger_MediaChangerFacadeTest, mountTapeReadOnlyAcs) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.mountTapeReadOnly(m_vid, *m_acsSlot);

  ASSERT_EQ(1, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_mmc.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_rmc.nbTimesDismountTapeCalled);
}

TEST_F(castor_mediachanger_MediaChangerFacadeTest, mountTapeReadOnlyMmc) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.mountTapeReadOnly(m_vid, *m_mmcSlot);

  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(1, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_mmc.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_rmc.nbTimesDismountTapeCalled);
}

TEST_F(castor_mediachanger_MediaChangerFacadeTest, mountTapeReadOnlyRmc) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.mountTapeReadOnly(m_vid, *m_rmcSlot);

  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_mmc.nbTimesDismountTapeCalled);

  // SCSI tape-libraries do not support mounting tapes for read-only access
  ASSERT_EQ(1, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_rmc.nbTimesDismountTapeCalled);
}

TEST_F(castor_mediachanger_MediaChangerFacadeTest, mountTapeReadWriteAcs) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.mountTapeReadWrite(m_vid, *m_acsSlot);

  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(1, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_mmc.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_rmc.nbTimesDismountTapeCalled);
}

TEST_F(castor_mediachanger_MediaChangerFacadeTest, mountTapeReadWriteMmc) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.mountTapeReadWrite(m_vid, *m_mmcSlot);

  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(1, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_mmc.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_rmc.nbTimesDismountTapeCalled);
}

TEST_F(castor_mediachanger_MediaChangerFacadeTest, mountTapeReadWriteRmc) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.mountTapeReadWrite(m_vid, *m_rmcSlot);

  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_mmc.nbTimesDismountTapeCalled);

  ASSERT_EQ(1, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_rmc.nbTimesDismountTapeCalled);
}

TEST_F(castor_mediachanger_MediaChangerFacadeTest, dismountTapeAcs) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.dismountTape(m_vid, *m_acsSlot);

  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(1, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_mmc.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_rmc.nbTimesDismountTapeCalled);
}

TEST_F(castor_mediachanger_MediaChangerFacadeTest, dismountTapeMmc) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.dismountTape(m_vid, *m_mmcSlot);

  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(1, m_mmc.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_rmc.nbTimesDismountTapeCalled);
}

TEST_F(castor_mediachanger_MediaChangerFacadeTest, dismountTapeRmc) {
  using namespace castor::mediachanger;

  MediaChangerFacade facade(m_acs, m_mmc, m_rmc);
  facade.dismountTape(m_vid, *m_rmcSlot);

  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_acs.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_acs.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadOnlyCalled);
  ASSERT_EQ(0, m_mmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(0, m_mmc.nbTimesDismountTapeCalled);

  ASSERT_EQ(0, m_rmc.nbTimesMountTapeReadWriteCalled);
  ASSERT_EQ(1, m_rmc.nbTimesDismountTapeCalled);
}

} // namespace unitTests
