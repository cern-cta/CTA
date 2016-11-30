/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "mediachanger/LibrarySlot.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "mediachanger/MediaChangerFacade.hpp"

#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <sstream>

namespace unitTests {

class cta_mediachanger_MediaChangerFacadeTest : public ::testing::Test {
protected:

  struct MockAcsProxy: public cta::mediachanger::AcsProxy {
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
      const cta::mediachanger::AcsLibrarySlot &librarySlot) {
      nbTimesMountTapeReadOnlyCalled++;
    }
    
    void mountTapeReadWrite(const std::string &vid,
      const cta::mediachanger::AcsLibrarySlot &librarySlot) {
      nbTimesMountTapeReadWriteCalled++;
    }

    void dismountTape(const std::string &vid,
      const cta::mediachanger::AcsLibrarySlot &librarySlot) {
      nbTimesDismountTapeCalled++;
    }

    void forceDismountTape(const std::string &vid,
      const cta::mediachanger::AcsLibrarySlot &librarySlot) {
      nbTimesForceDismountTapeCalled++;
    }
  }; // struct MockAcsProxy

  struct MockMmcProxy: public cta::mediachanger::MmcProxy {
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
      const cta::mediachanger::ManualLibrarySlot &librarySlot) {
      nbTimesMountTapeReadOnlyCalled++;
    }

    void mountTapeReadWrite(const std::string &vid,
      const cta::mediachanger::ManualLibrarySlot &librarySlot) {
      nbTimesMountTapeReadWriteCalled++;
    }

    void dismountTape(const std::string &vid,
      const cta::mediachanger::ManualLibrarySlot &librarySlot) {
      nbTimesDismountTapeCalled++;
    }

    void forceDismountTape(const std::string &vid,
      const cta::mediachanger::ManualLibrarySlot &librarySlot) {
      nbTimesForceDismountTapeCalled++;
    }
  }; // struct MockMmcProxy

  struct MockRmcProxy: public cta::mediachanger::RmcProxy {
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
      const cta::mediachanger::ScsiLibrarySlot &librarySlot) {
      nbTimesMountTapeReadOnlyCalled++;
    }

    void mountTapeReadWrite(const std::string &vid,
      const cta::mediachanger::ScsiLibrarySlot &librarySlot) {
      nbTimesMountTapeReadWriteCalled++;
    }

    void dismountTape(const std::string &vid,
      const cta::mediachanger::ScsiLibrarySlot &librarySlot) {
      nbTimesDismountTapeCalled++;
    }

    void forceDismountTape(const std::string &vid,
      const cta::mediachanger::ScsiLibrarySlot &librarySlot) {
      nbTimesForceDismountTapeCalled++;
    }
  }; // struct MockRmcProxy

  MockAcsProxy m_acs; // ACS
  MockMmcProxy m_mmc; // Manual
  MockRmcProxy m_rmc; // SCSI

  const std::string m_vid;

  const cta::mediachanger::LibrarySlot *m_acsSlot;
  const cta::mediachanger::LibrarySlot *m_mmcSlot;
  const cta::mediachanger::LibrarySlot *m_rmcSlot;

  cta_mediachanger_MediaChangerFacadeTest():
    m_vid("123456"),
    m_acsSlot(cta::mediachanger::LibrarySlotParser::parse("acs1,2,3,4")),
    m_mmcSlot(cta::mediachanger::LibrarySlotParser::parse("manual")),
    m_rmcSlot(cta::mediachanger::LibrarySlotParser::parse("smc1")) {
  }

  ~cta_mediachanger_MediaChangerFacadeTest() {
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

}; // class cta_mediachanger_MediaChangerFacadeTest

TEST_F(cta_mediachanger_MediaChangerFacadeTest, mountTapeReadOnlyAcs) {
  using namespace cta::mediachanger;

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

TEST_F(cta_mediachanger_MediaChangerFacadeTest, mountTapeReadOnlyMmc) {
  using namespace cta::mediachanger;

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

TEST_F(cta_mediachanger_MediaChangerFacadeTest, mountTapeReadOnlyRmc) {
  using namespace cta::mediachanger;

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

TEST_F(cta_mediachanger_MediaChangerFacadeTest, mountTapeReadWriteAcs) {
  using namespace cta::mediachanger;

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

TEST_F(cta_mediachanger_MediaChangerFacadeTest, mountTapeReadWriteMmc) {
  using namespace cta::mediachanger;

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

TEST_F(cta_mediachanger_MediaChangerFacadeTest, mountTapeReadWriteRmc) {
  using namespace cta::mediachanger;

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

TEST_F(cta_mediachanger_MediaChangerFacadeTest, dismountTapeAcs) {
  using namespace cta::mediachanger;

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

TEST_F(cta_mediachanger_MediaChangerFacadeTest, dismountTapeMmc) {
  using namespace cta::mediachanger;

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

TEST_F(cta_mediachanger_MediaChangerFacadeTest, dismountTapeRmc) {
  using namespace cta::mediachanger;

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
