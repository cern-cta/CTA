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

#include "castor/tape/tapeserver/daemon/DriveConfig.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_tapeserver_daemon_DriveConfigTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_tapeserver_daemon_DriveConfigTest, manual_drive_slot) {
  using namespace castor::tape::tapeserver::daemon;

  DriveConfig config(
    "unit",
    "dgn",
    "devFilename",
    "manual");

  ASSERT_EQ("unit", config.getUnitName());
  ASSERT_EQ("dgn", config.getDgn());
  ASSERT_EQ("devFilename", config.getDevFilename());
  ASSERT_NO_THROW(config.getLibrarySlot());
  {
    const castor::mediachanger::LibrarySlot &librarySlot =
      config.getLibrarySlot();
    ASSERT_EQ("manual", librarySlot.str());
    ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
      librarySlot.getLibraryType());
  }
}

TEST_F(castor_tape_tapeserver_daemon_DriveConfigTest, copy_constructor) {
  using namespace castor::tape::tapeserver::daemon;

  DriveConfig config1(
    "unit1",
    "dgn1",
    "devFilename1",
    "manual1");
  ASSERT_EQ("unit1", config1.getUnitName());
  ASSERT_EQ("dgn1", config1.getDgn());
  ASSERT_EQ("devFilename1", config1.getDevFilename());
  ASSERT_NO_THROW(config1.getLibrarySlot());
  {
    const castor::mediachanger::LibrarySlot &librarySlot1 =
      config1.getLibrarySlot();
    ASSERT_EQ("manual1", librarySlot1.str());
    ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
      librarySlot1.getLibraryType());
  }

  DriveConfig config2(config1);
  ASSERT_EQ("unit1", config2.getUnitName());
  ASSERT_EQ("dgn1", config2.getDgn());
  ASSERT_EQ("devFilename1", config2.getDevFilename());
  ASSERT_NO_THROW(config2.getLibrarySlot());
  {
    const castor::mediachanger::LibrarySlot &librarySlot2 =
      config2.getLibrarySlot();
    ASSERT_EQ("manual1", librarySlot2.str());
    ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
      librarySlot2.getLibraryType());
  }
}

TEST_F(castor_tape_tapeserver_daemon_DriveConfigTest, assignment) {
  using namespace castor::tape::tapeserver::daemon;

  DriveConfig config1(
    "unit1",
    "dgn1",
    "devFilename1",
    "manual1");
  ASSERT_EQ("unit1", config1.getUnitName());
  ASSERT_EQ("dgn1", config1.getDgn());
  ASSERT_EQ("devFilename1", config1.getDevFilename());
  ASSERT_NO_THROW(config1.getLibrarySlot());
  {
    const castor::mediachanger::LibrarySlot &librarySlot1 =
      config1.getLibrarySlot();
    ASSERT_EQ("manual1", librarySlot1.str());
    ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
      librarySlot1.getLibraryType());
  }

  DriveConfig config2(
    "unit2",
    "dgn2",
    "devFilename2",
    "manual2");
  ASSERT_EQ("unit2", config2.getUnitName());
  ASSERT_EQ("dgn2", config2.getDgn());
  ASSERT_EQ("devFilename2", config2.getDevFilename());
  ASSERT_NO_THROW(config2.getLibrarySlot());
  {
    const castor::mediachanger::LibrarySlot &librarySlot2 =
      config2.getLibrarySlot();
    ASSERT_EQ("manual2", librarySlot2.str());
    ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
      librarySlot2.getLibraryType());
  }

  config1 = config2;

  ASSERT_EQ("unit2", config1.getUnitName());
  ASSERT_EQ("dgn2", config1.getDgn());
  ASSERT_EQ("devFilename2", config1.getDevFilename());
  ASSERT_NO_THROW(config1.getLibrarySlot());
  {
    const castor::mediachanger::LibrarySlot &librarySlot1 =
      config2.getLibrarySlot();
    ASSERT_EQ("manual2", librarySlot1.str());
    ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
      librarySlot1.getLibraryType());
  }

  ASSERT_EQ("unit2", config2.getUnitName());
  ASSERT_EQ("dgn2", config2.getDgn());
  ASSERT_EQ("devFilename2", config2.getDevFilename());
  ASSERT_NO_THROW(config2.getLibrarySlot());
  {
    const castor::mediachanger::LibrarySlot &librarySlot2 =
      config2.getLibrarySlot();
    ASSERT_EQ("manual2", librarySlot2.str());
    ASSERT_EQ(castor::mediachanger::TAPE_LIBRARY_TYPE_MANUAL,
      librarySlot2.getLibraryType());
  }
}

} // namespace unitTests
