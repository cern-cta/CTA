// ----------------------------------------------------------------------
// File: File/StructuresTest.cc
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>
#include "Structures.hh"

namespace UnitTests {

  TEST(FILE_Structures, VOL1) {
    Tape::AULFile::VOL1 vol1Label;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof (vol1Label));
    /* test that verify throws exceptions */
    EXPECT_ANY_THROW({
      vol1Label.verify();
    });
    vol1Label.fill("test");
    ASSERT_NO_THROW({
      vol1Label.verify();
    });
    ASSERT_EQ("test  ", vol1Label.getVSN());
  }

  TEST(FILE_Structures, HDR1) {
    Tape::AULFile::HDR1 hdr1Label;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof (hdr1Label));
    /* test that verify throws exceptions */
    EXPECT_ANY_THROW({
      hdr1Label.verify();
    });
    hdr1Label.fill("AABBCC", "test", 12345);
    ASSERT_NO_THROW({
      hdr1Label.verify();
    });
    ASSERT_EQ("test  ", hdr1Label.getVSN());
    ASSERT_EQ("AABBCC           ", hdr1Label.getFileId());
    ASSERT_EQ("2345", hdr1Label.getfSeq());
  }
  TEST(FILE_Structures, HDR1PRELABEL) {
    Tape::AULFile::HDR1PRELABEL hdr1Prelabel;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof (hdr1Prelabel));
    /* test that verify throws exceptions */
    EXPECT_ANY_THROW({
      hdr1Prelabel.verify();
    });
    hdr1Prelabel.fill("TEST");
    ASSERT_NO_THROW({
      hdr1Prelabel.verify();
    });
    ASSERT_EQ("TEST  ", hdr1Prelabel.getVSN());
    ASSERT_EQ("PRELABEL         ", hdr1Prelabel.getFileId());
    ASSERT_EQ("0001", hdr1Prelabel.getfSeq());
  }

  TEST(FILE_Structures, EOF1) {
    Tape::AULFile::EOF1 eof1Label;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof (eof1Label));
    /* test that verify throws exceptions */
    EXPECT_ANY_THROW({
      eof1Label.verify();
    });
    eof1Label.fill("AABBCC", "test", 12345, 7654);
    ASSERT_NO_THROW({
      eof1Label.verify();
    });
    ASSERT_EQ("test  ", eof1Label.getVSN());
    ASSERT_EQ("AABBCC           ", eof1Label.getFileId());
    ASSERT_EQ("2345", eof1Label.getfSeq());
    ASSERT_EQ("007654", eof1Label.getBlockCount());
  }

  TEST(FILE_Structures, HDR2) {
    Tape::AULFile::HDR2 hdr2Label;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof (hdr2Label));
    /* test that verify throws exceptions */
    EXPECT_ANY_THROW({
      hdr2Label.verify();
    });
    hdr2Label.fill(262144);
    ASSERT_NO_THROW({
      hdr2Label.verify();
    });
    ASSERT_EQ("00000", hdr2Label.getBlockLength());
    hdr2Label.fill(32760);
    ASSERT_EQ("32760", hdr2Label.getBlockLength());
  }

  TEST(FILE_Structures, EOF2) {
    Tape::AULFile::EOF2 eof2Label;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof (eof2Label));
    /* test that verify throws exceptions */
    EXPECT_ANY_THROW({
      eof2Label.verify();
    });
    eof2Label.fill(262144);
    ASSERT_NO_THROW({
      eof2Label.verify();
    });
    ASSERT_EQ("00000", eof2Label.getBlockLength());
    eof2Label.fill(32760);
    ASSERT_EQ("32760", eof2Label.getBlockLength());
  }

  TEST(FILE_Structures, UHL1) {
    Tape::AULFile::UHL1 uhl1Label;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof (uhl1Label));
    /* test that verify throws exceptions */
    EXPECT_ANY_THROW({
      uhl1Label.verify();
    });
    Tape::deviceInfo deviceInfo;
    deviceInfo.vendor = "TEST";
    deviceInfo.serialNumber = "XXYYTEST";
    deviceInfo.product = "TEST DRIVE";
    uhl1Label.fill(12345, 262144,
            std::string("CERN"), std::string("TESTMOVER"), deviceInfo);
    ASSERT_NO_THROW({
      uhl1Label.verify();
    });
    ASSERT_EQ("0000262144", uhl1Label.getBlockSize());
    ASSERT_EQ("0000012345", uhl1Label.getfSeq());
  }

  TEST(FILE_Structures, UTL1) {
    Tape::AULFile::UTL1 utl1Label;
    /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(80U, sizeof (utl1Label));
    /* test that verify throws exceptions */
    EXPECT_ANY_THROW({
      utl1Label.verify();
    });
    Tape::deviceInfo deviceInfo;
    deviceInfo.vendor = "TEST";
    deviceInfo.serialNumber = "XXYYTEST";
    deviceInfo.product = "TEST DRIVE";
    utl1Label.fill(12345, 262144,
            std::string("CERN"), std::string("TESTMOVER"), deviceInfo);
    ASSERT_NO_THROW({
      utl1Label.verify();
    });
    ASSERT_EQ("0000262144", utl1Label.getBlockSize());
    ASSERT_EQ("0000012345", utl1Label.getfSeq());
  }
}
