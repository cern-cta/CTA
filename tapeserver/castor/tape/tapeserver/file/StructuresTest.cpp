/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "castor/tape/tapeserver/file/Structures.hpp"
#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>

namespace unitTests {

class testVOL1 : public castor::tape::tapeFile::VOL1 {
public:
  void backdoorSetLBPMethodString(const std::string& LBPString) { strcpy(m_LBPMethod, LBPString.c_str()); }
};

TEST(castor_tape_AULFile, VOL1) {
  typedef castor::tape::SCSI::logicBlockProtectionMethod LBPM;
  testVOL1 vol1Label;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U, sizeof(vol1Label));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ vol1Label.verify(); });
  vol1Label.fill("test", LBPM::DoNotUse);
  ASSERT_NO_THROW({ vol1Label.verify(); });
  ASSERT_EQ("test  ", vol1Label.getVSN());
  /* Validate that parser understands correctly values for LBP */
  uint8_t* buf = (uint8_t*) &vol1Label;
  ASSERT_EQ(buf[77], '0');
  ASSERT_EQ(buf[78], '0');
  vol1Label.backdoorSetLBPMethodString("  ");
  ASSERT_NO_THROW(vol1Label.getLBPMethod());
  ASSERT_EQ((int) LBPM::DoNotUse, (int) vol1Label.getLBPMethod());
  ASSERT_EQ(buf[77], ' ');
  ASSERT_EQ(buf[78], ' ');
  vol1Label.backdoorSetLBPMethodString("01");
  ASSERT_NO_THROW(vol1Label.getLBPMethod());
  ASSERT_EQ((int) LBPM::ReedSolomon, (int) vol1Label.getLBPMethod());
  ASSERT_EQ(buf[77], '0');
  ASSERT_EQ(buf[78], '1');
  vol1Label.backdoorSetLBPMethodString("00");
  ASSERT_NO_THROW(vol1Label.getLBPMethod());
  ASSERT_EQ((int) LBPM::DoNotUse, (int) vol1Label.getLBPMethod());
  vol1Label.backdoorSetLBPMethodString("02");
  ASSERT_NO_THROW(vol1Label.getLBPMethod());
  ASSERT_EQ((int) LBPM::CRC32C, (int) vol1Label.getLBPMethod());
  ASSERT_EQ(buf[77], '0');
  ASSERT_EQ(buf[78], '2');
  vol1Label.backdoorSetLBPMethodString("03");
  ASSERT_THROW(vol1Label.getLBPMethod(), cta::exception::Exception);
  vol1Label.backdoorSetLBPMethodString("XY");
  ASSERT_THROW(vol1Label.getLBPMethod(), cta::exception::Exception);
}

class testVOL1withCRC : public castor::tape::tapeFile::VOL1withCrc {
public:
  void backdoorSetLBPMethodString(const std::string& LBPString) { strcpy(m_LBPMethod, LBPString.c_str()); }
};

TEST(castor_tape_AULFile, VOL1WithCRC) {
  typedef castor::tape::SCSI::logicBlockProtectionMethod LBPM;
  testVOL1withCRC vol1LabelWithCRC;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U + 4U, sizeof(vol1LabelWithCRC));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ vol1LabelWithCRC.verify(); });
  vol1LabelWithCRC.fill("test", LBPM::DoNotUse);
  ASSERT_NO_THROW({ vol1LabelWithCRC.verify(); });
  ASSERT_EQ("test  ", vol1LabelWithCRC.getVSN());
  /* Validate that parser understands correctly values for LBP */
  uint8_t* buf = (uint8_t*) &vol1LabelWithCRC;
  ASSERT_EQ(buf[77], '0');
  ASSERT_EQ(buf[78], '0');
  vol1LabelWithCRC.backdoorSetLBPMethodString("  ");
  ASSERT_NO_THROW(vol1LabelWithCRC.getLBPMethod());
  ASSERT_EQ((int) LBPM::DoNotUse, (int) vol1LabelWithCRC.getLBPMethod());
  ASSERT_EQ(buf[77], ' ');
  ASSERT_EQ(buf[78], ' ');
  vol1LabelWithCRC.backdoorSetLBPMethodString("01");
  ASSERT_NO_THROW(vol1LabelWithCRC.getLBPMethod());
  ASSERT_EQ((int) LBPM::ReedSolomon, (int) vol1LabelWithCRC.getLBPMethod());
  ASSERT_EQ(buf[77], '0');
  ASSERT_EQ(buf[78], '1');
  vol1LabelWithCRC.backdoorSetLBPMethodString("00");
  ASSERT_NO_THROW(vol1LabelWithCRC.getLBPMethod());
  ASSERT_EQ((int) LBPM::DoNotUse, (int) vol1LabelWithCRC.getLBPMethod());
  vol1LabelWithCRC.backdoorSetLBPMethodString("02");
  ASSERT_NO_THROW(vol1LabelWithCRC.getLBPMethod());
  ASSERT_EQ((int) LBPM::CRC32C, (int) vol1LabelWithCRC.getLBPMethod());
  ASSERT_EQ(buf[77], '0');
  ASSERT_EQ(buf[78], '2');
  vol1LabelWithCRC.backdoorSetLBPMethodString("03");
  ASSERT_THROW(vol1LabelWithCRC.getLBPMethod(), cta::exception::Exception);
  vol1LabelWithCRC.backdoorSetLBPMethodString("XY");
  ASSERT_THROW(vol1LabelWithCRC.getLBPMethod(), cta::exception::Exception);
  ASSERT_EQ(0U, *((uint32_t*) &buf[80]));
}

TEST(castor_tape_AULFile, HDR1) {
  castor::tape::tapeFile::HDR1 hdr1Label;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U, sizeof(hdr1Label));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ hdr1Label.verify(); });
  hdr1Label.fill("AABBCC", "test", 12345);
  ASSERT_NO_THROW({ hdr1Label.verify(); });
  ASSERT_EQ("test  ", hdr1Label.getVSN());
  ASSERT_EQ("AABBCC           ", hdr1Label.getFileId());
  ASSERT_EQ("2345", hdr1Label.getfSeq());
}

TEST(castor_tape_AULFile, HDR1PRELABEL) {
  castor::tape::tapeFile::HDR1PRELABEL hdr1Prelabel;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U, sizeof(hdr1Prelabel));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ hdr1Prelabel.verify(); });
  hdr1Prelabel.fill("TEST");
  ASSERT_NO_THROW({ hdr1Prelabel.verify(); });
  ASSERT_EQ("TEST  ", hdr1Prelabel.getVSN());
  ASSERT_EQ("PRELABEL         ", hdr1Prelabel.getFileId());
  ASSERT_EQ("0001", hdr1Prelabel.getfSeq());
}

TEST(castor_tape_AULFile, EOF1) {
  castor::tape::tapeFile::EOF1 eof1Label;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U, sizeof(eof1Label));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ eof1Label.verify(); });
  eof1Label.fill("AABBCC", "test", 12345, 7654);
  ASSERT_NO_THROW({ eof1Label.verify(); });
  ASSERT_EQ("test  ", eof1Label.getVSN());
  ASSERT_EQ("AABBCC           ", eof1Label.getFileId());
  ASSERT_EQ("2345", eof1Label.getfSeq());
  ASSERT_EQ("007654", eof1Label.getBlockCount());
}

TEST(castor_tape_AULFile, HDR2) {
  castor::tape::tapeFile::HDR2 hdr2Label;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U, sizeof(hdr2Label));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ hdr2Label.verify(); });
  hdr2Label.fill(262144);
  ASSERT_NO_THROW({ hdr2Label.verify(); });
  ASSERT_EQ("00000", hdr2Label.getBlockLength());
  hdr2Label.fill(32760);
  ASSERT_EQ("32760", hdr2Label.getBlockLength());
}

TEST(castor_tape_AULFile, EOF2) {
  castor::tape::tapeFile::EOF2 eof2Label;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U, sizeof(eof2Label));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ eof2Label.verify(); });
  eof2Label.fill(262144);
  ASSERT_NO_THROW({ eof2Label.verify(); });
  ASSERT_EQ("00000", eof2Label.getBlockLength());
  eof2Label.fill(32760);
  ASSERT_EQ("32760", eof2Label.getBlockLength());
}

TEST(castor_tape_AULFile, UHL1) {
  castor::tape::tapeFile::UHL1 uhl1Label;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U, sizeof(uhl1Label));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ uhl1Label.verify(); });
  castor::tape::tapeserver::drive::deviceInfo deviceInfo;
  deviceInfo.vendor = "TEST";
  deviceInfo.serialNumber = "XXYYTEST";
  deviceInfo.product = "TEST DRIVE";
  uhl1Label.fill(12345, 262144, std::string("CERN"), std::string("TESTMOVER"), deviceInfo);
  ASSERT_NO_THROW({ uhl1Label.verify(); });
  ASSERT_EQ("0000262144", uhl1Label.getBlockSize());
  ASSERT_EQ("0000012345", uhl1Label.getfSeq());
}

TEST(castor_tape_AULFile, UTL1) {
  castor::tape::tapeFile::UTL1 utl1Label;
  /**
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
  ASSERT_EQ(80U, sizeof(utl1Label));
  /* test that verify throws exceptions */
  EXPECT_ANY_THROW({ utl1Label.verify(); });
  castor::tape::tapeserver::drive::deviceInfo deviceInfo;
  deviceInfo.vendor = "TEST";
  deviceInfo.serialNumber = "XXYYTEST";
  deviceInfo.product = "TEST DRIVE";
  utl1Label.fill(12345, 262144, std::string("CERN"), std::string("TESTMOVER"), deviceInfo);
  ASSERT_NO_THROW({ utl1Label.verify(); });
  ASSERT_EQ("0000262144", utl1Label.getBlockSize());
  ASSERT_EQ("0000012345", utl1Label.getfSeq());
}

TEST(castor_tape_AULFile, setDateTest) {
  const size_t lengthOfStr = 6;
  char str[lengthOfStr];
  castor::tape::tapeFile::setDate(str);
  const std::string buff(str);
  ASSERT_EQ(buff.length(), lengthOfStr);
  // 22088 is the date at 29/03/2022
  ASSERT_GT(stoi(buff), 22088);
}
}  // namespace unitTests
