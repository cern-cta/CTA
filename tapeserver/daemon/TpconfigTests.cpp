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

#include <gtest/gtest.h>

#include "tapeserver/daemon/Tpconfig.hpp"
#include "tests/TempFile.hpp"
#include "common/exception/Errnum.hpp"

namespace unitTests {

TEST(cta_Daemon, Tpconfig_base) {
  TempFile tf;
  // Test with empty file
  tf.stringFill("");
  cta::tape::daemon::Tpconfig tpc = cta::tape::daemon::Tpconfig::parseFile(tf.path());
  ASSERT_EQ(0, tpc.size());
  // Test with file full of comments (but no valid line)
  tf.stringFill("# some comment\n"
                "\t   \t # Some non-empty line (spaces)\n"
                "\t\t\t                   \n");
  tpc = cta::tape::daemon::Tpconfig::parseFile(tf.path());
  ASSERT_EQ(0, tpc.size());

  // Test with non-existing file
  ASSERT_THROW(cta::tape::daemon::Tpconfig::parseFile("/no/such/file"), cta::exception::Errnum);
  // Check we get the expected Errno.
  try {
    cta::tape::daemon::Tpconfig::parseFile("/no/such/file");
    ASSERT_TRUE(false);  // We should never get here.
  }
  catch (cta::exception::Errnum& ex) {
    ASSERT_NE(std::string::npos, ex.getMessageValue().find("Errno=2:"));
  }

  // Test with a line too short
  tf.stringFill("TapeDrive");
  ASSERT_THROW(cta::tape::daemon::Tpconfig::parseFile(tf.path()), cta::exception::Exception);
  try {
    cta::tape::daemon::Tpconfig::parseFile(tf.path());
    ASSERT_TRUE(false);  // We should never get here.
  }
  catch (cta::exception::Exception& ex) {
    ASSERT_NE(std::string::npos, ex.getMessageValue().find("missing"));
  }

  // Test with line too long
  tf.stringFill("TapeDrive lib /dev/tape libSlot ExtraArgument");
  ASSERT_THROW(cta::tape::daemon::Tpconfig::parseFile(tf.path()), cta::exception::Exception);
  try {
    cta::tape::daemon::Tpconfig::parseFile(tf.path());
    ASSERT_TRUE(false);  // We should never get here.
  }
  catch (cta::exception::Exception& ex) {
    ASSERT_NE(std::string::npos, ex.getMessageValue().find("extra parameter"));
  }

  // Test with several entries (valid file with various extra blanks)
  tf.stringFill("         drive0 lib0 \t\t\t /dev/tape0       smc0\n"
                "drive1 lib0 /dev/tape1 smc1         \n"
                "drive2 lib0 /dev/tape2 smc2");
  tpc = cta::tape::daemon::Tpconfig::parseFile(tf.path());
  ASSERT_EQ(3, tpc.size());
  int i = 0;
  for (auto& t : tpc) {
    ASSERT_EQ("drive", t.second.value().unitName.substr(0, 5));
    ASSERT_EQ("lib0", t.second.value().logicalLibrary);
    ASSERT_EQ("/dev/tape", t.second.value().devFilename.substr(0, 9));
    ASSERT_EQ("smc", t.second.value().rawLibrarySlot.substr(0, 3));
    ASSERT_EQ('0' + i, t.second.value().unitName.back());
    ASSERT_EQ('0' + i, t.second.value().devFilename.back());
    ASSERT_EQ('0' + i, t.second.value().rawLibrarySlot.back());
    i++;
  }
}

TEST(cta_Daemon, Tpconfig_duplicates) {
  TempFile tf;
  // Test duplicate unit name
  tf.stringFill("drive0 lib0 /dev/tape0 smc0\n"
                "drive1 lib0 /dev/tape1 smc1\n"
                "drive0 lib0 /dev/tape2 smc2");
  ASSERT_THROW(cta::tape::daemon::Tpconfig::parseFile(tf.path()), cta::exception::Exception);
  // Test duplicate path
  tf.stringFill("drive0 lib0 /dev/tape0 smc0\n"
                "drive1 lib0 /dev/tape1 smc1\n"
                "drive2 lib0 /dev/tape0 smc2");
  ASSERT_THROW(cta::tape::daemon::Tpconfig::parseFile(tf.path()), cta::exception::Exception);
  // Test duplicate slot
  tf.stringFill("drive0 lib0 /dev/tape0 smc0\n"
                "drive1 lib0 /dev/tape1 smc1\n"
                "drive2 lib0 /dev/tape2 smc0");
  ASSERT_THROW(cta::tape::daemon::Tpconfig::parseFile(tf.path()), cta::exception::Exception);
  // No duplication.
  tf.stringFill("drive0 lib0 /dev/tape0 smc0\n"
                "drive1 lib0 /dev/tape1 smc1\n"
                "drive2 lib0 /dev/tape2 smc2");
  cta::tape::daemon::Tpconfig::parseFile(tf.path());
}

}  // namespace unitTests
