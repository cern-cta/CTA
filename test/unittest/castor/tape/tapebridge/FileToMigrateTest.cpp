/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/FileToMigrateTest.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, fileToMigrate.or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, fileToMigrate.write to the Free Software
 * Foundation, fileToMigrate.Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/tapebridge/FileToMigrate.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdint.h>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class FileToMigrateTest: public CppUnit::TestFixture {
public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testConstructor() {
    const FileToMigrate fileToMigrate;

    CPPUNIT_ASSERT_EQUAL(    (uint64_t)0, fileToMigrate.fileTransactionId);
    CPPUNIT_ASSERT_EQUAL(std::string(""), fileToMigrate.nsHost);
    CPPUNIT_ASSERT_EQUAL(    (uint64_t)0, fileToMigrate.fileId);
    CPPUNIT_ASSERT_EQUAL(     (int32_t)0, fileToMigrate.fseq);
    CPPUNIT_ASSERT_EQUAL(     (int32_t)0, fileToMigrate.positionMethod);
    CPPUNIT_ASSERT_EQUAL(    (uint64_t)0, fileToMigrate.fileSize);
    CPPUNIT_ASSERT_EQUAL(std::string(""), fileToMigrate.lastKnownFilename);
    CPPUNIT_ASSERT_EQUAL(    (uint64_t)0, fileToMigrate.lastModificationTime);
    CPPUNIT_ASSERT_EQUAL(std::string(""), fileToMigrate.path);
    CPPUNIT_ASSERT_EQUAL(     (int32_t)0, fileToMigrate.umask);
  }

  CPPUNIT_TEST_SUITE(FileToMigrateTest);

  CPPUNIT_TEST(testConstructor);

  CPPUNIT_TEST_SUITE_END();
}; // class FileToMigrateTest

CPPUNIT_TEST_SUITE_REGISTRATION(FileToMigrateTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor
