/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/FileToRecallTest.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, fileToRecall.or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, fileToRecall.write to the Free Software
 * Foundation, fileToRecall.Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/tapebridge/FileToRecall.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdint.h>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class FileToRecallTest: public CppUnit::TestFixture {
public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testConstructor() {
    const FileToRecall fileToRecall;

    CPPUNIT_ASSERT_EQUAL(     (uint64_t)0, fileToRecall.fileTransactionId);
    CPPUNIT_ASSERT_EQUAL( std::string(""), fileToRecall.nsHost);
    CPPUNIT_ASSERT_EQUAL(     (uint64_t)0, fileToRecall.fileId);
    CPPUNIT_ASSERT_EQUAL(      (int32_t)0, fileToRecall.fseq);
    CPPUNIT_ASSERT_EQUAL(      (int32_t)0, fileToRecall.positionMethod);
    CPPUNIT_ASSERT_EQUAL( std::string(""), fileToRecall.path);
    CPPUNIT_ASSERT_EQUAL((unsigned char)0, fileToRecall.blockId0);
    CPPUNIT_ASSERT_EQUAL((unsigned char)0, fileToRecall.blockId1);
    CPPUNIT_ASSERT_EQUAL((unsigned char)0, fileToRecall.blockId2);
    CPPUNIT_ASSERT_EQUAL((unsigned char)0, fileToRecall.blockId3);
    CPPUNIT_ASSERT_EQUAL(      (int32_t)0, fileToRecall.umask);
  }

  CPPUNIT_TEST_SUITE(FileToRecallTest);

  CPPUNIT_TEST(testConstructor);

  CPPUNIT_TEST_SUITE_END();
}; // class FileToRecallTest

CPPUNIT_TEST_SUITE_REGISTRATION(FileToRecallTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor
