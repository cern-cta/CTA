/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/PendingMigrationsStoreTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP 1

#include "castor/tape/tapebridge/FileWrittenNotificationList.hpp"
#include "castor/tape/tapebridge/PendingMigrationsStore.hpp"
#include "h/Cuuid.h"
#include "h/rtcpd_constants.h"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdlib.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class PendingMigrationsStoreTest: public CppUnit::TestFixture {

public:

  void setUp() {
    // Do nothing
  }

  void tearDown() {
    // Do nothing
  }

  void testFlushEveryNthByte() {
    const uint64_t maxBytesBeforeFlush = 8;
    const uint64_t maxFilesBeforeFlush = 20;
    castor::tape::tapebridge::PendingMigrationsStore store(maxBytesBeforeFlush,
      maxFilesBeforeFlush);
    const uint32_t maxFseq = 1024;

    // Tell the pending-migrations store all files are pending write-to-tape.
    for(uint32_t i = 1; i <= maxFseq; i++) {
      castor::tape::tapebridge::RequestToMigrateFile
        request;
      request.fileTransactionId = 10000 + i;
      request.tapeFSeq = i;
      request.fileSize = 1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.receivedRequestToMigrateFile",
        store.receivedRequestToMigrateFile(request));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "store.getNbPendingMigrations after add",
        i,
        store.getNbPendingMigrations());
    }
    store.noMoreFilesToMigrate();

    // Write files to tape, flushing every maxBytesBeforeFlush files
    for(uint32_t i=1; i<=maxFseq; i++) {
      castor::tape::tapebridge::FileWrittenNotification
        notification;
      notification.fileTransactionId = 10000 + i;
      notification.tapeFSeq = i;
      notification.fileSize = 1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.fileWrittenWithoutFlush",
        fileWrittenWithoutFlush(store, notification));

      // If data should be flushed to tape
      if(0 == i % maxBytesBeforeFlush) {
        castor::tape::tapebridge::FileWrittenNotificationList
          notfications;

        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
          "store.dataFlushedToTape",
          notfications = dataFlushedToTape(store, (i)));

        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "size of result of store.dataFlushedToTape",
          (castor::tape::tapebridge::FileWrittenNotificationList::
          size_type)maxBytesBeforeFlush,
          notfications.size());

        // Check the tape-file sequence-numbers of the flushed files
        {
          int32_t k = i-maxBytesBeforeFlush+1;
          for(castor::tape::tapebridge::FileWrittenNotificationList::
            const_iterator itor = notfications.begin();
            itor != notfications.end(); itor++) {

            CPPUNIT_ASSERT_EQUAL_MESSAGE(
              "fseq of result of store.dataFlushedToTape",
              k,
              itor->tapeFSeq);

            k++;
          }
        }

        // Check the number of remaning files awating a flush to tape
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "store.getNbPendingMigrations after dataFlushedToTape",
          maxFseq - i,
          store.getNbPendingMigrations());
      } // If data should be flushed to tape
    } // Write files to tape, flushing every maxFilesBeforeFlush files
  } // testFlushEveryNthFile()

  void testFlushEveryNthFile() {
    const uint64_t maxBytesBeforeFlush = 8589934592UL;
    const uint64_t maxFilesBeforeFlush = 4;
    castor::tape::tapebridge::PendingMigrationsStore store(maxBytesBeforeFlush,
      maxFilesBeforeFlush);
    const uint32_t maxFseq = 1024;

    // Tell the pending-migrations store all files are pending write-to-tape.
    for(uint32_t i = 1; i <= maxFseq; i++) {
      castor::tape::tapebridge::RequestToMigrateFile
        request;
      request.fileTransactionId = 10000 + i;
      request.tapeFSeq = i;
      request.fileSize = 1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.receivedRequestToMigrateFile",
        store.receivedRequestToMigrateFile(request));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "store.getNbPendingMigrations after add",
        i,
        store.getNbPendingMigrations());
    }
    store.noMoreFilesToMigrate();

    // Write files to tape, flushing every maxFilesBeforeFlush files
    for(uint32_t i=1; i<=maxFseq; i++) {
      castor::tape::tapebridge::FileWrittenNotification
        notification;
      notification.fileTransactionId = 10000 + i;
      notification.tapeFSeq = i;
      notification.fileSize = 1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.fileWrittenWithoutFlush",
        fileWrittenWithoutFlush(store, notification));

      // If data should be flushed to tape
      if(0 == i % maxFilesBeforeFlush) {
        castor::tape::tapebridge::FileWrittenNotificationList
          notfications;

        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
          "store.dataFlushedToTape",
          notfications = dataFlushedToTape(store, (i)));

        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "size of result of store.dataFlushedToTape",
          (castor::tape::tapebridge::FileWrittenNotificationList::
          size_type)maxFilesBeforeFlush,
          notfications.size());

        // Check the tape-file sequence-numbers of the flushed files
        {
          int32_t k = i-maxFilesBeforeFlush+1;
          for(castor::tape::tapebridge::FileWrittenNotificationList::
            const_iterator itor = notfications.begin();
            itor != notfications.end(); itor++) {

            CPPUNIT_ASSERT_EQUAL_MESSAGE(
              "fseq of result of store.dataFlushedToTape",
              k,
              itor->tapeFSeq);

            k++;
          }
        }

        // Check the number of remaning files awating a flush to tape
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "store.getNbPendingMigrations after dataFlushedToTape",
          maxFseq - i,
          store.getNbPendingMigrations());
      } // If data should be flushed to tape
    } // Write files to tape, flushing every maxFilesBeforeFlush files
  } // testFlushEveryNthFile()

  void testInvalidFlushFseqMaxBytes() {
    const uint64_t maxBytesBeforeFlush = 67;
    const uint64_t maxFilesBeforeFlush = maxBytesBeforeFlush + 100;
    castor::tape::tapebridge::PendingMigrationsStore store(maxBytesBeforeFlush,
      maxFilesBeforeFlush);
    const uint32_t maxFseq = 1024;

    // Tell the pending-migrations store all files are pending write-to-tape.
    for(uint32_t i=1; i<=maxFseq; i++) {
      castor::tape::tapebridge::RequestToMigrateFile
        request;
      request.fileTransactionId = 10000 + i;
      request.tapeFSeq = i;
      request.fileSize = 1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.receivedRequestToMigrateFile",
        store.receivedRequestToMigrateFile(request));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "store.getNbPendingMigrations after add",
        i,
        store.getNbPendingMigrations());
    }
    store.noMoreFilesToMigrate();

    // Write all files up to maxBytesBeforeFlush;
    for(uint32_t i=1; i <= maxBytesBeforeFlush; i++) {
      castor::tape::tapebridge::FileWrittenNotification
      notification;
      notification.fileTransactionId = 10000 + i;
      notification.tapeFSeq = i;
      notification.fileSize = 1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.fileWrittenWithoutFlush",
        fileWrittenWithoutFlush(store, notification));
    }

    // Illegally flush to tape before maxBytesBeforeFlush
    {
      const uint32_t illegalFlushFseq = 50;
      castor::tape::tapebridge::FileWrittenNotificationList
        notfications;

      CPPUNIT_ASSERT_THROW_MESSAGE(
        "store.dataFlushedToTape",
        notfications = store.dataFlushedToTape(illegalFlushFseq),
          castor::exception::Exception);
    }
  }

  void testInvalidFlushFseqMaxFiles() {
    const uint64_t maxFilesBeforeFlush = 23;
    const uint64_t maxBytesBeforeFlush = maxFilesBeforeFlush + 100;
    castor::tape::tapebridge::PendingMigrationsStore store(maxBytesBeforeFlush,
      maxFilesBeforeFlush);
    const uint32_t maxFseq = 1024;

    // Tell the pending-migrations store all files are pending write-to-tape.
    for(uint32_t i=1; i<=maxFseq; i++) {
      castor::tape::tapebridge::RequestToMigrateFile
        request;
      request.fileTransactionId = 10000 + i;
      request.tapeFSeq = i;
      request.fileSize = 1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.receivedRequestToMigrateFile",
        store.receivedRequestToMigrateFile(request));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "store.getNbPendingMigrations after add",
        i,
        store.getNbPendingMigrations());
    }

    // Write all files up to maxFilesBeforeFlush;
    for(uint32_t i=1; i <= maxFilesBeforeFlush; i++) {
      castor::tape::tapebridge::FileWrittenNotification
        notification;
      notification.fileTransactionId = 10000 + i;
      notification.tapeFSeq = i;
      notification.fileSize = 1;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "store.fileWrittenWithoutFlush",
        fileWrittenWithoutFlush(store, notification));
    }

    // Illegally flush to tape before maxFilesBeforeFlush
    {
      const uint32_t illegalFlushFseq = 20;
      castor::tape::tapebridge::FileWrittenNotificationList
        notfications;

      CPPUNIT_ASSERT_THROW_MESSAGE(
        "store.dataFlushedToTape",
        notfications = store.dataFlushedToTape(illegalFlushFseq),
          castor::exception::Exception);
    }
  }

  CPPUNIT_TEST_SUITE(PendingMigrationsStoreTest);
  CPPUNIT_TEST(testFlushEveryNthByte);
  CPPUNIT_TEST(testFlushEveryNthFile);
  CPPUNIT_TEST(testInvalidFlushFseqMaxBytes);
  CPPUNIT_TEST(testInvalidFlushFseqMaxFiles);
  CPPUNIT_TEST_SUITE_END();

private:

  /**
   * Converts any thrown exception inheriting from castor::exception::Exception
   * into a std::exception.
   */
  castor::tape::tapebridge::FileWrittenNotificationList
    dataFlushedToTape(
    castor::tape::tapebridge::PendingMigrationsStore &store,
    const uint32_t tapeFseqOfFlush) throw(std::exception) {
    try {
      return store.dataFlushedToTape(tapeFseqOfFlush);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());
      throw(te);
    }
  }

  /**
   * Converts any thrown exception inheriting from castor::exception::Exception
   * into a std::exception.
   */
  void fileWrittenWithoutFlush(
    castor::tape::tapebridge::PendingMigrationsStore &store,
    const castor::tape::tapebridge::FileWrittenNotification
      &notification) throw(std::exception) {
    try {
      store.fileWrittenWithoutFlush(notification);
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());
      throw(te);
    }
  }
}; // class PendingMigrationsStoreTest

CPPUNIT_TEST_SUITE_REGISTRATION(PendingMigrationsStoreTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_PENDINGMIGRATIONSTEST_HPP
