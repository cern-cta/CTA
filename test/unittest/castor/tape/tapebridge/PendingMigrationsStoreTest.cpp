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

#include "castor/tape/tapebridge/FileWrittenNotificationList.hpp"
#include "castor/tape/tapebridge/PendingMigrationsStore.hpp"
#include "h/Cuuid.h"
#include "h/rtcpd_constants.h"
#include "h/tapebridge_constants.h"
#include "test/unittest/castor/tape/tapebridge/TestingTapeFlushConfigParams.hpp"
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
    TestingTapeFlushConfigParams tapeFlushConfigParams;
    tapeFlushConfigParams.setTapeFlushMode(
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxBytesBeforeFlush(
      8,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxFilesBeforeFlush(
      20,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    castor::tape::tapebridge::PendingMigrationsStore store(
      tapeFlushConfigParams);
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
      if(0 == i % tapeFlushConfigParams.getMaxBytesBeforeFlush().getValue()) {
        castor::tape::tapebridge::FileWrittenNotificationList
          notfications;

        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
          "store.dataFlushedToTape",
          notfications = dataFlushedToTape(store, (i)));

        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "size of result of store.dataFlushedToTape",
          (castor::tape::tapebridge::FileWrittenNotificationList::
          size_type)tapeFlushConfigParams.getMaxBytesBeforeFlush().getValue(),
          notfications.size());

        // Check the tape-file sequence-numbers of the flushed files
        {
          int32_t k = i -
            tapeFlushConfigParams.getMaxBytesBeforeFlush().getValue() + 1;
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
    TestingTapeFlushConfigParams tapeFlushConfigParams;
    tapeFlushConfigParams.setTapeFlushMode(
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxBytesBeforeFlush(
      8589934592UL,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxFilesBeforeFlush(
      4,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    castor::tape::tapebridge::PendingMigrationsStore store(
      tapeFlushConfigParams);
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
      if(0 == i % tapeFlushConfigParams.getMaxFilesBeforeFlush().getValue()) {
        castor::tape::tapebridge::FileWrittenNotificationList
          notfications;

        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
          "store.dataFlushedToTape",
          notfications = dataFlushedToTape(store, (i)));

        CPPUNIT_ASSERT_EQUAL_MESSAGE(
          "size of result of store.dataFlushedToTape",
          (castor::tape::tapebridge::FileWrittenNotificationList::
          size_type)tapeFlushConfigParams.getMaxFilesBeforeFlush().getValue(),
          notfications.size());

        // Check the tape-file sequence-numbers of the flushed files
        {
          int32_t k = i -
            tapeFlushConfigParams.getMaxFilesBeforeFlush().getValue() + 1;
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
    TestingTapeFlushConfigParams tapeFlushConfigParams;
    tapeFlushConfigParams.setTapeFlushMode(
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxBytesBeforeFlush(
      67,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxFilesBeforeFlush(
      tapeFlushConfigParams.getMaxBytesBeforeFlush().getValue() + 100,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    castor::tape::tapebridge::PendingMigrationsStore store(
      tapeFlushConfigParams);
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
    for(uint32_t i = 1;
      i <= tapeFlushConfigParams.getMaxBytesBeforeFlush().getValue(); i++) {
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
    TestingTapeFlushConfigParams tapeFlushConfigParams;
    tapeFlushConfigParams.setTapeFlushMode(
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxFilesBeforeFlush(
      23,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxBytesBeforeFlush(
      tapeFlushConfigParams.getMaxFilesBeforeFlush().getValue() + 100,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    castor::tape::tapebridge::PendingMigrationsStore store(
      tapeFlushConfigParams);
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
    for(uint32_t i = 1;
      i <= tapeFlushConfigParams.getMaxFilesBeforeFlush().getValue(); i++) {
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

  void testMigrateToFSeqZero() {
    TestingTapeFlushConfigParams tapeFlushConfigParams;
    tapeFlushConfigParams.setTapeFlushMode(
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxBytesBeforeFlush(
      8,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxFilesBeforeFlush(
      20,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    castor::tape::tapebridge::PendingMigrationsStore store(
      tapeFlushConfigParams);

    castor::tape::tapebridge::RequestToMigrateFile request;
    request.fileTransactionId = 1000;
    request.nsHost = "Name server host";
    request.nsFileId = 2000;
    request.tapeFSeq = 0;
    request.fileSize = 4000;

    try {
      store.receivedRequestToMigrateFile(request);
      CPPUNIT_ASSERT_MESSAGE(
        "receivedRequestToMigrateFile failed to throw an exception",
        false);
    } catch(SessionException &se) {
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking code of session exception",
        ETSESSIONERROR,
        se.code());

      const SessionError &error = se.getSessionError();

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking errorScope of session error",
        SessionError::FILE_SCOPE,
        error.getErrorScope());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking errorCode of session error",
        ETINVALIDTFSEQ,
        error.getErrorCode());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking fileTransactionId of session error",
        request.fileTransactionId,
        error.getFileTransactionId());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking nsHost of session error",
        request.nsHost,
        error.getNsHost());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking nsFileId of session error",
        request.nsFileId,
        error.getNsFileId());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking tapeFSeq of session error",
        request.tapeFSeq,
        error.getTapeFSeq());
    } catch(...) {
      CPPUNIT_ASSERT_MESSAGE(
        "receivedRequestToMigrateFile failed to throw SessionException",
        false);
    }
  }

  void testMigrateZeroLengthFile() {
    TestingTapeFlushConfigParams tapeFlushConfigParams;
    tapeFlushConfigParams.setTapeFlushMode(
      TAPEBRIDGE_ONE_FLUSH_PER_N_FILES,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxBytesBeforeFlush(
      8,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    tapeFlushConfigParams.setMaxFilesBeforeFlush(
      20,
      ConfigParamSource::COMPILE_TIME_DEFAULT);
    castor::tape::tapebridge::PendingMigrationsStore store(
      tapeFlushConfigParams);

    castor::tape::tapebridge::RequestToMigrateFile request;
    request.fileTransactionId = 1000;
    request.nsHost = "Name server host";
    request.nsFileId = 2000;
    request.tapeFSeq = 3000;
    request.fileSize = 0;

    try {
      store.receivedRequestToMigrateFile(request);
      CPPUNIT_ASSERT_MESSAGE(
        "receivedRequestToMigrateFile failed to throw an exception",
        false);
    } catch(SessionException &se) {
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking code of session exception",
        ETSESSIONERROR,
        se.code());

      const SessionError &error = se.getSessionError();

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking errorScope of session error",
        SessionError::FILE_SCOPE,
        error.getErrorScope());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking errorCode of session error",
        ETINVALIDTFSIZE,
        error.getErrorCode());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking fileTransactionId of session error",
        request.fileTransactionId,
        error.getFileTransactionId());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking nsHost of session error",
        request.nsHost,
        error.getNsHost());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking nsFileId of session error",
        request.nsFileId,
        error.getNsFileId());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking tapeFSeq of session error",
        request.tapeFSeq,
        error.getTapeFSeq());
    } catch(...) {
      CPPUNIT_ASSERT_MESSAGE(
        "receivedRequestToMigrateFile failed to throw SessionException",
        false);
    }
  }

  CPPUNIT_TEST_SUITE(PendingMigrationsStoreTest);
  CPPUNIT_TEST(testFlushEveryNthByte);
  CPPUNIT_TEST(testFlushEveryNthFile);
  CPPUNIT_TEST(testInvalidFlushFseqMaxBytes);
  CPPUNIT_TEST(testInvalidFlushFseqMaxFiles);
  CPPUNIT_TEST(testMigrateToFSeqZero);
  CPPUNIT_TEST(testMigrateZeroLengthFile);
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
