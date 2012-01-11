/******************************************************************************
 *                castor/tape/tapebridge/PendingMigrationsStore.cpp
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

#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapebridge/PendingMigrationsStore.hpp"
#include "castor/tape/utils/utils.hpp"

#include <sstream>


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::PendingMigrationsStore::PendingMigrationsStore(
  const uint64_t maxBytesBeforeFlush, const uint64_t maxFilesBeforeFlush):
  m_maxBytesBeforeFlush(maxBytesBeforeFlush),
  m_maxFilesBeforeFlush(maxFilesBeforeFlush) {
  clear();
}


//-----------------------------------------------------------------------------
// Copy constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::PendingMigrationsStore::PendingMigrationsStore(
  PendingMigrationsStore &other):
  m_maxBytesBeforeFlush(other.m_maxBytesBeforeFlush),
  m_maxFilesBeforeFlush(other.m_maxFilesBeforeFlush) {
  // Should never be called, therefore do nothing
}


//-----------------------------------------------------------------------------
// Assignment operator
//-----------------------------------------------------------------------------
castor::tape::tapebridge::PendingMigrationsStore
  &castor::tape::tapebridge::PendingMigrationsStore::operator=(
  const PendingMigrationsStore&) {
  // Should never be called, therefore do nothing
  return *this;
}


//-----------------------------------------------------------------------------
// receivedRequestToMigrateFile
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::PendingMigrationsStore::
  receivedRequestToMigrateFile(const RequestToMigrateFile &request)
  throw(castor::exception::Exception) {

  const char *const task = "add pending file-migration to pending"
    " file-migration store";

  // Throw an exception if the tape-file sequence-number of the request is
  // invalid
  if(0 == request.tapeFSeq) {
    TAPE_THROW_CODE(EBADMSG,
    ": Failed to " << task <<
    ": Request to migrate file to tape contains an invalid tape-file"
    " sequence-number"
    ": value=" << request.tapeFSeq);
  }

  // Throw an exception if the size of the file to be migrated is invalid
  if(0 == request.fileSize) {
    TAPE_THROW_CODE(EBADMSG,
    ": Failed to " << task <<
    ": Invalid file-size"
    ": value=0");
  }

  // Throw an exception if the store already contains a file with the same
  // tape-file sequence-number
  MigrationMap::iterator itor = m_migrations.find(request.tapeFSeq);
  if(itor != m_migrations.end()) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to " << task <<
      ": Store already contains a file with the same tape-file sequence-number"
      ": value=" << request.tapeFSeq);
  }

  // Throw an exception if the store is not empty and the tape-file
  // sequence-number of the file to be migrated is not 1 plus the tape-file
  // sequence-number of the previously added file to be migrated
  {
    const int32_t expectedTapeFSeq = m_tapeFSeqOfLastFileAdded + 1;
    if(!m_migrations.empty() && expectedTapeFSeq != request.tapeFSeq) {
      TAPE_THROW_EX(castor::exception::InvalidArgument,
        ": Failed to add pending file-migration to pending file-migration store"
        ": Tape-file sequence-number of file is not 1 plus the value of the"
        " previously added file"
        ": expected=" << expectedTapeFSeq <<
        ": actual=" << request.tapeFSeq);
    }
  }

  // Add the file-migration request
  {
    const Migration migration(Migration::SENT_TO_RTCPD, request);
    m_migrations[request.tapeFSeq] = migration;
  }

  // Update the tape-file sequence-number of the last file added
  m_tapeFSeqOfLastFileAdded = request.tapeFSeq;
}


//-----------------------------------------------------------------------------
// noMoreFilesToMigrate
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::PendingMigrationsStore::noMoreFilesToMigrate()
  throw(castor::exception::Exception) {
  const bool endOfSessionFileIsKnown = 0 != m_tapeFSeqOfEndOfSessionFile;

  if(endOfSessionFileIsKnown) {
    TAPE_THROW_CODE(EINVAL,
      ": Failed to register no more files to migrate"
      ": Already received no-more-files notification"
      ": tapeFSeqOfLastFileAdded=" << m_tapeFSeqOfLastFileAdded);
  }

  m_tapeFSeqOfEndOfSessionFile = m_tapeFSeqOfLastFileAdded;
}


//-----------------------------------------------------------------------------
// fileWrittenWithoutFlush
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::PendingMigrationsStore::fileWrittenWithoutFlush(
  const FileWrittenNotification &notification)
  throw(castor::exception::Exception) {

  const char *const task = "mark file-migration as written without flush";

  // Throw an exception if the tape-file sequence-number of the file
  // migrated message is invalid
  if(0 >= notification.tapeFSeq) {
    TAPE_THROW_CODE(EBADMSG,
    ": Failed to " << task <<
    ": File migrated message contains an invalid fseq"
    ": fseq=" << notification.tapeFSeq);
  }

  // Throw an exception if the file size of the file migrated message is
  // invalid
  if(0 == notification.fileSize) {
    TAPE_THROW_CODE(EBADMSG,
    ": Failed to " << task <<
    ": File migrated message contains the invalid file-size of 0" <<
    ": fseq=" << notification.tapeFSeq);
  }

  // Try to find the pending file-migration whose state is to be updated
  MigrationMap::iterator itor = m_migrations.find(
    notification.tapeFSeq);

  // Throw an exception if the pending file-migration could not be found
  if(m_migrations.end() == itor) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to " << task <<
      ": Pending file-migration not found"
      ": fseq=" << notification.tapeFSeq);
  }

  // Throw an exception if at least one file has already been marked as being
  // written but not yet flushed to tape and the tape-file sequence-number of
  // the current file to be marked is not 1 plus that of the previously marked
  // file
  if(m_tapeFSeqOfLastFileWrittenWithoutFlush != 0 &&
    (m_tapeFSeqOfLastFileWrittenWithoutFlush + 1 !=
      notification.tapeFSeq)) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to " << task <<
      ": Tape-file sequence-number of file is not 1 plus the value of the"
      " previously marked file"
      ": expected=" << (m_tapeFSeqOfLastFileWrittenWithoutFlush + 1) <<
      ": actual=" << notification.tapeFSeq);
  }

  // Throw an exception if any of the fields common to both the file to migrate
  // request and the file-migrated notification do not match exactly
  try {
    checkForMismatches(itor->second.getRequest(), notification);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to " << task <<
      ": " << ex.getMessage().str());
  }

  // Copy into the pending file-migration the file-migrated notification
  // message that will be sent to the client when the rtcpd daemon has notified
  // the tapebridged daemon that the file has been flushed to tape
  itor->second.setFileWrittenNotification(notification);

  // Update the state of the pending file-migration
  itor->second.setStatus(Migration::WRITTEN_WITHOUT_FLUSH);

  // Update the tape-file sequence-number of the last file marked as written
  // without a flush
  m_tapeFSeqOfLastFileWrittenWithoutFlush = notification.tapeFSeq;

  // Update the number of bytes and files written to tape without a flush
  m_nbBytesWrittenWithoutFlush += notification.fileSize;
  m_nbFilesWrittenWithoutFlush++;

  // Determine whether or not the current file is the one that matches or
  // immediately exceeds the maximum number of bytes to be written to tape
  // before a flush
  if(m_maxBytesBeforeFlush <= m_nbBytesWrittenWithoutFlush) {
    if(0 != m_tapeFSeqOfMaxBytesFile) {
      TAPE_THROW_EX(castor::exception::InvalidArgument,
        ": Failed to " << task <<
        ": Already matched or exceed maximum number of bytes before flush"
        ": maxBytesBeforeFlush=" << m_maxBytesBeforeFlush <<
        " nbBytesWrittenWithoutFlush=" << m_nbBytesWrittenWithoutFlush <<
        " tapeFSeqOfMaxBytesFile=" << m_tapeFSeqOfMaxBytesFile <<
        " illegalTapeFSeqOfMaxBytesFile=" << notification.tapeFSeq);
    }

    m_tapeFSeqOfMaxBytesFile = notification.tapeFSeq;
  }

  // Determine whether or not the current file is the one that matches the
  // the maximum number of files to be written to tape before a flush
  if(m_maxFilesBeforeFlush <= m_nbFilesWrittenWithoutFlush) {
    if(0 != m_tapeFSeqOfMaxFilesFile) {
      TAPE_THROW_EX(castor::exception::InvalidArgument,
        ": Failed to " << task <<
        ": Already matched maximum number of files before flush"
        ": maxFilesBeforeFlush=" << m_maxFilesBeforeFlush <<
        " nbFilesWrittenWithoutFlush=" << m_nbFilesWrittenWithoutFlush <<
        " tapeFSeqOfMaxFilesFile=" << m_tapeFSeqOfMaxFilesFile <<
        " illegalTapeFSeqOfMaxFilesFile=" << notification.tapeFSeq);
    }

    m_tapeFSeqOfMaxFilesFile = notification.tapeFSeq;
  }
}


//-----------------------------------------------------------------------------
// checkForMismatches
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::PendingMigrationsStore::checkForMismatches(
  const RequestToMigrateFile    &request,
  const FileWrittenNotification &notification)
  const throw(castor::exception::Exception) {

  bool foundMismatch = false;
  std::ostringstream oss;

  if(request.fileTransactionId != notification.fileTransactionId) {
    oss <<
      ": fileTransactionId mismatch"
      " request.fileTransactionId="  << request.fileTransactionId <<
      " notification.fileTransactionId=" << notification.fileTransactionId;
    foundMismatch = true;
  }
  if(request.nsHost != notification.nsHost) {
    oss <<
      ": nshost mismatch"
      " request.nsHost=" << request.nsHost <<
      " notification.nsHost=" << notification.nsHost;
    foundMismatch = true;
  }
  if(request.nsFileId != notification.nsFileId) {
    oss <<
      ": fileid mismatch"
      " request.nsFileId="  << request.nsFileId <<
      " notification.nsFileId=" << notification.nsFileId;
    foundMismatch = true;
  }
  if(request.tapeFSeq != notification.tapeFSeq) {
    oss <<
      ": fseq mismatch"
      " request.tapeFSeq=" << request.tapeFSeq <<
      " notification.tapeFSeq=" << notification.tapeFSeq;
    foundMismatch = true;
  }
  if(request.positionCommandCode != notification.positionCommandCode) {
    oss <<
      ": positionCommandCode mismatch"
      " request.positionCommandCode=" << request.positionCommandCode <<
      " notification.positionCommandCode=" <<
      notification.positionCommandCode;
    foundMismatch = true;
  }
  if(request.fileSize != notification.fileSize) {
    oss <<
      ": fileSize mismatch"
      " request.fileSize=" << request.fileSize <<
      " notification.fileSize=" << notification.fileSize;
    foundMismatch = true;
  }

  if(foundMismatch) {
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      "Mismatch(es) found"
      ": request.nsFileId="  << request.nsFileId <<
      " notification.nsFileId=" << notification.nsFileId <<
      oss.str();
    throw ex;
  }
}


//-----------------------------------------------------------------------------
// dataFlushedToTape
//-----------------------------------------------------------------------------
castor::tape::tapebridge::FileWrittenNotificationList
  castor::tape::tapebridge::PendingMigrationsStore::dataFlushedToTape(
  const int32_t tapeFSeqOfFlush) throw(castor::exception::Exception) {
  const char *const methodTask = "accept flush to tape";
  FileWrittenNotificationList outputList;

  // Check method arguments
  if(0 == tapeFSeqOfFlush) {
    TAPE_THROW_CODE(EINVAL,
      ": Failed to " << methodTask <<
      ": tapeFSeqOfFlush contains the invalid value of 0");
  }

  // Determine whether or not the tape-file sequence-number of the flush to
  // tape is valid taking into account the fact that
  // m_tapeFSeqOfEndOfSessionFile can only be used when neither
  // m_tapeFSeqOfMaxBytesFile and m_tapeFSeqOfMaxFilesFile are known
  bool tapeFSeqOfFlushIsValid = false;
  if(0 == m_tapeFSeqOfMaxBytesFile && 0 == m_tapeFSeqOfMaxFilesFile &&
    tapeFSeqOfFlush == m_tapeFSeqOfEndOfSessionFile) {
    tapeFSeqOfFlushIsValid = true;
  } else if(tapeFSeqOfFlush == m_tapeFSeqOfMaxBytesFile ||
    tapeFSeqOfFlush == m_tapeFSeqOfMaxFilesFile) {
    tapeFSeqOfFlushIsValid = true;
  }

  // Throw an exception if the flush has occured on an invalid tape-file
  // sequence-number
  if(!tapeFSeqOfFlushIsValid) {
    TAPE_THROW_CODE(EINVAL,
      ": Failed to " << methodTask <<
      ": Invalid tapeFSeqOfFlush"
      ": tapeFSeqOfMaxFilesFile=" << m_tapeFSeqOfMaxFilesFile <<
      " tapeFSeqOfMaxBytesFile=" << m_tapeFSeqOfMaxBytesFile <<
      " tapeFSeqOfEndOfSessionFile=" << m_tapeFSeqOfEndOfSessionFile <<
      " tapeFSeqOfFlush=" << tapeFSeqOfFlush);
  }

  // Reset the appropriate member-variables used to determine the tape-file
  // sequence-number of the next flush to tape
  m_nbBytesWrittenWithoutFlush = 0;
  m_nbFilesWrittenWithoutFlush = 0;
  m_tapeFSeqOfMaxBytesFile     = 0;
  m_tapeFSeqOfMaxFilesFile     = 0;

  // For each pending file-migration in the store in ascending order of
  // tape-file sequence-number
  for(MigrationMap::iterator itor = m_migrations.begin();
    itor != m_migrations.end();) {

    const int32_t   tapeFSeq = itor->first;
    const Migration &migration = itor->second;

    // Break out of the loop if we have reached a pending file-migration with
    // a tape-file sequence-number greater than that of the flush to tape
    if(tapeFSeqOfFlush < tapeFSeq) {
      break;
    }

    // Throw an exception if the pending file-migration has not been marked as
    // being written without a flush
    if(migration.getStatus() != Migration::WRITTEN_WITHOUT_FLUSH) {
      TAPE_THROW_CODE(ECANCELED,
        ": Failed to " << methodTask <<
        ": tapeFSeqOfFlush=" << tapeFSeqOfFlush <<
        ": Found pending file-migration not marked as being written without"
        " flush"
        ": badFSeq=" << tapeFSeq);
    }

    outputList.push_back(migration.getFileWrittenNotification());

    // Erase the pending file-migration from the store and move onto the next
    m_migrations.erase(itor++);
  }

  return outputList;
}


//-----------------------------------------------------------------------------
// clear
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::PendingMigrationsStore::clear() {
  m_nbBytesWrittenWithoutFlush            = 0;
  m_nbFilesWrittenWithoutFlush            = 0;
  m_tapeFSeqOfEndOfSessionFile            = 0;
  m_tapeFSeqOfLastFileAdded               = 0;
  m_tapeFSeqOfLastFileWrittenWithoutFlush = 0;
  m_tapeFSeqOfMaxBytesFile                = 0;
  m_tapeFSeqOfMaxFilesFile                = 0;

  m_migrations.clear();
}


//-----------------------------------------------------------------------------
// getNbPendingMigrations
//-----------------------------------------------------------------------------
uint32_t castor::tape::tapebridge::PendingMigrationsStore::
  getNbPendingMigrations() const {
  return m_migrations.size();
}
