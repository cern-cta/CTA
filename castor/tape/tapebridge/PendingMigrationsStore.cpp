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
castor::tape::tapebridge::PendingMigrationsStore::PendingMigrationsStore() {
  clear();
}


//-----------------------------------------------------------------------------
// Copy constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::PendingMigrationsStore::PendingMigrationsStore(
  PendingMigrationsStore &) {
  // Should never be called, therefore do nothing
}


//-----------------------------------------------------------------------------
// Assignment operator
//-----------------------------------------------------------------------------
castor::tape::tapebridge::PendingMigrationsStore::PendingMigrationsStore
  &castor::tape::tapebridge::PendingMigrationsStore::operator=(
  const PendingMigrationsStore&) {
  // Should never be called, therefore do nothing
  return *this;
}


//-----------------------------------------------------------------------------
// Constructor of inner struct PendingMigration
//-----------------------------------------------------------------------------
castor::tape::tapebridge::PendingMigrationsStore::PendingMigration::
  PendingMigration() {
  // Do nothing
}


//-----------------------------------------------------------------------------
// Constructor of inner struct PendingMigration
//-----------------------------------------------------------------------------
castor::tape::tapebridge::PendingMigrationsStore::PendingMigration::
  PendingMigration(
  const PendingMigrationStatus s,
  const tapegateway::FileToMigrate &f):
  status(s),
  fileToMigrate(f) {
  // Do nothing
}


//-----------------------------------------------------------------------------
// add
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::PendingMigrationsStore::add(
  const tapegateway::FileToMigrate &fileToMigrate)
  throw(castor::exception::Exception) {

  // Throw an exception if the tape-file sequence-number of the file to
  // migrate message is invalid
  if(0 >= fileToMigrate.fseq()) {
    TAPE_THROW_CODE(EBADMSG,
    ": Failed to add pending file-migration to pending file-migration store"
    ": File to migrate message contains an invalid fseq"
    ": value=" << fileToMigrate.fseq());
  }

  // Throw an exception if the store already contains the pending
  // file-migration that should be added
  PendingMigrationMap::iterator itor = m_pendingMigrations.find(
    fileToMigrate.fseq());
  if(itor != m_pendingMigrations.end()) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to add pending file-migration to pending file-migration store"
      ": Store already contains a file with the same tape-file sequence-number"
      ": fseq=" << fileToMigrate.fseq());
  }

  // Throw an exception if the store is not empty and the tape-file
  // sequence-number of the file to be added is not 1 plus the tape-file
  // sequence-number of the previously added file
  if(!m_pendingMigrations.empty() &&
    (m_tapeFseqOfLastFileAdded + 1 != (uint32_t)fileToMigrate.fseq())) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to add pending file-migration to pending file-migration store"
      ": Tape-file sequence-number of file is not 1 plus the value of the"
      " previously added file"
      ": expected=" << (m_tapeFseqOfLastFileAdded + 1) <<
      ": actual=" << fileToMigrate.fseq());
  }

  // Add the pending file-migration
  {
    PendingMigration pendingMigration(
      PendingMigration::PENDINGMIGRATION_SENT_TO_RTCPD,
      fileToMigrate);
    m_pendingMigrations[fileToMigrate.fseq()] = pendingMigration;
  }
  m_tapeFseqOfLastFileAdded = fileToMigrate.fseq();
}


//-----------------------------------------------------------------------------
// markAsWrittenWithoutFlush
//-----------------------------------------------------------------------------
void
  castor::tape::tapebridge::PendingMigrationsStore::markAsWrittenWithoutFlush(
  const tapegateway::FileMigratedNotification &fileMigratedNotification)
  throw(castor::exception::Exception) {

  // Throw an exception if the tape-file sequence-number of the file
  // migrated message is invalid
  if(0 >= fileMigratedNotification.fseq()) {
    TAPE_THROW_CODE(EBADMSG,
    ": Failed to mark pending file-migration as written without flush"
    ": File migrated message contains an invalid fseq"
    ": value=" << fileMigratedNotification.fseq());
  }

  // Try to find the pending file-migration whose state is to be updated
  PendingMigrationMap::iterator itor = m_pendingMigrations.find(
    fileMigratedNotification.fseq());

  // Throw an exception if the pending file-migration could not be found
  if(m_pendingMigrations.end() == itor) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to mark pending file-migration as written without flush"
      ": Pending file-migration not found"
      ": fseq=" << fileMigratedNotification.fseq());
  }

  // Throw an exception if at least one file has already been marked as being
  // written but not yet flushed to tape and the tape-file sequence-number of
  // the current file to be marked is not 1 plus that of the previously marked
  // file
  if(m_tapeFseqOfLastFileWrittenWithoutFlush != 0 &&
    (m_tapeFseqOfLastFileWrittenWithoutFlush + 1 !=
      (uint32_t)fileMigratedNotification.fseq())) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to mark pending file-migration as written without flush"
      ": Tape-file sequence-number of file is not 1 plus the value of the"
      " previously marked file"
      ": expected=" << (m_tapeFseqOfLastFileWrittenWithoutFlush + 1) <<
      ": actual=" << fileMigratedNotification.fseq());
  }

  // Throw an exception if any of the fields common to both the file to migrate
  // request and the file-migrated notification do not match exactly
  try {
    checkForMismatches(itor->second.fileToMigrate, fileMigratedNotification);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to mark pending file-migration as written without flush" <<
      ex.getMessage().str());
  }

  // Copy into the pending file-migration the file-migrated notification
  // message that will be sent to the client when the rtcpd daemon has notified
  // the tapebridged daemon that the file has been flushed to tape
  itor->second.fileMigratedNotification = fileMigratedNotification;

  // Update the state of the pending file-migration
  itor->second.status =
    PendingMigration::PENDINGMIGRATION_WRITTEN_WITHOUT_FLUSH;

  // Update the tape-file sequence-number of the last file marked as written
  // without a flush
  m_tapeFseqOfLastFileWrittenWithoutFlush = fileMigratedNotification.fseq();
}


//-----------------------------------------------------------------------------
// checkForMismatches
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::PendingMigrationsStore::checkForMismatches(
  const tapegateway::FileToMigrate &fileToMigrate,
  const tapegateway::FileMigratedNotification &fileMigratedNotification)
  const throw(castor::exception::Exception) {

  castor::exception::Exception ex(ECANCELED);
  bool foundMismatch = false;

  if(fileToMigrate.mountTransactionId() !=
    fileMigratedNotification.mountTransactionId()) {
    ex.getMessage() <<
      ": mountTransactionId mismatch"
      " fileToMigrate=" <<
      fileToMigrate.mountTransactionId() <<
      " fileMigratedNotification=" <<
      fileMigratedNotification.mountTransactionId();
    foundMismatch = true;
  }
  if(fileToMigrate.fileTransactionId() !=
    fileMigratedNotification.fileTransactionId()) {
    ex.getMessage() <<
      ": fileTransactionId mismatch"
      " fileToMigrate="  <<
      fileToMigrate.fileTransactionId() <<
      " fileMigratedNotification=" <<
      fileMigratedNotification.fileTransactionId();
    foundMismatch = true;
  }
  if(fileToMigrate.nshost() !=
    fileMigratedNotification.nshost()) {
    ex.getMessage() <<
      ": nshost mismatch"
      " fileToMigrate="  <<
      fileToMigrate.nshost() <<
      " fileMigratedNotification=" <<
      fileMigratedNotification.nshost();
    foundMismatch = true;
  }
  if(fileToMigrate.fileid() !=
    fileMigratedNotification.fileid()) {
    ex.getMessage() <<
      ": fileid mismatch"
      " fileToMigrate="  <<
      fileToMigrate.fileid() <<
      " fileMigratedNotification=" <<
      fileMigratedNotification.fileid();
    foundMismatch = true;
  }
  if(fileToMigrate.fseq() !=
    fileMigratedNotification.fseq()) {
    ex.getMessage() <<
      ": fseq mismatch"
      " fileToMigrate="  <<
      fileToMigrate.fseq() <<
      " fileMigratedNotification=" <<
      fileMigratedNotification.fseq();
    foundMismatch = true;
  }
  if(fileToMigrate.positionCommandCode() !=
    fileMigratedNotification.positionCommandCode()) {
    ex.getMessage() <<
      ": positionCommandCode mismatch"
      " fileToMigrate="  <<
      fileToMigrate.positionCommandCode() <<
      " fileMigratedNotification=" <<
      fileMigratedNotification.positionCommandCode();
    foundMismatch = true;
  }
  if(fileToMigrate.fileSize() !=
    fileMigratedNotification.fileSize()) {
    ex.getMessage() <<
      ": fileSize mismatch"
      " fileToMigrate="  <<
      fileToMigrate.fileSize() <<
      " fileMigratedNotification=" <<
      fileMigratedNotification.fileSize();
    foundMismatch = true;
  }

  if(foundMismatch) {
    throw ex;
  }
}


//-----------------------------------------------------------------------------
// getAndRemoveFilesWrittenWithoutFlush
//-----------------------------------------------------------------------------
std::list<castor::tape::tapegateway::FileMigratedNotification>
  castor::tape::tapebridge::PendingMigrationsStore::
  getAndRemoveFilesWrittenWithoutFlush(const uint32_t maxFseq)
  throw(castor::exception::Exception) {
  std::list<castor::tape::tapegateway::FileMigratedNotification> outputList;

  // Check method arguments
  if(0 == maxFseq) {
    TAPE_THROW_CODE(EINVAL,
      ": Failed to get and remove pending file-migrations written without"
      " flush"
      ": maxFseq contains the invalid value of 0");
  }

  // For each pending file-migration in the store in ascending order of
  // tape-file sequence-number
  for(PendingMigrationMap::iterator itor = m_pendingMigrations.begin();
    itor != m_pendingMigrations.end();) {

    // Break out of the loop if we have reached a pending file-migration with
    // a tape-file sequence-number greater than the maximum to be got and
    // removed
    if(maxFseq < (uint32_t)(itor->second.fileToMigrate.fseq())) {
      break;
    }

    // Throw an exception if the pending file-migration has not been marked as
    // being written without a flush
    if(itor->second.status !=
      PendingMigration::PENDINGMIGRATION_WRITTEN_WITHOUT_FLUSH) {
      TAPE_THROW_CODE(ECANCELED,
        ": Failed to get and remove pending file-migrations written without"
        " flush"
        ": maxFseq=" << maxFseq <<
        ": Found pending file-migration not marked as being written without"
        " flush"
        ": badFseq=" << itor->second.fileToMigrate.fseq());
    }

    // Push the file-migrated notfication of the pending file-migration onto
    // the back of the output list
    outputList.push_back(itor->second.fileMigratedNotification);

    // Erase the pending file-migration from the store
    m_pendingMigrations.erase(itor++);
  }

  return outputList;
}


//-----------------------------------------------------------------------------
// clear
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::PendingMigrationsStore::clear() {
  m_tapeFseqOfLastFileAdded               = 0;
  m_tapeFseqOfLastFileWrittenWithoutFlush = 0;

  m_pendingMigrations.clear();
}
