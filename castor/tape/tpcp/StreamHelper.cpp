/******************************************************************************
 *                 castor/tape/tpcp/StreamHelper.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/tape/tpcp/StreamHelper.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::StreamHelper::StreamHelper() {
  // Do nothing
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::BaseFileInfo &value) throw() {

  os << "{"
        "mountTransactionId()=" << value.mountTransactionId()  <<   ","
        "fileTransactionId()="  << value.fileTransactionId()   <<   ","
        "nshost=\""             << value.nshost()              << "\","
        "fileid="               << value.fileid()              <<   ","
        "fseq="                 << value.fseq()                <<   ","
        "positionCommandCode="  << value.positionCommandCode() <<   "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::FileToRecall &value) throw() {

  os << "{";
  write(os, (castor::tape::tapegateway::BaseFileInfo&)value);
  os <<                                           ","
     << "path=\"" << value.path()            << "\","
     << std::hex
     << "blockId0=" << (int)value.blockId0() <<   ","   
        "blockId1=" << (int)value.blockId1() <<   ","
        "blockId2=" << (int)value.blockId2() <<   ","
        "blockId3=" << (int)value.blockId3() <<   "}"
     << std::dec;
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::FileToMigrate &value) throw() {

  os << "{";
  write(os, (castor::tape::tapegateway::BaseFileInfo&)value);
  os <<                                                              ","
     << "path=\""               << value.path()                 << "\","
     << "fileSize="             << value.fileSize()             <<   ","
     << "lastKnownFilename="    << value.lastKnownFilename()    <<   ","
     << "lastModificationTime=" << value.lastModificationTime() <<   "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::NoMoreFiles &value) throw() {

  os << "{"
        "mountTransactionId()=" << value.mountTransactionId()<< "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::FileToMigrateRequest &value) throw() {

  os << "{"
        "mountTransactionId()=" << value.mountTransactionId()<< "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::FileToRecallRequest &value) throw() {

  os << "{"
        "mountTransactionId()=" << value.mountTransactionId()<< "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::FileMigratedNotification &value) throw() {
  os <<                                                          "{"
     << "fileSize="           << value.fileSize()           <<   ","
     << "checksumName=\""     << value.checksumName()       << "\"," 
     << std::hex
     << "checksum=0x"         << value.checksum()           <<   ","
     << std::dec
     << "compressedFileSize=" << value.compressedFileSize() <<   ","
     << std::hex
     << "blockId0="           << (int)value.blockId0()      <<   ","
        "blockId1="           << (int)value.blockId1()      <<   ","
        "blockId2="           << (int)value.blockId2()      <<   ","
        "blockId3="           << (int)value.blockId3()      <<   "}"
     << std::dec;
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::FileRecalledNotification &value) throw() {

  os << "{";
  write(os, (castor::tape::tapegateway::BaseFileInfo&)value);
  os <<                                                ","
     << "path=\""         << value.path()         << "\","
     << "checksumName=\"" << value.checksumName() << "\","
     << "fileSize="       << value.fileSize()     <<   "}"; 
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::NotificationAcknowledge &value) throw() {

  os << "{"
        "mountTransactionId()=" << value.mountTransactionId()<< "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::EndNotification &value) throw() {

  os << "{"
        "mountTransactionId()=" << value.mountTransactionId() << "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::EndNotificationErrorReport &value) throw() {

  os << "{"
        "mountTransactionId()="  << value.mountTransactionId() <<   "," 
        "errorCode="      << value.errorCode()     <<   "," 
        "errorMessage=\"" << value.errorMessage()  << "\"}";
}

//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::VolumeRequest &value) throw() {

  os << "{"
        "mountTransactionId()="  << value.mountTransactionId() <<   ","
        "unit()=\"" << value.unit()  << "\"}";
} 

