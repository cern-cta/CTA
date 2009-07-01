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
        "transactionId="       << value.transactionId()       <<   ","
        "nshost=\""            << value.nshost()              << "\","     
        "fileid="              << value.fileid()              <<   ","
        "fseq="                << value.fseq()                <<   ","
        "positionCommandCode=" << value.positionCommandCode() <<   "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::FileToRecall &value) throw() {

  os << "{"
     << (castor::tape::tapegateway::BaseFileInfo&)value
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
  const castor::tape::tapegateway::FileToRecallRequest &value) throw() {

  os << "{"
        "transactionId=" << value.transactionId()<< "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::FileRecalledNotification &value) throw() {

  os << "{"
     << (castor::tape::tapegateway::BaseFileInfo&)value
     << "path=\""         << value.path()         << "\","
     << "checksumName=\"" << value.checksumName() << "\"}"; 
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::EndNotification &value) throw() {

  os << "{"
        "transactionId=" << value.transactionId() << "}";
}


//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void castor::tape::tpcp::StreamHelper::write(std::ostream &os,
  const castor::tape::tapegateway::EndNotificationErrorReport &value) throw() {

  os << "{"
        "transactionId="  << value.transactionId() <<   "," 
        "errorCode="      << value.errorCode()     <<   "," 
        "errorMessage=\"" << value.errorMessage()  << "\"}";
}
