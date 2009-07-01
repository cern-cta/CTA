/******************************************************************************
 *                 castor/tape/tpcp/StreamOperators.cpp
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
 
#include "castor/tape/tpcp/StreamOperators.hpp"
#include "castor/tape/utils/utils.hpp"


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::Action
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::Action &value) {

  os << value.str();

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::FilenameList
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::FilenameList &value) {

  os << '{';

  for(castor::tape::tpcp::FilenameList::const_iterator itor =
    value.begin(); itor != value.end(); itor++) {

    // Write a separating comma if not the first item in the list
    if(itor!=value.begin()) {
      os << ",";
    }

    os << "\"" << *itor << "\"";
  }

  os << '}';

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::ParsedCommandLine
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::ParsedCommandLine &value) {

  using namespace castor::tape;
  using namespace castor::tape::tpcp;

  os <<"{"
        "debugOptionSet=" << utils::boolToString(value.debugOptionSet)<<","
        "helpOptionSet="  << utils::boolToString(value.helpOptionSet) <<","
        "action="         << value.action                             <<","
        "vid=";
  if(value.vid == NULL) {
    os << "NULL,";
  } else {
    os << "\"" << value.vid << "\",";
  }
  os <<"tapeFseqRanges="    << value.tapeFseqRanges                        <<","
       "fileListOptionSet=" << utils::boolToString(value.fileListOptionSet)<<","
       "fileListFilename=\""<< value.fileListFilename                   << "\","
       "filenames="         << value.filenames
     <<"}";

   return os;
}


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::TapeFseqRange
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::TapeFseqRange &value) {

  os << value.lower << "-";

  // 0 means end of tape ("END")
  if(value.upper == 0) {
    os << "END";
  } else {
    os << value.upper;
  }

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::TapeFseqRangeList
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::TapeFseqRangeList &value) {

  os << '{';

  for(castor::tape::tpcp::TapeFseqRangeList::const_iterator itor =
    value.begin(); itor != value.end(); itor++) {

    // Write a separating comma if not the first item in the list
    if(itor!=value.begin()) {
      os << ",";
    }

    os << *itor;
  }

  os << '}';

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for vmgr_tape_info 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const vmgr_tape_info &value) {

  os << "{"
        "vid=\""               << value.vid                 << "\","
        "vsn=\""               << value.vsn                 << "\","
        "library=\""           << value.library             << "\","
        "density=\""           << value.density             << "\","
        "lbltype=\""           << value.lbltype             << "\","
        "model=\""             << value.model               << "\","
        "media_letter=\""      << value.media_letter        << "\","
        "manufacturer=\""      << value.manufacturer        << "\","
        "sn= \""               << value.sn                  << "\","
        "nbsides="             << value.nbsides             <<   ","
        "etime="               << value.etime               <<   ","
        "rcount="              << value.rcount              <<   ","
        "wcount="              << value.wcount              <<   ","
        "rhost=\""             << value.rhost               << "\","
        "whost=\""             << value.whost               << "\","
        "rjid="                << value.rjid                <<   ","
        "wjid="                << value.wjid                <<   ","
        "rtime="               << value.rtime               <<   ","
        "wtime="               << value.wtime               <<   ","
        "side="                << value.side                <<   ","
        "poolname=\""          << value.poolname            << "\","
        "status="              << value.status              <<   ","
        "estimated_free_space="<< value.estimated_free_space<<   ","
        "nbfiles="             << value.nbfiles
     << "}";

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::BaseFileInfo object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::BaseFileInfo &value) {

  os << "transactionId="       << value.transactionId()       <<   ","
        "nshost=\""            << value.nshost()              << "\","     
        "fileid="              << value.fileid()              <<   ","
        "fseq="                << value.fseq()                <<   ","
        "positionCommandCode=" << value.positionCommandCode() <<   "";

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileToRecall object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::FileToRecall &value) {

  os << "{"
     << (castor::tape::tapegateway::BaseFileInfo&)value
     << "path=\"" << value.path() << "\","
     << std::hex
     << "blockId0="   << (int)value.blockId0() << ","   
        "blockId1="   << (int)value.blockId1() << ","
        "blockId2="   << (int)value.blockId2() << ","
        "blockId3="   << (int)value.blockId3() << "}"
     << std::dec;

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileToRecallRequest object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::FileToRecallRequest &value) {

  os << "transactionId=" << value.transactionId();

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileRecalledNotification object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::FileRecalledNotification &value) {

  os << "{"
     << (castor::tape::tapegateway::BaseFileInfo&)value
     << "path=\""         << value.path()         << "\","
     << "checksumName=\"" << value.checksumName() << "\"}"; 

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::EndNotification object
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::EndNotification &value) {

  os << "{"
        "transactionId=" << value.transactionId()<< "}";

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::EndNotificationErrorReport object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::EndNotificationErrorReport &value)
  {

  os << "{"
        "transactionId=" << value.transactionId()<<   "," 
        "errorCode="     << value.errorCode()    <<   "," 
        "errorMessage=\""<< value.errorMessage() << "\"}";

  return os;
}
