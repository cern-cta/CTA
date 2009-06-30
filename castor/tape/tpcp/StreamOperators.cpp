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

  os << "debugOptionSet    = " << utils::boolToString(value.debugOptionSet)
     << std::endl
     << "helpOptionSet     = " << utils::boolToString(value.helpOptionSet)
     << std::endl
     << "action            = " << value.action
     << std::endl
     << "vid               = ";
  if(value.vid == NULL) {
    os << "NULL";
  } else {
    os << "\"" << value.vid << "\"";
  }
  os << std::endl;
  os << "tapeFseqRanges    = " << value.tapeFseqRanges
     << std::endl
     << "fileListOptionSet = " << utils::boolToString(value.fileListOptionSet)
     << std::endl
     << "fileListFilename  = \"" << value.fileListFilename << "\""
     << std::endl
     << "filenames         = " << value.filenames
     << std::endl;

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

  os << "vid                  = \"" << value.vid << "\""          << std::endl
     << "vsn                  = \"" << value.vsn << "\""          << std::endl
     << "library              = \"" << value.library << "\""      << std::endl
     << "density              = \"" << value.density << "\""      << std::endl
     << "lbltype              = \"" << value.lbltype << "\""      << std::endl
     << "model                = \"" << value.model << "\""        << std::endl
     << "media_letter         = \"" << value.media_letter << "\"" << std::endl
     << "manufacturer         = \"" << value.manufacturer << "\"" << std::endl
     << "sn                   = \"" << value.sn << "\""           << std::endl
     << "nbsides              = "   << value.nbsides              << std::endl
     << "etime                = "   << value.etime                << std::endl
     << "rcount               = "   << value.rcount               << std::endl
     << "wcount               = "   << value.wcount               << std::endl
     << "rhost                = \"" << value.rhost << "\""        << std::endl
     << "whost                = \"" << value.whost << "\""        << std::endl
     << "rjid                 = "   << value.rjid                 << std::endl
     << "wjid                 = "   << value.wjid                 << std::endl
     << "rtime                = "   << value.rtime                << std::endl
     << "wtime                = "   << value.wtime                << std::endl
     << "side                 = "   << value.side                 << std::endl
     << "poolname             = \"" << value.poolname << "\""     << std::endl
     << "status               = "   << value.status               << std::endl
     << "estimated_free_space = "   << value.estimated_free_space << std::endl
     << "nbfiles              = "   << value.nbfiles              << std::endl;

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::BaseFileInfo object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::BaseFileInfo &value) {

  os << "transactionId       = "   << value.transactionId()       << std::endl
     << "nshost              = \"" << value.nshost() << "\""      << std::endl
     << "fileid              = "   << value.fileid()              << std::endl
     << "fseq                = "   << value.fseq()                << std::endl
     << "positionCommandCode = "   << value.positionCommandCode() << std::endl;

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileToRecall object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::FileToRecall &value) {

  os << (castor::tape::tapegateway::BaseFileInfo&)value
     << "path                = \"" << value.path() << "\""  << std::endl
     << std::hex
     << "blockId0            = "   << (int)value.blockId0() << std::endl
     << "blockId1            = "   << (int)value.blockId1() << std::endl
     << "blockId2            = "   << (int)value.blockId2() << std::endl
     << "blockId3            = "   << (int)value.blockId3() << std::endl
     << std::dec;

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileToRecallRequest object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::FileToRecallRequest &value) {

  os << "transactionId = "   << value.transactionId() << std::endl;

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileRecalledNotification object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tapegateway::FileRecalledNotification &value)
  {

  os << (castor::tape::tapegateway::BaseFileInfo&)value
     << "path                = \"" << value.path() << "\""         << std::endl
     << "checksumName        = \"" << value.checksumName() << "\"" << std::endl;

  return os;
}
