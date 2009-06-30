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

#include <errno.h>


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::Action
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::Action &action) {

  os << action.str();

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::FilenameList
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::FilenameList &list) {

  os << '{';

  for(castor::tape::tpcp::FilenameList::const_iterator itor =
    list.begin(); itor != list.end(); itor++) {

    // Write a separating comma if not the first item in the list
    if(itor!=list.begin()) {
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
  const castor::tape::tpcp::ParsedCommandLine &cmdLine) {

  using namespace castor::tape;
  using namespace castor::tape::tpcp;

  os << "debugOptionSet    = " << utils::boolToString(cmdLine.debugOptionSet)
     << std::endl
     << "helpOptionSet     = " << utils::boolToString(cmdLine.helpOptionSet)
     << std::endl
     << "action            = " << cmdLine.action
     << std::endl
     << "vid               = ";
  if(cmdLine.vid == NULL) {
    os << "NULL";
  } else {
    os << "\"" << cmdLine.vid << "\"";
  }
  os << std::endl;
  os << "tapeFseqRanges    = " << cmdLine.tapeFseqRanges
     << std::endl
     << "fileListOptionSet = " << utils::boolToString(cmdLine.fileListOptionSet)
     << std::endl
     << "fileListFilename  = \"" << cmdLine.fileListFilename << "\""
     << std::endl
     << "filenames         = " << cmdLine.filenames
     << std::endl;

   return os;
}


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::TapeFseqRange
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::TapeFseqRange &range) {

  os << range.lower << "-";

  // 0 means end of tape ("END")
  if(range.upper == 0) {
    os << "END";
  } else {
    os << range.upper;
  }

  return os;
}


//------------------------------------------------------------------------------
// ostream << operator for castor::tape::tpcp::TapeFseqRangeList
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const castor::tape::tpcp::TapeFseqRangeList &list) {

  os << '{';

  for(castor::tape::tpcp::TapeFseqRangeList::const_iterator itor =
    list.begin(); itor != list.end(); itor++) {

    // Write a separating comma if not the first item in the list
    if(itor!=list.begin()) {
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
std::ostream &operator<<(std::ostream &os,
  const vmgr_tape_info &tapeInfo) {

  os << std::endl
     << "vid                  = \"" << tapeInfo.vid << "\""          << std::endl
     << "vsn                  = \"" << tapeInfo.vsn << "\""          << std::endl
     << "library              = \"" << tapeInfo.library << "\""      << std::endl
     << "density              = \"" << tapeInfo.density << "\""      << std::endl
     << "lbltype              = \"" << tapeInfo.lbltype << "\""      << std::endl
     << "model                = \"" << tapeInfo.model << "\""        << std::endl
     << "media_letter         = \"" << tapeInfo.media_letter << "\"" << std::endl
     << "manufacturer         = \"" << tapeInfo.manufacturer << "\"" << std::endl
     << "sn                   = \"" << tapeInfo.sn << "\""           << std::endl
     << "nbsides              = "   << tapeInfo.nbsides              << std::endl
     << "etime                = "   << tapeInfo.etime                << std::endl
     << "rcount               = "   << tapeInfo.rcount               << std::endl
     << "wcount               = "   << tapeInfo.wcount               << std::endl
     << "rhost                = \"" << tapeInfo.rhost << "\""        << std::endl
     << "whost                = \"" << tapeInfo.whost << "\""        << std::endl
     << "rjid                 = "   << tapeInfo.rjid                 << std::endl
     << "wjid                 = "   << tapeInfo.wjid                 << std::endl
     << "rtime                = "   << tapeInfo.rtime                << std::endl
     << "wtime                = "   << tapeInfo.wtime                << std::endl
     << "side                 = "   << tapeInfo.side                 << std::endl
     << "poolname             = \"" << tapeInfo.poolname << "\""     << std::endl
     << "status               = "   << tapeInfo.status               << std::endl
     << "estimated_free_space = "   << tapeInfo.estimated_free_space << std::endl
     << "nbfiles              = "   << tapeInfo.nbfiles              << std::endl;

  return os;

}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileToRecall object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const tapegateway::FileToRecall &fileToRecallInfo) {

  castor::ObjectSet alreadyPrinted;
  fileToRecallInfo.print(os, "", alreadyPrinted);

  return os;

}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileToRecallRequest object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const tapegateway::FileToRecallRequest &fileToRecallRequest) {

//  castor::ObjectSet alreadyPrinted;
//  fileToRecallRequest.print(os, "", alreadyPrinted);

  // Output of all members
  os << std::endl;
  //   << "transactionId = " << fileToRecallRequest.transactionId << std::endl
  //   << "id            = " << fileToRecallRequest.id            << std::endl;

  return os;

}


//------------------------------------------------------------------------------
// ostream << operator for tapegateway::FileRecalledNotification object 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const tapegateway::FileRecalledNotification &notificationInfo){

  castor::ObjectSet alreadyPrinted;
  notificationInfo.print(os, "", alreadyPrinted);
  notificationInfo.print();

  return os;

 
}
