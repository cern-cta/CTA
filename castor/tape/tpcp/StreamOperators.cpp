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
        "debugSet=" << utils::boolToString(value.debugSet)<<","
        "helpSet="  << utils::boolToString(value.helpSet) <<","
        "action="         << value.action                             <<","
        "vid="
     << "\"" << value.vid << "\","
     <<"tapeFseqRanges="    << value.tapeFseqRanges                        <<","
       "fileListSet=" << utils::boolToString(value.fileListSet)<<","
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

  if(value.isEmpty()) {
    os << "EMPTY";
  } else {
    uint32_t lower = 0;
    uint32_t upper = 0;

    try {
      lower = value.lower();
      upper = value.upper();

      os << lower << "-";

      // An upper value of 0 means END of tape
      if(upper !=0) {
        os << upper;
      } else {
        os << "END";
      }
    } catch(castor::exception::NoValue &ex) {
      os << "ERROR";
    }
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
// ostream << operator for vmgr_tape_info_byte_u64 
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const vmgr_tape_info_byte_u64 &value) {

  os << "{"
        "vid=\""                        << value.vid                 << "\","
        "vsn=\""                        << value.vsn                 << "\","
        "library=\""                    << value.library             << "\","
        "density=\""                    << value.density             << "\","
        "lbltype=\""                    << value.lbltype             << "\","
        "model=\""                      << value.model               << "\","
        "media_letter=\""               << value.media_letter        << "\","
        "manufacturer=\""               << value.manufacturer        << "\","
        "sn= \""                        << value.sn                  << "\","
        "nbsides="                      << value.nbsides             <<   ","
        "etime="                        << value.etime               <<   ","
        "rcount="                       << value.rcount              <<   ","
        "wcount="                       << value.wcount              <<   ","
        "rhost=\""                      << value.rhost               << "\","
        "whost=\""                      << value.whost               << "\","
        "rjid="                         << value.rjid                <<   ","
        "wjid="                         << value.wjid                <<   ","
        "rtime="                        << value.rtime               <<   ","
        "wtime="                        << value.wtime               <<   ","
        "side="                         << value.side                <<   ","
        "poolname=\""                   << value.poolname            << "\","
        "status="                       << value.status              <<   ","
        "estimated_free_space_byte_u64="<< value.estimated_free_space_byte_u64
          <<   ","
        "nbfiles="                      << value.nbfiles
     << "}";

  return os;
}
