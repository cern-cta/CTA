/******************************************************************************
 *                 castor/tape/tpcp/ParsedCommandLine.cpp
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
 
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/ParsedCommandLine.hpp"
#include "castor/tape/tpcp/TapeFseqRange.hpp"
#include "castor/tape/utils/utils.hpp"


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
