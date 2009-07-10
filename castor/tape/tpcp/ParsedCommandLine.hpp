/******************************************************************************
 *                 castor/tape/tpcp/ParsedCommandLine.hpp
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

#ifndef CASTOR_TAPE_TPCP_PARSEDCOMMANDLINE_HPP
#define CASTOR_TAPE_TPCP_PARSEDCOMMANDLINE_HPP 1

#include "castor/tape/tpcp/Action.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/TapeFseqRangeList.hpp"
#include "h/Castor_limits.h"

#include <list>
#include <ostream>
#include <string>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Data type used to store the results of parsing the command-line.
 */
struct ParsedCommandLine {
  bool              debugOptionSet;
  bool              helpOptionSet;
  Action            action;
  char              vid[CA_MAXVIDLEN+1];
  TapeFseqRangeList tapeFseqRanges;
  bool              tapeFseqPositionOptionSet;
  uint32_t          tapeFseqPosition;
  bool              fileListOptionSet;
  std::string       fileListFilename;
  FilenameList      filenames;

  /**
   * Constructor.
   */
  ParsedCommandLine() :
    debugOptionSet(false),
    helpOptionSet(false),
    action(Action::read),
    tapeFseqPositionOptionSet(false),
    tapeFseqPosition(0),
    fileListOptionSet(false) {

    for(size_t i=0; i<sizeof(vid); i++) {
      vid[i] = '\0';
    }
  }
}; // struct ParsedCommandLine

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_PARSEDCOMMANDLINE_HPP
