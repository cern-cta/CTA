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
#include "castor/tape/utils/utils.hpp"
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
  Action            action;
  bool              debugOptionSet;
  std::string       fileListFilename;
  bool              fileListOptionSet;
  FilenameList      filenames;
  bool              helpOptionSet;
  bool              serverOptionSet;
  char              server[CA_MAXHOSTNAMELEN+1];
  int32_t           tapeFseqPosition;
  bool              tapeFseqPositionOptionSet;
  TapeFseqRangeList tapeFseqRanges;
  char              vid[CA_MAXVIDLEN+1];

  /**
   * Constructor.
   */
  ParsedCommandLine() :
    action(Action::read),
    debugOptionSet(false),
    fileListOptionSet(false),
    helpOptionSet(false),
    serverOptionSet(false),
    tapeFseqPosition(0),
    tapeFseqPositionOptionSet(false) {

    utils::setBytes(server, '\0');
    utils::setBytes(vid   , '\0');
  }
}; // struct ParsedCommandLine

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_PARSEDCOMMANDLINE_HPP
