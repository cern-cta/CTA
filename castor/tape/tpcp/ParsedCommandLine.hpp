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

#include "h/Castor_limits.h"
#include "castor/tape/tpcp/Action.hpp"

#include <list>
#include <string>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * A range of uint32_t's specified by an inclusive upper and lower set of
 * bounds.
 */
struct Uint32Range {
  uint32_t lower;
  uint32_t upper;
};

/**
 * List of ranges.
 */
typedef std::list<Uint32Range> Uint32RangeList;

/**
 * Data type used to store the results of parsing the command-line.
 */
struct ParsedCommandLine {
  bool                   debugOptionSet;
  bool                   helpOptionSet;
  Action                 action;
  char                   vid[CA_MAXVIDLEN+1];
  Uint32RangeList        tapeFseqRanges;
  std::list<std::string> filenamesList;

  /**
   * Constructor.
   */
  ParsedCommandLine() :
    debugOptionSet(false),
    helpOptionSet(false),
    action(Action::read) {

    for(size_t i=0; i<sizeof(vid); i++) {
      vid[i] = '\0';
    }
  }
}; // struct ParsedCommandLine

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_PARSEDCOMMANDLINE_HPP
