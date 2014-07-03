/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/utils/utils.hpp"
#include "h/Castor_limits.h"

namespace castor {
namespace tape {
namespace label {

/**
 * Data type used to store the results of parsing the tplabel command-line.
 */
struct ParsedTpLabelCommandLine {

  /**
   * The filename of the "file list" file.
   */
  char drive[CA_MAXUNMLEN+1];

  /**
   * The filename of the "file list" file.
   */
  char dgn[CA_MAXDGNLEN+1];

  /**
   * The VID of the tape to be mounted.
   */
  char vid[CA_MAXVIDLEN+1];
  
  bool driveIsSet;
  bool dgnIsSet;
  bool vidIsSet;
  bool helpIsSet;
  bool forceIsSet;
  bool debugIsSet;

  /**
   * Constructor.
   */
  ParsedTpLabelCommandLine() :
  driveIsSet(false),
  dgnIsSet(false),
  vidIsSet(false),
  helpIsSet(false),
  forceIsSet(false),
  debugIsSet(false)
  {
    castor::utils::setBytes(drive, '\0');
    castor::utils::setBytes(dgn, '\0');
    castor::utils::setBytes(vid, '\0');
  }
  
  bool allCompulsoryOptionsSet() {
    return driveIsSet && dgnIsSet && vidIsSet;
  }
}; // struct ParsedTpLabelCommandLine

} // namespace label
} // namespace tape
} // namespace castor
