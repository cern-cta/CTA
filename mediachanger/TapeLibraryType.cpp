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

#include "mediachanger/TapeLibraryType.hpp"

//------------------------------------------------------------------------------
// tapeLibraryTypeToString
//------------------------------------------------------------------------------
const char *cta::mediachanger::tapeLibraryTypeToString(
  const TapeLibraryType libraryType) throw() {
  switch(libraryType) {
  case TAPE_LIBRARY_TYPE_NONE  : return "NONE";
  case TAPE_LIBRARY_TYPE_ACS   : return "ACS";
  case TAPE_LIBRARY_TYPE_MANUAL: return "MANUAL";
  case TAPE_LIBRARY_TYPE_SCSI  : return "SCSI";
  default                      : return "UNKNOWN";
  }
}
