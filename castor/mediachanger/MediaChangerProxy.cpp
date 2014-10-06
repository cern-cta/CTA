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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/mediachanger/MediaChangerProxy.hpp"
#include "h/Castor_limits.h"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::mediachanger::MediaChangerProxy::~MediaChangerProxy() {
}

//------------------------------------------------------------------------------
// verifyVidAndLibrarySlot
//------------------------------------------------------------------------------
void castor::mediachanger::MediaChangerProxy::verifyVidAndLibrarySlot(
  const std::string &vid,
  const mediachanger::TapeLibraryType expectedLibraryType,
  const mediachanger::TapeLibrarySlot librarySlot) {
  // Verify VID
  if(vid.empty()) {
    castor::exception::Exception ex;
    ex.getMessage() << "VID is an empty string";
    throw ex;
  }
  if(CA_MAXVIDLEN < vid.length()) {
    castor::exception::Exception ex;
    ex.getMessage() << "VID is too long"
      ": vid=" << vid << " maxLen=" << CA_MAXVIDLEN << " actualLen=" <<
      vid.length();
    throw ex;
  }

  // Verify library slot
  const TapeLibraryType actualLibraryType = librarySlot.getLibraryType();
  if(expectedLibraryType != actualLibraryType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Incompatible library type: expected=" <<
      mediachanger::tapeLibraryTypeToString(expectedLibraryType) << " actual=" <<
      mediachanger::tapeLibraryTypeToString(actualLibraryType);
    throw ex;
  }
}
