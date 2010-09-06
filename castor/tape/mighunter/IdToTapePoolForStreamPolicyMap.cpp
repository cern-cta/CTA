/******************************************************************************
 *         castor/tape/mighunter/IdToTapePoolForStreamPolicyMapToString.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/mighunter/IdToTapePoolForStreamPolicyMap.hpp"


//------------------------------------------------------------------------------
// writeIdToTapePoolForStreamPolicyMapToString
//------------------------------------------------------------------------------
void castor::tape::mighunter::writeIdToTapePoolForStreamPolicyMapToString(
  std::ostream &os, const IdToTapePoolForStreamPolicyMap &m) {
  for(IdToTapePoolForStreamPolicyMap::const_iterator itor=m.begin();
    itor != m.end(); itor++) {

    // Write a coma if this is not the first maplet
    if(itor != m.begin()) {
      os << ", ";
    }

    os <<
      "("
      "id=" << itor->first <<
      ", name='" << itor->second.tapePoolName << "'"
      ", nbRunningStreams=" << itor->second.nbRunningStreams <<
      ")";
  }
}
