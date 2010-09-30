/******************************************************************************
 *                      castor/tape/mighunter/TapePoolForStreamPolicyMap.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_MIGHUNTER_IDTOTAPEPOOLFORSTREAMPOLICYMAP_HPP
#define CASTOR_TAPE_MIGHUNTER_IDTOTAPEPOOLFORSTREAMPOLICYMAP_HPP

#include "castor/tape/mighunter/TapePoolForStreamPolicy.hpp"
#include "h/osdep.h"

#include <iostream>
#include <map>

namespace castor    {
namespace tape      {
namespace mighunter {

/**
 * Map of tape-pool database IDs to the tape-pool name and number of running
 * streams.
 */
class IdToTapePoolForStreamPolicyMap:
  public std::map<u_signed64, TapePoolForStreamPolicy> {

public:

  /**
   * Returns the total number of running streams recorded in this map.
   *
   * @return The total number of running-streams.
   */
  u_signed64 getTotalNbRunningStreams() const throw();

}; // class IdToTapePoolForStreamPolicyMap


/**
 * Writes to the specified output stream a string-represenation of the
 * specified map of tape-pool database IDs to the tape-pool name and number ofi
 * running streams.
 *
 * @param os The outpu-stream to be written to.
 * @param m  The map to be written to the output-stream.
 */
void writeIdToTapePoolForStreamPolicyMapToString(
  std::ostream &os, const IdToTapePoolForStreamPolicyMap &m);

} // namespace mighunter
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_MIGHUNTER_IDTOTAPEPOOLFORSTREAMPOLICYMAP_HPP
