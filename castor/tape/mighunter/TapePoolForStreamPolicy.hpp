/******************************************************************************
 *                      castor/tape/mighunter/TapePoolForStreamPolicy.hpp
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

#ifndef CASTOR_TAPE_MIGHUNTER_TAPEPOOLFORSTREAMPOLICY_HPP
#define CASTOR_TAPE_MIGHUNTER_TAPEPOOLFORSTREAMPOLICY_HPP

#include "h/osdep.h"

#include <string>

namespace castor    {
namespace tape      {
namespace mighunter {

/**
 * The name of a tape-pool and its number of running streams to be used by the
 * stream-policy of a service-class.
 */
struct TapePoolForStreamPolicy {
  std::string name;
  u_signed64  nbRunningStreams;
}; // struct TapePoolForStreamPolicy

} // namespace mighunter
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_MIGHUNTER_TAPEPOOLFORSTREAMPOLICY_HPP
