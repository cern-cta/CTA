/******************************************************************************
 *                      castor/tape/aggregator/TapeSessionCatalogue.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_TAPESESSIONCATALOGUE
#define CASTOR_TAPE_AGGREGATOR_TAPESESSIONCATALOGUE

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"

#include <map>
#include <stdint.h>
#include <string>
#include <pthread.h>


namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * Thread-safe catalogue of on-going tape-sessions, where a tape-session is the
 * data transfer activity applied to a tape during the time it is mounted.
 */
class TapeSessionCatalogue {

public:

  /**
   * Constructor.
   */
  TapeSessionCatalogue() throw(castor::exception::Exception);

  /**
   * Adds the specified tape session to the catalogue.
   *
   * This method throws an InvalidArgument exception if the specified tape
   * session has:
   * <ul>
   * <li>The same mount transactionId as an existing session.
   * <li>The same drive unit name as an existing session.
   * <li>The same mount transactionId and the same drive unit name as an
   *     existing session.
   * </ul>
   *
   * @param mountTransactionId The mount transaction ID of the tape session to
   *                           be removed.
   * @param driveName          The drive unit name.
   */
  void addTapeSession(const uint64_t mountTransactionId,
    const char *const driveName) throw(castor::exception::InvalidArgument);

  /**
   * Removes the specified tape session from the catalogue.
   *
   * This method throws an InvalidArgument exception if the specified tape
   * session does not exist in the tape session catalogue.
   *
   * @param mountTransactionId The mount transaction ID of the tape session to
   *                           be removed.
   */
  void removeTapeSession(const uint64_t mountTransactionId)
    throw(castor::exception::InvalidArgument);


private:

  /**
   * Mutex used to serialise access to the catalogue.
   */
  pthread_mutex_t m_mutex;

  /**
   * Datatype for a map of mount transaction ID to drive unit name
   */
  typedef std::map<uint64_t, std::string> TransIdToDriveNameMap;

  /**
   * Map of mount transaction ID to drive unit name
   */
  TransIdToDriveNameMap m_transIdToDriveNameMap;

};

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_TAPESESSIONCATALOGUE
