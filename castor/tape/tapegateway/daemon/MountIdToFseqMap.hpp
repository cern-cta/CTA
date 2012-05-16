/******************************************************************************
 *                castor/tape/tapegateway/daemon/MountIdToFseqMap.hpp
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

#ifndef CASTOR_TAPE_TAPEGATEWAY_DAEMON_MOUNTidTOFSEQMAP_HPP
#define CASTOR_TAPE_TAPEGATEWAY_DAEMON_MOUNTidTOFSEQMAP_HPP 1

#include "castor/exception/Exception.hpp"

#include <map>
#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>


namespace castor      {
namespace tape        {
namespace tapegateway {

/* XXX seemingly unused */

/**
 * Thread-safe map of mount transaction ids to tape file sequence numbers.
 */
class MountIdToFseqMap {

private:

  /**
   * Mutex used to make this class thread-safe.
   */
  pthread_mutex_t m_mutex;

  /**
   * Data type for a map of mount transaction ids to tape file sequence numbers.
   */
  typedef std::map<uint64_t, int32_t> Map;

  /**
   * Map of mount transaction ids to tape file sequence numbers.
   */
  Map m_map;

public:

  /**
   * Constructor
   */
  MountIdToFseqMap() throw(castor::exception::Exception);

  /**
   * Inserts the specified mount transaction id to file sequence number mapping.
   */
  void insert(const uint64_t mountTransactionId, const int32_t fseq)
    throw(castor::exception::Exception);

  /**
   * Returns the next FSEQ for the specified mount transaction id.
   */
  int32_t nextFseq(const uint64_t mountTransactionId)
    throw(castor::exception::Exception);

  /**
   * Erases the specified mount transaction id and its associated FSEQ from
   * this map.
   */
  void erase(const uint64_t mountTransactionId)
    throw(castor::exception::Exception);
};

} // namespace tapegateway
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEGATEWAY_DAEMON_MOUNTidTOFSEQMAP_HPP
