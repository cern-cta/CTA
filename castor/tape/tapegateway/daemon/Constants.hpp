/******************************************************************************
 *              castor/tape/tapegateway/daemon/Constants.hpp
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
 * @author Giulia Taurelli
 *****************************************************************************/


#ifndef GATEWAY_CONSTANTS_HPP
#define GATEWAY_CONSTANTS_HPP 1


namespace castor      {
namespace tape        {
namespace tapegateway {

  /**
   * The default time in seconds between two execution of threads
   */

  const uint64_t  DEFAULT_SLEEP_INTERVAL=10;

  /**
   * The default time in seconds between two polls on a VDQM request
   */

  const uint64_t VDQM_TIME_OUT_INTERVAL=600;
  
  /**
   * Default parameters to initialize the Dynamic Thread pool
   */

  const uint64_t MIN_WORKER_THREADS = 20;
  const uint64_t MAX_WORKER_THREADS = 20;
  const uint64_t TG_THRESHOLD = 50;
  const uint64_t TG_MAXTASKS = 50;


} // namespace tapegateway
} // namespace tape
} // namespace castor

#endif // GATEWAY_CONSTANTS_HPP
