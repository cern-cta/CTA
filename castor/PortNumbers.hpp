/******************************************************************************
 *                      castor/PortNumbers.hpp
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
 * @author Castor dev team
 *****************************************************************************/

#ifndef CASTOR_PORTNUMBERS
#define CASTOR_PORTNUMBERS

namespace castor {

  /**
   * The default port on which the tape aggregator listens for connections from
   * the VDQM.
   */
  static const unsigned int AGGREGATOR_VDQMPORT = 5070;

  /**
   * The default inclusive low port of the aggregator client callback port
   * number range.
   */
  const unsigned short AGGREGATORCLIENT_LOWPORT = 30201;

  /**
   * The default inclusive high port of the aggregator client callback port
   * number range.
   */
  const unsigned short AGGREGATORCLIENT_HIGHPORT = 30300;

  /**
   * The default inclusive low port of the aggregator's RTCPD callback port
   * number range.
   */
  const unsigned short AGGREGATORRTCPD_LOWPORT = 30101;

  /**
   * The default inclusive high port of the aggregator's RTCPD callback port
   * number range.
   */
  const unsigned short AGGREGATORRTCPD_HIGHPORT = 30200;

  /**
   * The default port on which the job manager listenes for notifications.
   */
  static const int JOBMANAGER_DEFAULT_NOTIFYPORT = 15011;

  /**
   * The default port on which rtcpclientd listens for notifications.
   */
  static const int RTCPCLIENTD_DEFAULT_NOTIFYPORT = 5050;  // for future use

  /**
   * The default port on which the stager listens for notifications.
   */
  static const int STAGER_DEFAULT_NOTIFYPORT = 55015;

  /**
   * The default port on which tapegateway listens for notifications.
   */
  static const int TAPEGATEWAY_DEFAULT_NOTIFYPORT = 62801;  // for future use

} // namespace castor

#endif // CASTOR_PORTNUMBERS
