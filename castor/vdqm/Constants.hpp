/******************************************************************************
 *                      Constants.hpp
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
 * @author castor dev team
 *****************************************************************************/

#ifndef CASTOR_VDQM_CONSTANTS_HPP
#define CASTOR_VDQM_CONSTANTS_HPP 1


namespace castor { namespace vdqm {
  /**
   * The schema version with which this release of the VDQM is compatible with.
   */
  const std::string VDQMSCHEMAVERSION = "2_1_12_0";

  /**
   * The schema version with which this release of the VDQM is compatible with.
   */
  const std::string VDQMORACONFIGFILE = "/etc/castor/ORAVDQMCONFIG";

  /**
   * Default number of request handler threads.
   */
  const int REQUESTHANDLERDEFAULTTHREADNUMBER = 20;

  /**
   * Default number of remote tape copy job submitter threads.
   */
  const int RTCPJOBSUBMITTERDEFAULTTHREADNUMBER = 1;

  /**
   * Default number of scheduler threads.
   */
  const int SCHEDULERDEFAULTTHREADNUMBER = 1;

  /**
   * Default VDQM port number.
   */
  const int VDQMPORT = 5012;

  /**
   * Default VDQM secure port number.
   */
  const int SVDQMPORT = 5512;

  /**
   * Default VDQM UDP notification port number.
   */
  const int VDQMNOTIFYPORT = 5012;

  /**
   * Default scheduler timeout, in other words the time a scheduler thread will
   * sleep when there is no work to be done.
   */
  const int SCHEDULERTIMEOUT = 10;

  /**
   * Default RTCP job submitter timeout, in other words the time an RTCP job
   * submitter thread will sleep when there is no work to be done.
   */
  const int RTCPJOBSUBMITTERTIMEOUT = 10;
} } // namespace vdqm - namespace castor


#endif // CASTOR_VDQM_CONSTANTS_HPP
