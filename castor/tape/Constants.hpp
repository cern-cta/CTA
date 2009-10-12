/******************************************************************************
 *                      castor/tape/Constants.hpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_CONSTANTS_HPP
#define CASTOR_TAPE_CONSTANTS_HPP 1

#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>


namespace castor {
namespace tape   {

  /**
   * The size in bytes of the error string buffer to be used with the strerror
   * family of commands.
   */
  const size_t STRERRORBUFLEN = 256;

  /**
   * The size in bytes of the VMGR error buffer.
   */
  const size_t VMGRERRORBUFLEN = 512;

  /**
   * The conservative umask to be used in the RTCOPY protocol when it makes no
   * sense to send one. The RTCOPY protocol sends umask information when
   * migrating files from disk to tape and when indicating that more files can
   * be requested by RTCPD.
   */
  const mode_t RTCOPYCONSERVATIVEUMASK = 077;

} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_CONSTANTS_HPP
