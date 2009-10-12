/******************************************************************************
 *                      castor/tape/legacymsg/RtcpFileRqstErrMsgBody.hpp
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

#ifndef CASTOR_TAPE_LEGACYMSG_RTCPFILERQSTERRMSGBODY
#define CASTOR_TAPE_LEGACYMSG_RTCPFILERQSTERRMSGBODY

#include "castor/tape/legacymsg/RtcpErrorAppendix.hpp"
#include "castor/tape/legacymsg/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpSegmentAttributes.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <stdint.h>


namespace castor     {
namespace tape       {
namespace legacymsg {

  /**
   * An RTCP file request with error appendix message.
   *
   * Please note that the presence of an error appendix does not necessarily
   * indicate an error.
   */
  struct RtcpFileRqstErrMsgBody : public RtcpFileRqstMsgBody {
    RtcpErrorAppendix err; // Error reporting
  }; // struct RtcpFileRqstErrMsgBody

} // namespace legacymsg
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_LEGACYMSG_RTCPFILERQSTERRMSGBODY
