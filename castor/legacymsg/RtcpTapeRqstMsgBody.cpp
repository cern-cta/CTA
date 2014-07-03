/******************************************************************************
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/legacymsg/RtcpTapeRqstMsgBody.hpp"

#include <string.h>

castor::legacymsg::RtcpTapeRqstMsgBody::RtcpTapeRqstMsgBody() throw():
  volReqId(0),
  jobId(0),
  mode(0),
  start_file(0),
  end_file(0),
  side(0),
  tprc(0),
  tStartRequest(0),
  tEndRequest(0),
  tStartRtcpd(0),
  tStartMount(0),
  tEndMount(0),
  tStartUnmount(0),
  tEndUnmount(0),
  rtcpReqId(nullCuuid) {
  memset(vid, '\0', sizeof(vid));
  memset(vsn, '\0', sizeof(vsn));
  memset(label, '\0', sizeof(label));
  memset(devtype, '\0', sizeof(devtype));
  memset(density, '\0', sizeof(density));
  memset(unit, '\0', sizeof(unit));
}
