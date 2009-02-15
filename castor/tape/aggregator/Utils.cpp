/******************************************************************************
 *                      Utils.cpp
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

#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/Utils.hpp"
#include "h/rtcp_constants.h"

#include <arpa/inet.h>
#include <sys/socket.h>


//-----------------------------------------------------------------------------
// copyString
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Utils::copyString(char *const dst,
  const char *src, const size_t n) throw(castor::exception::Exception) {

  const size_t srcLen = strlen(src);

  if(srcLen >= n) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Source string is longer than destination.  Source length: "
      << srcLen << " Max destination length: " << (n-1);

    throw ex;
  }

  strncpy(dst, src, n);
    *(dst+n-1) = '\0'; // Ensure destination is null terminated
}


//-----------------------------------------------------------------------------
// copyString
//-----------------------------------------------------------------------------
void castor::tape::aggregator::Utils::copyString(char *const dst,
  const std::string &src) throw(castor::exception::Exception) {

  copyString(dst, src.c_str());
}


//-----------------------------------------------------------------------------
// magicToStr
//-----------------------------------------------------------------------------
const char *castor::tape::aggregator::Utils::magicToStr(const uint32_t magic) {
  switch(magic) {
  case RTCOPY_MAGIC_VERYOLD: return "RTCOPY_MAGIC_VERYOLD";
  case RTCOPY_MAGIC_SHIFT  : return "RTCOPY_MAGIC_SHIFT";
  case RTCOPY_MAGIC_OLD0   : return "RTCOPY_MAGIC_OLD0";
  case RTCOPY_MAGIC        : return "RTCOPY_MAGIC";
  case RFIO2TPREAD_MAGIC   : return "RFIO2TPREAD_MAGIC";
  default                  : return "UNKNOWN";
  }
}


//-----------------------------------------------------------------------------
// rtcopyReqTypeToStr
//-----------------------------------------------------------------------------
const char *castor::tape::aggregator::Utils::rtcopyReqTypeToStr(
  const uint32_t reqType) {
  switch(reqType) {
  case RTCP_TAPE_REQ     : return "RTCP_TAPE_REQ";
  case RTCP_FILE_REQ     : return "RTCP_FILE_REQ";
  case RTCP_NOMORE_REQ   : return "RTCP_NOMORE_REQ";
  case RTCP_TAPEERR_REQ  : return "RTCP_TAPEERR_REQ";
  case RTCP_FILEERR_REQ  : return "RTCP_FILEERR_REQ";
  case RTCP_ENDOF_REQ    : return "RTCP_ENDOF_REQ";
  case RTCP_ABORT_REQ    : return "RTCP_ABORT_REQ";
  case RTCP_DUMP_REQ     : return "RTCP_DUMP_REQ";
  case RTCP_DUMPTAPE_REQ : return "RTCP_DUMPTAPE_REQ";
  case RTCP_KILLJID_REQ  : return "RTCP_KILLJID_REQ";
  case RTCP_RSLCT_REQ    : return "RTCP_RSLCT_REQ";
  case RTCP_PING_REQ     : return "RTCP_PING_REQ";
  case RTCP_HAS_MORE_WORK: return "RTCP_HAS_MORE_WORK";
  default                : return "UNKNOWN";
  }
}


//-----------------------------------------------------------------------------
// procStatusToStr
//-----------------------------------------------------------------------------
const char *castor::tape::aggregator::Utils::procStatusToStr(
  const uint32_t procStatus) {
  switch(procStatus) {
  case RTCP_WAITING           : return "RTCP_WAITING";
  case RTCP_POSITIONED        : return "RTCP_POSITIONED";
  case RTCP_PARTIALLY_FINISHED: return "RTCP_PARTIALLY_FINISHED";
  case RTCP_FINISHED          : return "RTCP_FINISHED";
  case RTCP_EOV_HIT           : return "RTCP_EOV_HIT";
  case RTCP_UNREACHABLE       : return "RTCP_UNREACHABLE";
  case RTCP_REQUEST_MORE_WORK : return "RTCP_REQUEST_MORE_WORK";
  default                     : return "UNKNOWN";
  }
}
