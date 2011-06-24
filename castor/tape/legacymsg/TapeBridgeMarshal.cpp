/******************************************************************************
 *                      castor/tape/legacymsg/TapeBridgeMarshal.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/TapeBridgeMarshal.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <errno.h>
#include <iostream>
#include <string.h>


//-----------------------------------------------------------------------------
// unmarshal
//-----------------------------------------------------------------------------
void castor::tape::legacymsg::unmarshal(const char * &src,
  size_t &srcLen, tapeBridgeFlushedToTapeMsgBody_t &dst)
  throw(castor::exception::Exception) {

  unmarshalUint32(src, srcLen, dst.volReqId);
  unmarshalUint32(src, srcLen, dst.tapeFseq);
}
