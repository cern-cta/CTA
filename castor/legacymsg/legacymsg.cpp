/******************************************************************************
 *         castor/legacymsg/legacymsg.cpp
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
 * @author steven.murray@cern.ch
 *****************************************************************************/

#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/legacymsg.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/GenericReplyMsgBody.hpp"
#include "h/Ctape.h"
#include "castor/utils/utils.hpp"
#include "GenericMarshal.hpp"

//------------------------------------------------------------------------------
// writeTapeRcReplyMsg
//------------------------------------------------------------------------------
void castor::legacymsg::writeTapeRcReplyMsg(const int timeout, const int fd, const int rc) {
  try {
    char dst[12];
    legacymsg::MessageHeader src;
    src.magic = TPMAGIC;
    src.reqType = TAPERC;
    src.lenOrStatus = rc;
    const size_t len = legacymsg::marshal(dst, src);
    castor::io::writeBytes(fd, timeout, len, dst);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write tape-label reply message with rc=" << rc
      << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// writeTapeReplyMsg
//------------------------------------------------------------------------------
void castor::legacymsg::writeTapeReplyMsg(const int timeout, const int fd, const int rc, const std::string &message) {
  try {    
    const int dstlen = 12+4+CA_MAXLINELEN+1; // 12 bytes of header + 4 bytes of return code + max length of error message
    char dst[dstlen];
    castor::legacymsg::GenericReplyMsgBody src;    
    src.status=rc;
    castor::utils::copyString(src.errorMessage, CA_MAXLINELEN+1, message.c_str());
    const size_t len = castor::legacymsg::marshal(dst, dstlen, TPMAGIC, MSG_DATA, src);    
    castor::io::writeBytes(fd, timeout, len, dst);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write tape-label reply message with rc=" << rc << " and error message=\"" << message << "\". "
      << ne.getMessage().str();
    throw ex;
  }
}