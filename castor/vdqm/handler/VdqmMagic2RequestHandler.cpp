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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/NotSupported.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/handler/VdqmMagic2RequestHandler.hpp"
#include "h/vdqm_constants.h"

#include <iostream>


void castor::vdqm::handler::VdqmMagic2RequestHandler::handleVolPriority(
  const Cuuid_t &cuuid, vdqmVolPriority_t &msg)
   {
  castor::dlf::Param param[] = {
    castor::dlf::Param("priority"    , msg.priority),
    castor::dlf::Param("clientUID"   , msg.clientUID),
    castor::dlf::Param("clientGID"   , msg.clientGID),
    castor::dlf::Param("clientHost"  , msg.clientHost),
    castor::dlf::Param("TPVID"         , msg.vid),
    castor::dlf::Param("tpMode"      , msg.tpMode),
    castor::dlf::Param("lifespanType", msg.lifespanType)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM,
    VDQM_HANDLE_VDQM2_VOL_PRIORITY, 7, param);

  ptr_IVdqmService->setVolPriority(msg.priority, msg.clientUID, msg.clientGID,
    msg.clientHost, msg.vid, msg.tpMode, msg.lifespanType);
}
