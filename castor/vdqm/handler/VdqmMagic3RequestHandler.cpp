/******************************************************************************
 *                      VdqmMagic3RequestHandler.hpp
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
#include "castor/io/ServerSocket.hpp"
#include "castor/vdqm/SocketHelper.hpp"
#include "castor/vdqm/VdqmDlfMessageConstants.hpp"
#include "castor/vdqm/handler/VdqmMagic3RequestHandler.hpp"
#include "h/Cupv_api.h"
#include "h/vdqm_constants.h"

#include <iostream>


void castor::vdqm::handler::VdqmMagic3RequestHandler::handleDelDrv(
  castor::io::ServerSocket &socket, const Cuuid_t &cuuid,
  vdqmDelDrv_t &msg) throw (castor::exception::Exception) {

  castor::dlf::Param param[] = {
    castor::dlf::Param("clientUID" , msg.clientUID),
    castor::dlf::Param("clientGID" , msg.clientGID),
    castor::dlf::Param("clientHost", msg.clientHost),
    castor::dlf::Param("server"    , msg.server),
    castor::dlf::Param("drive"     , msg.drive),
    castor::dlf::Param("dgn"       , msg.dgn)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, VDQM_HANDLE_VDQM3_DEL_DRV, 6,
    param);

  SocketHelper::checkCupvPermissions(socket, msg.clientUID, msg.clientGID,
    P_TAPE_OPERATOR, "P_TAPE_OPERATOR", "VDQM_DEL_DRVREQ");

  ptr_IVdqmService->deleteDrive(msg.drive, msg.server, msg.dgn);
}


void castor::vdqm::handler::VdqmMagic3RequestHandler::handleDedicate(
  castor::io::ServerSocket &socket, const Cuuid_t &cuuid, vdqmDedicate_t &msg)
  throw (castor::exception::Exception) {
  castor::dlf::Param param[] = {
    castor::dlf::Param("clientUID" , msg.clientUID),
    castor::dlf::Param("clientGID" , msg.clientGID),
    castor::dlf::Param("clientHost", msg.clientHost),
    castor::dlf::Param("server"    , msg.server),
    castor::dlf::Param("drive"     , msg.drive),
    castor::dlf::Param("dgn"       , msg.dgn),
    castor::dlf::Param("dedicate"  , msg.dedicate)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, VDQM_HANDLE_VDQM3_DEDICATE, 7,
    param);

  SocketHelper::checkCupvPermissions(socket, msg.clientUID, msg.clientGID,
    P_TAPE_OPERATOR, "P_TAPE_OPERATOR", "VDQM_DEDICATE_DRV");

  ptr_IVdqmService->dedicateDrive(msg.drive, msg.server, msg.dgn, msg.dedicate);
}
