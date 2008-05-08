/******************************************************************************
 *                      VdqmMagic2RequestFacade.hpp
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
#include "castor/vdqm/VdqmMagic2RequestFacade.hpp"
#include "h/vdqm_constants.h"


void castor::vdqm::VdqmMagic2RequestFacade::checkRequestType(
  const int reqType) throw (castor::exception::Exception) {

  if (!VDQM2_VALID_REQTYPE(reqType)) {
    castor::exception::NotSupported ex;
    ex.getMessage() << "Invalid Request 0x" << std::hex << reqType << std::dec
      << std::endl;
    throw ex;
  }
}
