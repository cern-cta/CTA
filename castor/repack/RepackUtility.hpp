
/******************************************************************************
 *                       RepackUtility.hpp
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
 * @author Giulia Taurelli
 *****************************************************************************/


#ifndef REPACKUTILITY_HPP
#define REPACKUTILITY_HPP 1

#include <string>
#include "Cuuid.h"
#include "stager_client_api.h"
#include "stager_constants.h"
#include "RepackSubRequest.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {
	namespace repack {
    std::string Cuuidtostring(_Cuuid_t *cuuid);
    _Cuuid_t stringtoCuuid(std::string strcuuid);
    void free_stager_response(struct stage_filequery_resp* resp);
    void freeRepackObj(castor::IObject* obj);
    void getStageOpts(struct stage_options* opts, RepackSubRequest* sreq)
                                       throw (castor::exception::Exception);
  }
}
#endif
