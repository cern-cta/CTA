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

#ifndef _VDQMMAGIC2REQUESTFACADE_HPP_
#define _VDQMMAGIC2REQUESTFACADE_HPP_ 1


#include "castor/exception/Exception.hpp"
#include "h/vdqm_messages.h"


namespace castor {

  namespace vdqm {

    /**
     * This class provides functions to handle VDQM messages with the magic
     * number VDQM_MAGIC2.
     */
    class VdqmMagic2RequestFacade {

    public:

      /**
       * Throws an exception if the specified request type is invalid.
       *
       * @param reqType The request type
       */
      static void checkRequestType(const int reqType)
        throw (castor::exception::Exception);

    }; // class VdqmMagic2RequestFacade

  } // namespace vdqm

} // namespace castor

#endif // _VDQMMAGIC2REQUESTFACADE_HPP_
