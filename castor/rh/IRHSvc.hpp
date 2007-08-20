/******************************************************************************
 *                      IRHSvc.hpp
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
 * @(#)$RCSfile: IRHSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/08/20 10:23:44 $ $Author: sponcec3 $
 *
 * This class provides specific request handler methods
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef RH_IRHSVC_HPP 
#define RH_IRHSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include <string>

namespace castor {

  // Forward declaration

  namespace rh {

    /**
     * This class provides specific request handler methods
     */
    class IRHSvc : public virtual castor::stager::ICommonSvc {

    public:

      /**
       * checks permission for a given user to make a give request
       * in a given service class. Just returns nothing in case
       * permission is granted and throw a permission denied
       * exception otherwise
       * @param svcClassName the name of the service class
       * @param euid the uid of the user
       * @param egid the gid of the user
       * @param type the type of request
       * @exception throws Exception in case of permission denial
       */
      virtual void checkPermission
      (const std::string svcClassName, unsigned long euid,
       unsigned long egid, int type)
        throw (castor::exception::Exception) = 0;

    }; // end of class IRHSvc

  } // end of namespace rh

} // end of namespace castor

#endif // RH_IRHSVC_HPP
