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
 * @author castor dev team
 *****************************************************************************/

#pragma once

#include "castor/BaseObject.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"


namespace castor {

  // Forward declarations
  class IObject;
  class Services;
  
  namespace vdqm {
    
    /**
     * The DatabaseHelper class provides a set of helper methods for working
     * with VDQM database objects.
     *
     * The majority of the code in this class was copied and pasted from the
     * castor::vdqm::handler::BaseRequestHandler class written by Matthias
     * Braeger.
     */
    class DatabaseHelper : public castor::BaseObject {
  
    public:

      /**
       * Stores the IObject into the data base. Please edit this function and
       * add here for your concrete Class instance your concrete Objects,
       * which you want to have created.
       * @param fr the request
       * @param cuuid its uuid (for logging purposes only)
       */
      static void storeRepresentation(castor::IObject *const fr, Cuuid_t cuuid)
        ;

      /**
       * Deletes the IObject in the data base. Please edit this function and 
       * add here for your concrete Class instance your concrete Objects, 
       * which you want to have deleted.
       * @param fr The Object, with the ID of the row, which should be deleted
       * @param cuuid its uuid (for logging purposes only)
       */
      static void deleteRepresentation(castor::IObject* fr, Cuuid_t cuuid)
        ;
        
      /**
       * Updates the IObject representation in the data base. Please edit this
       * function and add here for your concrete Class instance your concrete
       * Objects, which you want to have deleted.
       * @param fr The Object, with the ID of the row, which should be deleted
       * @param cuuid its uuid (for logging purposes only)
       */
      static void updateRepresentation(castor::IObject* fr, Cuuid_t cuuid)
        ;          


    private:

      /**
       * Private constructor to prohibit the instantiation of DatabaseHelper
       * objects.
       */
      DatabaseHelper();
  
    }; // class DatabaseHelper
    
  } // namespace vdqm

} // namespace castor

