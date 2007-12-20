/******************************************************************************
 *                castor/vdqm/DriveDedicationThread.hpp
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

#ifndef CASTOR_VDQM_DRIVEDEDICATIONTHREAD_HPP
#define CASTOR_VDQM_DRIVEDEDICATIONTHREAD_HPP 1

#include "castor/server/SelectProcessThread.hpp"


namespace castor {

  namespace vdqm {

    // Forward declarations
    class IVdqmSvc;


    /**
     * Handles the dedication of tape drives.
     */
    class DriveDedicationThread :
    public virtual castor::server::SelectProcessThread {

    public:

      /**
       * Constructor
       */
      DriveDedicationThread() throw();

      /**
       * Destructor
       */
      ~DriveDedicationThread() throw();

      /**
       * Performs the select query
       */
      virtual castor::IObject* select() throw();

      /**
       * Processes the results of the select.  This method allocates the
       * required resources to do the work, then delegates the work to
       * processWork, and finally cleans up independent of whether or not
       * processWork raised an exception.
       *
       * @param param The IObject returned by select
       */
      virtual void process(castor::IObject* param) throw();


    private:

      /**
       * The process method delegates the actual work to be done to this method
       * and then cleans up after this method has returned or has thrown an
       * exception.
       */
      void processWork(castor::IObject* param)
        throw(castor::exception::Exception);

      /**
       * Returns a pointer to the DbVdqmSvc or throws an exception if it cannot.
       *
       * Please note that this method never returns NULL.  The method returns a
       * non-zero pointer or it throws an exception.
       */
      castor::vdqm::IVdqmSvc *getDbVdqmSvc()
        throw(castor::exception::Exception);

    }; // class DriveDedicationThread

  } // end namespace vdqm

} //end namespace castor

#endif // CASTOR_VDQM_DRIVEDEDICATIONTHREAD_HPP
