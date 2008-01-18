/******************************************************************************
 *                castor/vdqm/DriveSchedulerThread.hpp
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

#ifndef CASTOR_VDQM_DRIVESCHEDULERTHREAD_HPP
#define CASTOR_VDQM_DRIVESCHEDULERTHREAD_HPP 1

#include "castor/server/SelectProcessThread.hpp"


namespace castor {

  namespace vdqm {

    // Forward declarations
    class IVdqmSvc;
    class TapeDrive;
    class TapeRequest;


    /**
     * Allocates free tape drives to tape requests.
     */
    class DriveSchedulerThread :
    public virtual castor::server::SelectProcessThread {

    public:

      /**
       * Constructor
       */
      DriveSchedulerThread() throw();

      /**
       * Destructor
       */
      ~DriveSchedulerThread() throw();

      /**
       * Performs the select query
       */
      virtual castor::IObject* select() throw();

      /**
       * Processes the results of the select.
       *
       * @param param The IObject returned by select
       */
      virtual void process(castor::IObject* param) throw();


    private:

      /**
       * Contains the old VDQM2 code which starts the old tape dedication
       * (drive scheduling) threads.
       */
      void startOLDDriveSchedulerThreads() throw(castor::exception::Exception);

      /**
       * Allocates the already associated free drive of the specified tape
       * request to the tape request.
       */
      void allocateDrive(castor::vdqm::TapeRequest* request)
        throw(castor::exception::Exception);

      /**
       * Returns a pointer to the DbVdqmSvc or throws an exception if it cannot.
       *
       * Please note that this method never returns NULL.  The method returns a
       * non-zero pointer or it throws an exception.
       */
      castor::vdqm::IVdqmSvc *getDbVdqmSvc()
        throw(castor::exception::Exception);

    }; // class DriveSchedulerThread

  } // end namespace vdqm

} //end namespace castor

#endif // CASTOR_VDQM_DRIVESCHEDULERTHREAD_HPP
