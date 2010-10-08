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

#include "castor/BaseObject.hpp"
#include "castor/server/IThread.hpp"

namespace castor {

  namespace vdqm {

    // Forward declarations
    class IVdqmSvc;
    class TapeDrive;
    class TapeRequest;


    /**
     * Allocates free tape drives to waiting tape requests.  This class also
     * has the secondary role of deleting old volume priorities.
     */
    class DriveSchedulerThread : public virtual castor::server::IThread,
                                 public castor::BaseObject {

    public:

      /**
       * Initialization of the thread.
       */
      virtual void init() {}
    
      /**
       * Run the tape drive scheduling algorithm, plus the secondary activity
       * of deleting old volume priorities.
       */
      virtual void run(void *param);

      /**
       * Stop of the thread
       */
      virtual void stop() {}

    private:

      /**
       * The default maximum age of a volume priority in seconds.  This
       * default is overruled by a "VDQM MAXVOLPRIORITYAGE" entry in the
       * castor configuration file, e.g. /etc/castor/castor.conf
       */
      static const unsigned int s_maxVolPriorityAge = 86400; // 24 hours

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
