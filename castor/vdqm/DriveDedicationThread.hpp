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
       * Select a new ... to be processed
       */
      virtual castor::IObject* select() throw();

      /**
       * Performs the selected query
       * @param param The IObject returned by select
       */
      virtual void process(castor::IObject* param) throw();

    }; // class DriveDedicationThread

  } // end namespace vdqm

} //end namespace castor

#endif // CASTOR_VDQM_DRIVEDEDICATIONTHREAD_HPP
