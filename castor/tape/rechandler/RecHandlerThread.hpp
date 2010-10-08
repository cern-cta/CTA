/******************************************************************************
*                      RecHandlerThread.hpp
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
* @(#)$RCSfile: RecHandlerThread.hpp,v $ $Author: gtaur $
*
*
*
* @author Giulia Taurelli
*****************************************************************************/

#ifndef RECHANDLERTHREAD_HPP
#define RECHANDLERTHREAD_HPP 1


// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

// Include files

#include "castor/tape/rechandler/RecallPolicyElement.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/IThread.hpp"


namespace castor    {
namespace tape      {
namespace rechandler{

    /**
     * RecHandler tread.
     * This thread cleans out the out of date failed requests and diskcopies.
     */
    
    class RecHandlerThread : public virtual castor::server::IThread,
                             public castor::BaseObject {
      
      PyObject* m_pyFunction; 
      bool applyRecallPolicy(const RecallPolicyElement& elem, const int nbMountsForRecall)
	throw (castor::exception::Exception );
    
    public:
      
      /**
       * constructor
       * @param maximum numbers of days that a request can stay in
       * the datebase before being deleted.
       */    

      RecHandlerThread(PyObject* pyFunction);

      /**
       * destructor
       */
      virtual ~RecHandlerThread() throw() {};
      
      /**
       * Initialization of the thread.
       */
      virtual void init() {}

      /*For thread management*/
      virtual void run(void*);
      
      /**
       * Stop of the thread
       */
      virtual void stop() {}

    };

} // end of namespace rechandler
} // end of namespace tape  
} // end of namespace castor

#endif // RECHANDLERTHREAD_HPP
