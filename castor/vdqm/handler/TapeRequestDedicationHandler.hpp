/******************************************************************************
 *                      TapeRequestDedicationHandler.cpp
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
 * @(#)RCSfile: TapeRequestDedicationHandler.cpp  Revision: 1.0  Release Date: Jul 18, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _TAPEREQUESTDEDICATIONHANDLER_HPP_
#define _TAPEREQUESTDEDICATIONHANDLER_HPP_

#include "BaseRequestHandler.hpp"

typedef struct vdqmdDrvReq vdqmDrvReq_t;

namespace castor {

  namespace vdqm {

		namespace handler {
	    /**
	     * This class looks through the tapeRequests and tries to find the best
	     * order to handle the requests.
	     * Note, that this class is a Singleton and is running in parallel during 
	     * the whole lifetime of the vdqm daemon.
	     */
	    class TapeRequestDedicationHandler : public BaseRequestHandler {
	
				public:
					/**
					 * Specifies in which intervalls the loop should be executed.
					 * The amount of this value should not be more than 1000000!
					 */				
					static unsigned const int m_sleepMilliTime;
				
					static TapeRequestDedicationHandler* Instance() 
						throw(castor::exception::Exception);
					
		      /**
		       * Destructor
		       */
					virtual ~TapeRequestDedicationHandler() throw();
					
					/**
					 * function to start the loop, which handles the dedication of a
					 * tape to a tape Drive
					 */
					void run();
					

					/**
					 * function to stop the run() loop
					 */
					void stop();					
					
					
				protected:
				
		      /**
		       * Constructor. 
		       * Please use Instance() to get an instance of this class.
		       * 
					 * @exception In case of error
		       */
					TapeRequestDedicationHandler() throw(castor::exception::Exception);				
					
				
				private:
				
					/**
					 * The Singleton instance
					 */
					static TapeRequestDedicationHandler* _instance;
	
					/**
					 * The run function stops, if this variable is set to false
					 */
					bool _stopLoop;
					
	     		/**
	     		 * This method is used to look, if they are free drives for the queued
	     		 * tape requests. 
	     		 */
	     		void handleTapeRequestQueue() 
						throw (castor::exception::Exception);					
												
	    }; // class TapeRequestDedicationHandler
    
  	} // end of namespace handler

  } // end of namespace vdqm

} // end of namespace castor

#endif //_TAPEREQUESTDEDICATIONHANDLER_HPP_
