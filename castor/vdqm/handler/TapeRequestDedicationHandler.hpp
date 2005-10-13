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

namespace castor {

  namespace vdqm {
  	
  	// Forward declaration
  	class TapeDrive;
  	class TapeRequest;

		namespace handler {
			
	  	/**
		   * Static method used to pass to Cpool_assign
		   */
		  void *staticDedicationRequest(void *param);
			
	    /**
	     * This class looks through the tapeRequests and tries to find the best
	     * order to handle the requests.
	     * Note, that this class is a Singleton and is running in parallel during 
	     * the whole lifetime of the vdqm daemon.
	     */
	    class TapeRequestDedicationHandler : public BaseRequestHandler {
	
				public:
					/**
					 * Specifies, how many seconds the thread should sleep,
					 * when it didn't find a free tapeDrive 
					 */				
					static unsigned const int m_sleepTime;
				
					/**
					 * Static method to get the Singleton instance of this class
					 * 
					 * @param dedicationThreadNumber The number of threads in the thread
					 * pool. This value is only important at the first call. If you want to 
					 * take the default value, just give 0 as parameter.
					 */
					static TapeRequestDedicationHandler* Instance(
						int dedicationThreadNumber) throw (castor::exception::Exception);
					
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
					
					/**
			     * Assigns work to a thread from the pool
			     */    
			    int threadAssign(void *param);	
			    
			    /**
		       * Method called once per found pair of fitting tape and tape request
		       * in an extra thread.
		       */
		      virtual void *dedicationRequest(void *param) throw();				
					
					
				protected:
				
		      /**
		       * Constructor. 
		       * Please use Instance() to get an instance of this class.
		       * 
		       * @param dedicationThreadNumber The number of threads in the thread
					 * pool. If you want to take the default value, just give 0 as 
					 * parameter.
					 * @exception In case of error
		       */
					TapeRequestDedicationHandler(int dedicationThreadNumber)
						throw (castor::exception::Exception);				
					
				
				private:
				
					/**
					 * The Singleton instance
					 */
					static TapeRequestDedicationHandler* _instance;
					
			    /**
			     * default number of threads in the thread pool
			     */    
			    static const int DEFAULT_THREAD_NUMBER = 20;
	
					/**
					 * The run function stops, if this variable is set to false
					 */
					bool _stopLoop;
					
					/**
			     * The id of the pool created
			     */
			    int m_threadPoolId;
			    
			    /**
			     * Number of threads in the tape to tape drive dedication pool
			     */
			    int m_dedicationThreadNumber;		
			    			
					
	     		/**
	     		 * This method is used to handle the dedication of a tape request to
	     		 * a tape drive. It will update the status in the db and inform the
	     		 * RTCPD about the dedication.
	     		 * 
	     		 * @param freeTapeDrive The tape drive which has been chosen by the 
	     		 * db select statement
	     		 * @param waitingTapeRequest An unhandled tape request, which fits the
	     		 * best for the free tape drive
	     		 * @exception In case of errors
	     		 */
	     		void handleDedication(
	     			castor::vdqm::TapeDrive* freeTapeDrive, 
	     			castor::vdqm::TapeRequest* waitingTapeRequest) 
						throw (castor::exception::Exception);
						
					
					/**
					 * Creates the thread pool for the tape to tape drive dedication. The
					 * amount of threads can be specified by the server start parameter -d
					 * 
					 * @exception In case of errors
					 */
					void createThreadPool() throw (castor::exception::Exception);
					
					
					/**
					 * Rollback of the dedication in the database
					 * 
					 * @param freeTapeDrive The tape drive which has been chosen by the 
	     		 * db select statement and which has a reference to its dedicated 
	     		 * tapeRequest
					 * @exception In case of errors
					 */
					void rollback(castor::vdqm::TapeDrive* freeTapeDrive)
						throw (castor::exception::Exception);
						
					
					/**
					 * Free the memory for this thread
					 * 
					 * @param freeTapeDrive The tape drive which has been chosen by the 
	     		 * db select statement
	     		 * @param waitingTapeRequest An unhandled tape request, which fits the
	     		 * best for the free tape drive
	     		 * @exception In case of errors
					 */
					void freeMemory(castor::vdqm::TapeDrive* freeTapeDrive,
					  castor::vdqm::TapeRequest* waitingTapeRequest) 
						throw (castor::exception::Exception);
	    }; // class TapeRequestDedicationHandler
    
  	} // end of namespace handler

  } // end of namespace vdqm

} // end of namespace castor


/**
 * Structure used to pass arguments to the method through Cpool_assign
 */
struct dedicationRequestArgs {
  castor::vdqm::handler::TapeRequestDedicationHandler *handler;
  void *param;
};

#endif //_TAPEREQUESTDEDICATIONHANDLER_HPP_
