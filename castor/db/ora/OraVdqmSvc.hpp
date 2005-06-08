/******************************************************************************
 *                      OraVdqmSvc.hpp
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
 * @(#)RCSfile: OraVdqmSvc.hpp  Revision: 1.0  Release Date: May 31, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#ifndef _ORAVDQMSVC_HPP_
#define _ORAVDQMSVC_HPP_

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/ora/OraBaseObj.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "occi.h"

namespace castor {
	
  // Forward declaration
	class stager::Tape;
  class vdqm::ExtendedDeviceGroup;
	class vdqm::TapeRequest;
	class vdqm::TapeDrive;
	class vdqm::TapeServer;
	
	namespace db {
		
		namespace ora {
			
      /**
       * Implementation of the IVdqmSvc for Oracle
       */
      class OraVdqmSvc : public BaseSvc,
                           public OraBaseObj,
                           public virtual castor::vdqm::IVdqmSvc {

	    	public:
	    	
	       /**
	         * default constructor
	         */
	        OraVdqmSvc(const std::string name);
	
	        /**
	         * default destructor
	         */
	        virtual ~OraVdqmSvc() throw();
	
	        /**
	         * Get the service id
	         */
	        virtual inline const unsigned int id() const;
	
	        /**
	         * Get the service id
	         */
	        static const unsigned int ID();
	
	        /**
	         * Reset the converter statements.
	         */
	        void reset() throw ();
	    	
		    	/**
		    	 * Checks, if there is an entry in the ExtendedDeviceGroup table,
		    	 * which has exactly these parameters
		    	 * @return true, if the entry exists
		    	 * @exception in case of error
		    	 */
		    	virtual bool checkExtDevGroup(
		    		const castor::vdqm::ExtendedDeviceGroup *extDevGrp) 
		    		throw (castor::exception::Exception);
		    		
			    /**
		    	 * Looks in the data base for the TapeServer, which has exactly 
		    	 * this name. If there is no entry in the db table with this parameters,
		    	 * the return value is NULL
		    	 * @parameter serverName The name of the tape server, which is beeing requested
		    	 * @return The object representation of the table entry or NULL
		    	 * @exception in case of error
		    	 */
		    	virtual castor::vdqm::TapeServer* selectTapeServer(
		    		const std::string serverName)
		    		throw (castor::exception::Exception);
		    		
		    		
		    	/**
		    	 * Checks, if there is already an entry for that tapeRequest. The entry
		    	 * must have exactly these parameters. Only the assoziation to the
		    	 * tapeDrive is not checked.
		    	 * @return true, if there is already an entry for the request 
		    	 * @exception in case of error
		    	 */
		    	virtual bool checkTapeRequest(
		    		const castor::vdqm::TapeRequest *tapeRequest)
		    		throw (castor::exception::Exception);
		    	
		    	
		    	/**
		    	 * Looks for the best fitting tape drive. If it is for example an
		    	 * older tape, it will first look if an older drive is free, before
		    	 * it chooses a newer one. This strategy should avoid, that the newer
		    	 * drive, which are able to deal with several tape models, are blocked
		    	 * if an request for a newer tape model arrives.
		    	 * Please notice that caller is responsible for deleting the object.
		    	 * @parameter the requested Extended Device Group for the tape
		    	 * @return the free TapeDrive or NULL if there is none.
		    	 * @exception in case of error
		    	 */	
		    	virtual castor::vdqm::TapeDrive* selectFreeTapeDrive(
		    		const castor::vdqm::ExtendedDeviceGroup *extDevGrp) 
		    		throw (castor::exception::Exception);    
		    		
		    		
	      private:
	
	        /// SQL statement for function checkExtDevGroup
	        static const std::string s_checkExtDevGroupStatementString;
	
	        /// SQL statement object for function checkExtDevGroup
	        oracle::occi::Statement *m_checkExtDevGroupStatement;  
	        
	        /// SQL statement for function getTapeServer
	        static const std::string s_selectTapeServerStatementString;
	
	        /// SQL statement object for function getTapeServer
	        oracle::occi::Statement *m_selectTapeServerStatement; 
	        
 	        /// SQL statement for function checkTapeRequest
	        static const std::string s_checkTapeRequestStatementString;
	
	        /// SQL statement object for function checkTapeRequest
	        oracle::occi::Statement *m_checkTapeRequestStatement; 
	        
 	        /// SQL statement for function getFreeTapeDrive
	        static const std::string s_selectFreeTapeDriveStatementString;
	
	        /// SQL statement object for function getFreeTapeDrive
	        oracle::occi::Statement *m_selectFreeTapeDriveStatement;	                          	
			};
		}
	}
}

#endif //_ORAVDQMSVC_HPP_
