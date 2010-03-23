/******************************************************************************
 *                castor/repack/ora/OraRepackSvc.hpp
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: OraRepackSvc.hpp,v $ $Revision: 1.5 $ $Release$ $Date: 2009/02/05 15:51:20 $ $Author: gtaur $
 *
 *
 * @author castordev
 *****************************************************************************/

#ifndef ORA_ORAREPACKSVC_HPP
#define ORA_ORAREPACKSVC_HPP 

// Include Files

#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/db/newora/OraCommonSvc.hpp"
#include "occi.h"

#include "castor/BaseAddress.hpp"

#include <Cuuid.h>
#include <fcntl.h>

#include "castor/repack/IRepackSvc.hpp"




namespace castor {

  namespace repack {

    namespace ora {

      /**
       * Implementation of the IRepackSvc for Oracle
       */
      class OraRepackSvc : public castor::db::ora::OraCommonSvc, public virtual castor::repack::IRepackSvc {

      public:

        /**
         * default constructor
         */
        OraRepackSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraRepackSvc() throw();

        /**
         * Get the service id
         */
        virtual inline unsigned int id() const;

        /**
         * Get the service id
         */
        static unsigned int ID();

        /**
         * Reset the converter statements.
         */
        void reset() throw ();

	/**
	 * storeRequest 
	 */
	virtual castor::repack::RepackAck* storeRequest(castor::repack::RepackRequest* rreq)throw (castor::exception::Exception);

	/**
	 * updateSubRequest
	 */
	virtual void updateSubRequest(castor::repack::RepackSubRequest* obj) throw (castor::exception::Exception);
	
	/**
	 * updateSubRequestSegments
	 */
	virtual void updateSubRequestSegments(castor::repack::RepackSubRequest* obj, std::vector<RepackSegment*> listToUpdate) throw (castor::exception::Exception);
	
	/**
	 * insert SubRequestSegments
	 */
	virtual void insertSubRequestSegments(castor::repack::RepackSubRequest* obj) throw (castor::exception::Exception);
	
	/**
	 * getSubRequestByVid 
	 */
	virtual castor::repack::RepackResponse* getSubRequestByVid(std::string vid, bool fill) throw (castor::exception::Exception);
	
	/**
	 * getSubRequestsByStatus
	 */
	virtual std::vector<castor::repack::RepackSubRequest*> getSubRequestsByStatus(castor::repack::RepackSubRequestStatusCode st, bool fill) throw (castor::exception::Exception);
	
	/**
	 * getAllSubRequests
	 */
	virtual castor::repack::RepackAck *getAllSubRequests() throw (castor::exception::Exception);

	/**
	 * validateRepackSubrequest
	 */ 

	virtual bool  validateRepackSubRequest( RepackSubRequest* tape, int numFiles, int numTapes) throw (castor::exception::Exception);
	
	/**
	 * resurrectTapesOnHold
	 */ 

	virtual void  resurrectTapesOnHold(int numFiles, int numTapes) throw (castor::exception::Exception);

	/* 
	 * restartSubRequestTapes
	 */

	virtual void restartSubRequest(u_signed64 srId) throw (castor::exception::Exception);
	
	/**
	 * changeSubRequestsStatus
	 */
	virtual castor::repack::RepackAck* changeSubRequestsStatus(std::vector<castor::repack::RepackSubRequest*> srs, castor::repack::RepackSubRequestStatusCode st) throw (castor::exception::Exception);
	
	/**
	 * changeAllSubRequestsStatus
	 */ 
	virtual castor::repack::RepackAck* changeAllSubRequestsStatus(castor::repack::RepackSubRequestStatusCode st) throw (castor::exception::Exception);
	
	/**
	 * getLastTapeInformation 
	 */
	
	virtual std::vector<castor::repack::RepackSegment*>  getLastTapeInformation(std::string vidName, u_signed64* creationTime) throw (castor::exception::Exception);

     private:

	virtual void endTransation() throw (castor::exception::Exception);

	virtual void getSegmentsForSubRequest(RepackSubRequest* rsub) throw (castor::exception::Exception);

	/** The SQL Statement Objects and their strings */
	static const std::string s_storeRequestStatementString;
	oracle::occi::Statement *m_storeRequestStatement;
	static const std::string s_updateSubRequestSegmentsStatementString;
	oracle::occi::Statement *m_updateSubRequestSegmentsStatement;
	static const std::string s_getSegmentsForSubRequestStatementString;
	oracle::occi::Statement *m_getSegmentsForSubRequestStatement;
	static const std::string s_getSubRequestByVidStatementString;
	oracle::occi::Statement *m_getSubRequestByVidStatement;
	static const std::string s_getSubRequestsByStatusStatementString;
	oracle::occi::Statement *m_getSubRequestsByStatusStatement;
	static const std::string s_getAllSubRequestsStatementString;
	oracle::occi::Statement *m_getAllSubRequestsStatement;
	static const std::string s_validateRepackSubRequestStatementString;
	oracle::occi::Statement *m_validateRepackSubRequestStatement;
	static const std::string s_resurrectTapesOnHoldStatementString;
	oracle::occi::Statement *m_resurrectTapesOnHoldStatement; 	
	static const std::string s_restartSubRequestStatementString;
	oracle::occi::Statement *m_restartSubRequestStatement;
	static const std::string s_changeSubRequestsStatusStatementString;
	oracle::occi::Statement *m_changeSubRequestsStatusStatement;
	static const std::string s_changeAllSubRequestsStatusStatementString;
	oracle::occi::Statement *m_changeAllSubRequestsStatusStatement;
	static const std::string s_selectLastSegmentsSituationStatementString;
	oracle::occi::Statement *m_selectLastSegmentsSituationStatement;   
	
     }; // end of class OraRepackSvc

    } // end of namespace ora

  } // end of namespace repack

} // end of namespace castor

#endif // ORA_ORAREPACKSVC_HPP
