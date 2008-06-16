/******************************************************************************
 *                castor/repack/IRepackSvc.hpp
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
 * @(#)$RCSfile: IRepackSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/06/16 11:30:32 $ $Author: waldron $
 *
 * This class provides methods related to tape handling
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_IREPACKSVC_HPP
#define STAGER_IREPACKSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/repack/RepackCommonHeader.hpp"
#include "castor/db/IDbStatement.hpp"
#include "castor/repack/RepackSubRequestStatusCode.hpp"
#include "castor/repack/RepackAck.hpp"

#include <string>
#include <list>

namespace castor {

  // Forward declaration
  class IObject;
  class IClient;
  class IAddress;

  namespace repack {


    /**
     * This class provides methods related to tape handling
     */

    class IRepackSvc : public virtual castor::stager::ICommonSvc {

    private:

      /**
       * bulk fill obj
       */

      virtual void getSegmentsForSubRequest(RepackSubRequest* rsub) throw (castor::exception::Exception)=0;


      /*
       * Used to conclude a transation in the db
       */

        virtual void endTransation() throw ()=0;

    public:

	/**
	 *  storeRequest
         *  Function used by the RepackWorker to store a new repack requests
	 *    and the subrequests associated to it
         *  
	 */

	virtual RepackAck* storeRequest(castor::repack::RepackRequest* rreq)throw ()=0;

	/**
	 * updateSubRequest
         * Used by the RepackMonitor, RepackFileStager, RepackFileChecker to update the status of the subrequest
	 */

	virtual void updateSubRequest( castor::repack::RepackSubRequest* obj) throw ()=0;

	/**
	 * updateSubRequestSegments
	 * Used by RepackMonitor and RepackFileStager to 
	 * record Segments which are done or invalid
	 *
	 */

	virtual void updateSubRequestSegments( castor::repack::RepackSubRequest* obj, std::vector<RepackSegment*> listToUpdate) throw ()=0;

	
	/**
	 * insert SubRequestSegments 
	 * Used by the RepackFileChecker to insert the segment for a 
	 * new tape
	 */

	virtual void insertSubRequestSegments(castor::repack::RepackSubRequest* obj) throw ()=0;
      

       /**
	* getSubRequestByVid 
	* used by the RepackWork to retrieve information about the segments of a tape using the Vid 
	*/

	virtual castor::repack::RepackResponse* getSubRequestByVid(std::string vid, bool fill) throw ()=0;


       /*
	* getSubRequestsByStatus
	* Used by RepackCleaner RepackFileChecker RepackFileStager RepackMonitor
        *
	*/

	virtual std::vector<castor::repack::RepackSubRequest*> getSubRequestsByStatus( castor::repack::RepackSubRequestStatusCode st, bool fill) throw ()=0;

	/*
	 * getAllSubRequests
         * Used by RepackWork to get a snapshot of the not archived repacksubrequest
	 */

	virtual castor::repack::RepackAck* getAllSubRequests() throw ()=0;

	/*
	 * validateRepackSubrequest
	 * Used by the RepackFileChecker to know if the request should be kept on hold waiting for another
         * tape to finish
	 *
	 */ 


	virtual bool  validateRepackSubRequest( RepackSubRequest* tape) throw ()=0;


	/*
	 * resurrectTapesOnHold
	 * Used by the RepackCleaner to resurrect tape that were waiting
	 *
	 */ 

	virtual void  resurrectTapesOnHold() throw ()=0;

	/*
	 * restartSubRequestTapes
	 * Used by the RepackFileStager to restart a RepackSubrequest
	 *
	 */

	virtual void  restartSubRequest(u_signed64 srId) throw ()=0;


	/*
	 * changeSubRequestsStatus
	 * Used by the RepackWorker to trigger the operation on the repack process
	 *
	 */

	virtual castor::repack::RepackAck*  changeSubRequestsStatus(std::vector<castor::repack::RepackSubRequest*> srs,  castor::repack::RepackSubRequestStatusCode st) throw ()=0;


	/*
	 * changeAllSubRequestsStatus
	 * Used by the RepackWorker to trigger the operation on the repack process
	 *
	 */ 

	virtual castor::repack::RepackAck*  changeAllSubRequestsStatus(castor::repack::RepackSubRequestStatusCode st) throw ()=0;


	/*
	 * getLastTapeInformation 
	 * Used for the undo (separate component)
	 */

	virtual castor::repack::RepackRequest* getLastTapeInformation(std::string vidName) throw ()=0;


    }; // end of class IRepackSvc

  } // end of namespace repack

} // end of namespace castor

#endif // STAGER_IREPACKSVC_HPP
