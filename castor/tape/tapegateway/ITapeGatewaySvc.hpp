/******************************************************************************
 *                castor/stager/ITapeGatewaySvc.hpp
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
 * @(#)$RCSfile: ITapeGatewaySvc.hpp,v $ $Revision: 1.14 $ $Release$ $Date: 2009/06/25 08:08:30 $ $Author: gtaur $
 *
 * This class provides methods related to tape handling
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef STAGER_ITAPEGATEWAYSVC_HPP
#define STAGER_ITAPEGATEWAYSVC_HPP 1

// Include Files
#include "castor/Constants.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"
#include <vector>
#include <string>
#include <list>


#include "castor/exception/Exception.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/tape/tapegateway/TapeRequestState.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToMigrateRequest.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/EndNotification.hpp"
#include "castor/tape/tapegateway/VolumeRequest.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/FileErrorReport.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/tape/tapegateway/TapeRequestState.hpp" 



namespace castor {
  namespace tape {
    namespace tapegateway {

 
    /**
     * This class provides methods related to tape gateway handling
     */

      class ITapeGatewaySvc : public virtual castor::stager::ICommonSvc {
      
      public:
	/**
	 * Get all the pending streams to associate a tape of the right 
	 * tapepool.
	 */

	virtual std::vector<castor::stager::Stream*> getStreamsToResolve()throw (castor::exception::Exception)=0;

        /**
         * Create the link between tape-stream.
         */

        virtual void resolveStreams(std::vector<u_signed64> strIds, std::vector<std::string> vids, std::vector<int> fseqs)
          throw (castor::exception::Exception)=0;

        /*
         * Get all the tapes for which we have to send a request to VDQM.     
         */

        virtual std::vector<castor::tape::tapegateway::TapeRequestState*> getTapesToSubmit()
          throw (castor::exception::Exception)=0;

	/*
         * Update the request that we sent  to VDQM.     
         */

	virtual void  updateSubmittedTapes(std::vector<castor::tape::tapegateway::TapeRequestState*> tapeRequests )=0;

        /**
         * Get all the tapes for which we have to check that VDQM didn't lost 
	 * our request
         */

        virtual std::vector<castor::tape::tapegateway::TapeRequestState*> getTapesToCheck(u_signed64 timeOut) 
          throw (castor::exception::Exception)=0;

        /**
         * Update all the tapes we checked in VDQM
         */

	virtual void updateCheckedTapes( std::vector<castor::tape::tapegateway::TapeRequestState*> tapes) 
	  throw (castor::exception::Exception)=0;

        /**
         * Get the best file to migrate at a certain point
         */

        virtual castor::tape::tapegateway::FileToMigrate* fileToMigrate(castor::tape::tapegateway::FileToMigrateRequest& req)
          throw (castor::exception::Exception)=0;

        /*
         * Update the db for a file which is migrated successfully or not
	 * calling fileMigrated or fileFailedToMigrate
         */

        virtual  void  fileMigrationUpdate(castor::tape::tapegateway::FileMigratedNotification& resp)
          throw (castor::exception::Exception)=0;

        /*
	 * Get the best fileToRecall at a certain moment
         */
        virtual castor::tape::tapegateway::FileToRecall* fileToRecall(castor::tape::tapegateway::FileToRecallRequest&  req)
          throw (castor::exception::Exception)=0;

	
        /**
         * Update the db for a segment which has been recalled successfully or not
         * calling extendedFileRecalled, fileRecalled and fileFailedToRecall
	 */

        virtual bool  segmentRecallUpdate(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception)=0;

        /**
         * Update the db for a file which has been recalled successfully or not
         * calling extendedFileRecalled, fileRecalled and fileFailedToRecall
	 */

        virtual void  fileRecallUpdate(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception)=0;

	/**
	 * Get Input for migration retries
	 */
	
	virtual std::vector<castor::stager::TapeCopy*>  inputForMigrationRetryPolicy()
	  throw (castor::exception::Exception)=0;

	/**
	 * Update the db using the retry migration policy returned values
	 */
	
	virtual void  updateWithMigrationRetryPolicyResult(std::vector<u_signed64> tcToRetry, std::vector<u_signed64> tcToFail ) throw (castor::exception::Exception)=0;

	/**
	 * Get Input for recall retries
	 */

	virtual std::vector<castor::stager::TapeCopy*>  inputForRecallRetryPolicy()
	  throw (castor::exception::Exception)=0;
	

	/**
	 * Update the db using the retry recall policy returned values
	 */

        virtual void  updateWithRecallRetryPolicyResult(std::vector<u_signed64> tcToRetry,std::vector<u_signed64> tcToFail) throw (castor::exception::Exception)=0;


	/**
	 * Check file for repack returning the repackvid if any and other information about the file for vmgr and ns update
	 */
	
	virtual std::string getRepackVidAndFileInformation(castor::tape::tapegateway::FileMigratedNotification& file, std::string& vid, int& copyNumber, u_signed64& lastModificationTime )throw (castor::exception::Exception)=0;

	/*
	 * Update the database when the tape aggregator allows us to serve a request 
	 */

	virtual castor::tape::tapegateway::Volume*  updateDbStartTape(castor::tape::tapegateway::VolumeRequest& startReq) throw (castor::exception::Exception)=0; 


	/*
	 * Update the database when the tape request has been served 
	 */

	virtual castor::stager::Tape* updateDbEndTape(castor::tape::tapegateway::EndNotification& endRequest) throw (castor::exception::Exception)=0; 

	/*
	 * get information to make cross check in the nameserver after a recall
	 */


	virtual void getSegmentInformation(FileRecalledNotification &fileRecalled, std::string& vid,int& copyNb)=0; 

	/*
	 * update the db after a major failure
	 */

	virtual castor::stager::Tape updateAfterFailure(FileErrorReport& failure)=0;

      }; // end of class ITapeGatewaySvc
    } // end of namespace tapegateway
  } // end of namespace tape
} // end of namespace castor

#endif // STAGER_ITAPEGATEWAYSVC_HPP
