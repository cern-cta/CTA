/******************************************************************************
 *                castor/tape/tapegateway/ora/OraTapeGateway.hpp
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
 * @(#)$RCSfile: OraTapeGatewaySvc.hpp,v $ $Revision: 1.15 $ $Release$ $Date: 2009/06/25 08:08:31 $ $Author: gtaur $
 *
 * Implementation of the ITapeGatewaySvc for Oracle
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef ORA_ORATAPEGATEWAYSVC_HPP
#define ORA_ORATAPEGATEWAYSVC_HPP 1

// Include Files

#include <u64subr.h>
#include <vector>

#include "occi.h"

#include "castor/BaseSvc.hpp"

#include "castor/db/newora/OraCommonSvc.hpp"

#include "castor/tape/tapegateway/ITapeGatewaySvc.hpp"


namespace castor {

  namespace tape {

    namespace tapegateway {
      
      namespace ora {

      /**
       * Implementation of the ITapeSvc for Oracle
       */
      class OraTapeGatewaySvc : public castor::db::ora::OraCommonSvc,
                         public virtual castor::tape::tapegateway::ITapeGatewaySvc {

      public:

        /**
         * default constructor
         */
        OraTapeGatewaySvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraTapeGatewaySvc() throw();

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

      public:

	/**
	 * Get all the pending streams to associate a tape of the right 
	 * tapepool.
	 */

	virtual std::vector<castor::stager::Stream*> getStreamsToResolve()throw (castor::exception::Exception);

        /**
         * Create the link between tape-stream.
         */

        virtual void resolveStreams(std::vector<u_signed64> strIds, std::vector<std::string> vids, std::vector<int> fseqs)
          throw (castor::exception::Exception);

        /*
         * Get all the tapes for which we have to send a request to VDQM.     
         */

        virtual std::vector<castor::tape::tapegateway::TapeRequestState*> getTapesToSubmit()
          throw (castor::exception::Exception);	

	/*
         * Update the request that we sent  to VDQM.     
         */

	virtual void  updateSubmittedTapes(std::vector<castor::tape::tapegateway::TapeRequestState*> tapeRequests);

        /**
         * Get all the tapes for which we have to check that VDQM didn't lost 
	 * our request
         */

        virtual std::vector<castor::tape::tapegateway::TapeRequestState*> getTapesToCheck(u_signed64 timeOut) 
          throw (castor::exception::Exception);


        /**
         * Update all the tapes we checked in VDQM
         */

        virtual void  updateCheckedTapes( std::vector<castor::tape::tapegateway::TapeRequestState*> tapes) 
          throw (castor::exception::Exception);
	


        /**
         * Get the best file to migrate at a certain point
         */

	virtual castor::tape::tapegateway::FileToMigrate* fileToMigrate(castor::tape::tapegateway::FileToMigrateRequest& req)
          throw (castor::exception::Exception);

        /*
         * Update the db for a file which is migrated successfully or not
	 * calling fileMigrated or fileFailedToMigrate
         */

        virtual void  fileMigrationUpdate (castor::tape::tapegateway::FileMigratedNotification& resp)
          throw (castor::exception::Exception);

        /*
	 * Get the best fileToRecall at a certain moment
         */
	
	virtual castor::tape::tapegateway::FileToRecall* fileToRecall(castor::tape::tapegateway::FileToRecallRequest& req)
          throw (castor::exception::Exception);

        /**
         * Update the db for a segment which has been recalled successfully or not
	 */

        virtual bool segmentRecallUpdate(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception);

        /**
         * Update the db for a file which has been recalled successfully or not
	 */

        virtual void fileRecallUpdate(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception);

	/**
	 * Get Input for migration retries
	 */
	
	virtual std::vector<castor::stager::TapeCopy*>  inputForMigrationRetryPolicy()
	  throw (castor::exception::Exception);

	/**
	 * Update the db using the retry migration policy returned values
	 */
	
	virtual void  updateWithMigrationRetryPolicyResult(std::vector<u_signed64> tcToRetry, std::vector<u_signed64> tcToFail  ) throw (castor::exception::Exception);

	/**
	 * Get Input for recall retries
	 */

	virtual std::vector<castor::stager::TapeCopy*>  inputForRecallRetryPolicy()
	  throw (castor::exception::Exception);
	

	/**
	 * Update the db using the retry recall policy returned values
	 */

        virtual void  updateWithRecallRetryPolicyResult(std::vector<u_signed64> tcToRetry,std::vector<u_signed64> tcToFail) throw (castor::exception::Exception);
	
	/**
	 * Check file for repack returning the repackvid if any
	 */

	virtual std::string getRepackVidAndFileInformation(castor::tape::tapegateway::FileMigratedNotification& file, std::string& vid, int& copyNumber, u_signed64& lastModificationTime)throw (castor::exception::Exception);
	
	
	/*
	 * Update the database when the tape aggregator allows us to serve a request 
	 */

	virtual castor::tape::tapegateway::Volume*  updateDbStartTape(castor::tape::tapegateway::VolumeRequest& startReq) throw (castor::exception::Exception); 

	/*
	 * Update the database when the tape request has been served 
	 */

	virtual castor::stager::Tape*  updateDbEndTape(castor::tape::tapegateway::EndNotification& endRequest) throw (castor::exception::Exception); 

	
	/*
	 * get information to make cross check in the nameserver after a recall
	 */


	virtual void getSegmentInformation(FileRecalledNotification &fileRecalled, std::string& vid,int& copyNb ) throw (castor::exception::Exception);  
	
	/*
	 * update the db after a major failure
	 */
	
	virtual castor::stager::Tape updateAfterFailure(FileErrorReport& failure) throw (castor::exception::Exception);

      private:

        /// SQL statement for function getStreamsToResolve
        static const std::string s_getStreamsToResolveStatementString;

        /// SQL statement object for function getStreamsToResolve
        oracle::occi::Statement *m_getStreamsToResolveStatement;

        /// SQL statement for function resolveStream
        static const std::string s_resolveStreamsStatementString;

        /// SQL statement object for function resolveStreams
        oracle::occi::Statement *m_resolveStreamsStatement;

        /// SQL statement for function getTapesTosubmit
        static const std::string s_getTapesToSubmitStatementString;

        /// SQL statement object for function getTapesToSubmit
        oracle::occi::Statement *m_getTapesToSubmitStatement;

	/// SQL statement for function updateSubmittedTapes
        static const std::string s_updateSubmittedTapesStatementString;

        /// SQL statement object for function updateSubmittedTapes
        oracle::occi::Statement *m_updateSubmittedTapesStatement;

        /// SQL statement for function getTapesToCheck
        static const std::string s_getTapesToCheckStatementString;

        /// SQL statement object for function getTapesToCheck
        oracle::occi::Statement *m_getTapesToCheckStatement;

        /// SQL statement for function updateCheckedTapes  
        static const std::string s_updateCheckedTapesStatementString;

        /// SQL statement object for function updateCheckedTapes
        oracle::occi::Statement *m_updateCheckedTapesStatement;

        /// SQL statement for function fileToMigrate
        static const std::string s_fileToMigrateStatementString;

        /// SQL statement object for function fileToMigrate
        oracle::occi::Statement *m_fileToMigrateStatement;

        /// SQL statement for function fileMigrationUpdate
        static const std::string s_fileMigrationUpdateStatementString;

        /// SQL statement object for function fileMigrationUpdate
        oracle::occi::Statement *m_fileMigrationUpdateStatement;

        /// SQL statement for function fileToRecall
        static const std::string s_fileToRecallStatementString;

        /// SQL statement object for function fileToRecall
        oracle::occi::Statement *m_fileToRecallStatement;

        /// SQL statement for function segmentRecallUpdate
        static const std::string s_segmentRecallUpdateStatementString;

        /// SQL statement object for function segmentRecallUpdate
        oracle::occi::Statement *m_segmentRecallUpdateStatement;

        /// SQL statement for function fileRecallUpdate
        static const std::string s_fileRecallUpdateStatementString;

        /// SQL statement object for function fileRecallUpdate
        oracle::occi::Statement *m_fileRecallUpdateStatement;

        /// SQL statement for function inputForMigrationRetryPolicy 
        static const std::string s_inputForMigrationRetryPolicyStatementString;

        /// SQL statement object for function inputForMigrationRetryPolicy
        oracle::occi::Statement *m_inputForMigrationRetryPolicyStatement;

	/// SQL statement for function updateWithMigrationRetryPolicyResult   
        static const std::string s_updateWithMigrationRetryPolicyResultStatementString;

        /// SQL statement object for function updateWithMigrationRetryPolicyResult  
        oracle::occi::Statement *m_updateWithMigrationRetryPolicyResultStatement;

        /// SQL statement for function inputForRecallRetryPolicy 
        static const std::string s_inputForRecallRetryPolicyStatementString;

        /// SQL statement object for function inputForRecallRetryPolicy
        oracle::occi::Statement *m_inputForRecallRetryPolicyStatement;

	/// SQL statement for function updateWithRecallRetryPolicyResult   
        static const std::string s_updateWithRecallRetryPolicyResultStatementString;

        /// SQL statement object for function updateWithRecallRetryPolicyResult  
        oracle::occi::Statement *m_updateWithRecallRetryPolicyResultStatement;

	/// SQL statement for function getRepackVidAndFileInformation  
        static const std::string s_getRepackVidAndFileInformationStatementString;

        /// SQL statement object for function getRepackVidAndFileInformation   
        oracle::occi::Statement *m_getRepackVidAndFileInformationStatement;

	//SQL statement object for function updateDbStartTape
	static const std::string s_updateDbStartTapeStatementString;

	/// SQL statement for function updateDbStartTape
	oracle::occi::Statement *m_updateDbStartTapeStatement;

	//SQL statement object for function updateDbEndTape
	static const std::string s_updateDbEndTapeStatementString;

	/// SQL statement for function updateDbEndTape
	oracle::occi::Statement *m_updateDbEndTapeStatement;

	// SQL statement object for function getSegmentInformation
        static const std::string s_getSegmentInformationStatementString;

        /// SQL statement object for function getSegmentInformation
        oracle::occi::Statement *m_getSegmentInformationStatement;

	// SQL statement object for function updateAfterFailure
        static const std::string s_updateAfterFailureStatementString;

        /// SQL statement object for function updateAfterFailure 
        oracle::occi::Statement *m_updateAfterFailureStatement;

      }; // end of class OraTapeGateway

      } // end of namespace ora

    } // end of namespace tapegateway

  }  // end of namespace tape

} // end of namespace castor

#endif // ORA_ORATAPEGATEWAYSVC_HPP
