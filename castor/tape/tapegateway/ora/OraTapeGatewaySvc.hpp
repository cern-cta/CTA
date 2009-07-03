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
 * @(#)$RCSfile: OraTapeGatewaySvc.hpp,v $ $Revision: 1.16 $ $Release$ $Date: 2009/07/03 13:19:36 $ $Author: gtaur $
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
	 * Get all the pending streams 
	 */

	virtual std::vector<castor::stager::Stream*> getStreamsWithoutTapes()throw (castor::exception::Exception);

        /**
         * Associate to each Stream a Tape
         */

        virtual void attachTapesToStreams(std::vector<u_signed64> strIds, std::vector<std::string> vids, std::vector<int> fseqs)
          throw (castor::exception::Exception);

        /*
         * Get all the tapes for which we have to send a request to VDQM.     
         */

        virtual std::vector<castor::tape::tapegateway::TapeRequestState*> getTapesWithoutDriveReqs()
          throw (castor::exception::Exception);

	/*
         * Update the request that we sent  to VDQM.     
         */

	virtual void  attachDriveReqsToTapes(std::vector<castor::tape::tapegateway::TapeRequestState*> tapeRequests )throw (castor::exception::Exception);

        /**
         * Get all the tapes for which we have to check that VDQM didn't lost 
	 * our request
         */

        virtual std::vector<castor::tape::tapegateway::TapeRequestState*> getTapesWithDriveReqs(u_signed64 timeOut) 
          throw (castor::exception::Exception);

        /**
         * Restart the request lost by VDQM or left around
         */

	virtual void restartLostReqs( std::vector<castor::tape::tapegateway::TapeRequestState*> tapes) 
	  throw (castor::exception::Exception);

        /**
         * Get the best file to migrate
         */

        virtual castor::tape::tapegateway::FileToMigrate* getFileToMigrate(castor::tape::tapegateway::FileToMigrateRequest& req)
          throw (castor::exception::Exception);

        /*
         * Update the db for a file which is migrated successfully 
         */

        virtual  void  setFileMigrated(castor::tape::tapegateway::FileMigratedNotification& resp)
          throw (castor::exception::Exception);

        /*
	 * Get the best file to recall 
         */
        virtual castor::tape::tapegateway::FileToRecall* getFileToRecall(castor::tape::tapegateway::FileToRecallRequest&  req)
          throw (castor::exception::Exception);

	
        /**
         * Update the db for a segment which has been recalled successfully 
	 */

        virtual bool  setSegmentRecalled(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception);

        /**
         * Update the db for a file which has been recalled successfully 
	 */

        virtual void  setFileRecalled(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception);

	/**
	 * Get the tapecopies which faced a migration failure
	 */
	
	virtual std::vector<castor::stager::TapeCopy*>  getFailedMigrations()
	  throw (castor::exception::Exception);

	/**
	 * Update the db using the retry migration policy returned values
	 */
	
	virtual void  setMigRetryResult(std::vector<u_signed64> tcToRetry, std::vector<u_signed64> tcToFail ) throw (castor::exception::Exception);

	/**
	 * Get the tapecopies which faced a recall failure 
	 */

	virtual std::vector<castor::stager::TapeCopy*>  getFailedRecalls()
	  throw (castor::exception::Exception);
	

	/**
	 * Update the db using the retry recall policy returned values
	 */

        virtual void  setRecRetryResult(std::vector<u_signed64> tcToRetry,std::vector<u_signed64> tcToFail) throw (castor::exception::Exception);


	/**
	 * Access the db to retrieve the information about a completed migration
	 */
	
	virtual std::string getRepackVidAndFileInfo(castor::tape::tapegateway::FileMigratedNotification& file, std::string& vid, int& copyNumber, u_signed64& lastModificationTime )throw (castor::exception::Exception);

	/*
	 * Update the database when the tape aggregator allows us to serve a request
	 */

	virtual castor::tape::tapegateway::Volume*  startTapeSession(castor::tape::tapegateway::VolumeRequest& startReq) throw (castor::exception::Exception); 


	/*
	 * Update the database when the tape request has been served 
	 */

	virtual castor::stager::Tape* endTapeSession(castor::tape::tapegateway::EndNotification& endRequest) throw (castor::exception::Exception); 

	/*
	 * Access the db to retrieve the information about a completed recall 
	 */


	virtual void getSegmentInfo(FileRecalledNotification &fileRecalled, std::string& vid,int& copyNb)throw (castor::exception::Exception); 

	/*
	 * update the db after a major failure
	 */

	virtual castor::stager::Tape failTapeSession(castor::tape::tapegateway::EndNotificationErrorReport& failure)throw (castor::exception::Exception);

	
	/*
	 * update the db after a file failure
	 */


	virtual void failFileTransfer(FileErrorReport& failure)throw (castor::exception::Exception);

      private:

	static const std::string s_getStreamsWithoutTapesStatementString;
	oracle::occi::Statement *m_getStreamsWithoutTapesStatement;
	
	static const std::string s_attachTapesToStreamsStatementString;
	oracle::occi::Statement *m_attachTapesToStreamsStatement;
	
	static const std::string s_getTapesWithoutDriveReqsStatementString;
	oracle::occi::Statement *m_getTapesWithoutDriveReqsStatement;

	static const std::string s_attachDriveReqsToTapesStatementString;
	oracle::occi::Statement *m_attachDriveReqsToTapesStatement;
	
	static const std::string s_getTapesWithDriveReqsStatementString;
	oracle::occi::Statement *m_getTapesWithDriveReqsStatement;

	static const std::string s_restartLostReqsStatementString;
	oracle::occi::Statement *m_restartLostReqsStatement;
	
	static const std::string s_getFileToMigrateStatementString;
	oracle::occi::Statement *m_getFileToMigrateStatement;

	static const std::string s_setFileMigratedStatementString;
	oracle::occi::Statement *m_setFileMigratedStatement;
	
	static const std::string s_getFileToRecallStatementString;
	oracle::occi::Statement *m_getFileToRecallStatement;
	
	static const std::string s_setFileRecalledStatementString;
	oracle::occi::Statement *m_setFileRecalledStatement;

	static const std::string s_setSegmentRecalledStatementString;
	oracle::occi::Statement *m_setSegmentRecalledStatement;

	static const std::string s_getFailedMigrationsStatementString;
	oracle::occi::Statement *m_getFailedMigrationsStatement;

	static const std::string s_setMigRetryResultStatementString;
	oracle::occi::Statement *m_setMigRetryResultStatement;

	static const std::string s_getFailedRecallsStatementString;
	oracle::occi::Statement *m_getFailedRecallsStatement;

	static const std::string s_setRecRetryResultStatementString;	
	oracle::occi::Statement *m_setRecRetryResultStatement;	
	
	static const std::string s_getRepackVidAndFileInfoStatementString;
	oracle::occi::Statement *m_getRepackVidAndFileInfoStatement;

	static const std::string s_startTapeSessionStatementString;
	oracle::occi::Statement *m_startTapeSessionStatement;

	static const std::string s_endTapeSessionStatementString;
	oracle::occi::Statement *m_endTapeSessionStatement;
	
	static const std::string s_getSegmentInfoStatementString;
	oracle::occi::Statement *m_getSegmentInfoStatement;

	static const std::string s_failTapeSessionStatementString;
	oracle::occi::Statement *m_failTapeSessionStatement;

	static const std::string s_failFileTransferStatementString;
	oracle::occi::Statement *m_failFileTransferStatement;
	

      }; // end of class OraTapeGateway

      } // end of namespace ora

    } // end of namespace tapegateway

  }  // end of namespace tape

} // end of namespace castor

#endif // ORA_ORATAPEGATEWAYSVC_HPP
