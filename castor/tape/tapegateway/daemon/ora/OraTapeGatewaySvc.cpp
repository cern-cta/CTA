/******************************************************************************
 *                      OraTapeGatewaySvc.cpp
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
 * @(#)OraTapeGatewaySvc.cpp,v 1.20 $Release$ 2007/04/13 11:58:53 gtaur
 *
 * Implementation of the ITapeGatewaySvc for Oracle
 *
 * @author Giulia Taurelli
 *****************************************************************************/

// Include Files

#include <serrno.h>
#include <string>
#include <list>

#include "errno.h"

#include "occi.h"

#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"


#include "castor/exception/Internal.hpp"
#include "castor/exception/OutOfMemory.hpp"

#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/stager/TapeStatusCodes.hpp"

#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/PositionCommandCode.hpp"
#include "castor/tape/tapegateway/RetryPolicyElement.hpp"
#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"

#include "castor/tape/tapegateway/daemon/NsTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/ora/OraTapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/daemon/RmMasterTapeGatewayHelper.hpp"


//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::tape::tapegateway::ora::OraTapeGatewaySvc>* s_factoryOraTapeGatewaySvc =
  new castor::SvcFactory<castor::tape::tapegateway::ora::OraTapeGatewaySvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getStreamsWithoutTapesStatementString= "BEGIN tg_getStreamsWithoutTapes(:1);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_attachTapesToStreamsStatementString="BEGIN tg_attachTapesToStreams(:1,:2,:3);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapeWithoutDriveReqStatementString="BEGIN tg_getTapeWithoutDriveReq(:1,:2,:3,:4);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_attachDriveReqToTapeStatementString="BEGIN tg_attachDriveReqToTape(:1,:2,:3,:4,:5);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapesWithDriveReqsStatementString="BEGIN tg_getTapesWithDriveReqs(:1,:2);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_restartLostReqsStatementString="BEGIN tg_restartLostReqs(:1);END;";
  

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getFileToMigrateStatementString="BEGIN tg_getFileToMigrate(:1,:2,:3,:4);END;";
  

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_setFileMigratedStatementString="BEGIN tg_setFileMigrated(:1,:2,:3,:4,:5,:6);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getFileToRecallStatementString="BEGIN tg_getFileToRecall(:1,:2,:3,:4);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_setFileRecalledStatementString="BEGIN tg_setFileRecalled(:1,:2,:3,:4,:5,:6);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getFailedMigrationsStatementString="BEGIN tg_getFailedMigrations(:1);END;";
 
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_setMigRetryResultStatementString="BEGIN tg_setMigRetryResult(:1,:2);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getFailedRecallsStatementString="BEGIN tg_getFailedRecalls(:1);END;";
   
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_setRecRetryResultStatementString="BEGIN tg_setRecRetryResult(:1,:2);END;";
  
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getRepackVidAndFileInfoStatementString="BEGIN tg_getRepackVidAndFileInfo(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_startTapeSessionStatementString="BEGIN tg_startTapeSession(:1,:2,:3,:4,:5,:6);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_endTapeSessionStatementString="BEGIN tg_endTapeSession(:1,:2);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getSegmentInfoStatementString="BEGIN tg_getSegmentInfo(:1,:2,:3,:4,:5,:6);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_failFileTransferStatementString="BEGIN tg_failFileTransfer(:1,:2,:3,:4,:5);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_invalidateFileStatementString="BEGIN tg_invalidateFile(:1,:2,:3,:4,:5);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapeToReleaseStatementString="BEGIN tg_getTapeToRelease(:1,:2,:3);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_checkConfigurationStatementString ="BEGIN tg_checkConfiguration();END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_deleteStreamWithBadTapePoolStatementString="BEGIN tg_deleteStream(:1);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_deleteTapeRequestStatementString="BEGIN tg_deleteTapeRequest(:1);END;";


//------------------------------------------------------------------------------
// OraTapeGatewaySvc
//------------------------------------------------------------------------------
castor::tape::tapegateway::ora::OraTapeGatewaySvc::OraTapeGatewaySvc(const std::string name) :
  ITapeGatewaySvc(),
  OraCommonSvc(name),
  m_getStreamsWithoutTapesStatement(0),
  m_attachTapesToStreamsStatement(0),
  m_getTapeWithoutDriveReqStatement(0),
  m_attachDriveReqToTapeStatement(0),
  m_getTapesWithDriveReqsStatement(0),
  m_restartLostReqsStatement(0),
  m_getFileToMigrateStatement(0),
  m_setFileMigratedStatement(0),
  m_getFileToRecallStatement(0),
  m_setFileRecalledStatement(0),
  m_getFailedMigrationsStatement(0),
  m_setMigRetryResultStatement(0),
  m_getFailedRecallsStatement(0),
  m_setRecRetryResultStatement(0),	
  m_getRepackVidAndFileInfoStatement(0),
  m_startTapeSessionStatement(0),
  m_endTapeSessionStatement(0),
  m_getSegmentInfoStatement(0),
  m_failFileTransferStatement(0),
  m_invalidateFileStatement(0),
  m_getTapeToReleaseStatement(0),
  m_checkConfigurationStatement(0),
  m_deleteStreamWithBadTapePoolStatement(0),
  m_deleteTapeRequestStatement(0)
{
}

//------------------------------------------------------------------------------
// ~OraTapeGatewaySvc
//------------------------------------------------------------------------------
castor::tape::tapegateway::ora::OraTapeGatewaySvc::~OraTapeGatewaySvc() throw() {
  reset();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::tape::tapegateway::ora::OraTapeGatewaySvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::tape::tapegateway::ora::OraTapeGatewaySvc::ID() {
  return castor::SVC_ORATAPEGATEWAYSVC;
}

//-----------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {

    if ( m_getStreamsWithoutTapesStatement ) deleteStatement(m_getStreamsWithoutTapesStatement);
    if ( m_attachTapesToStreamsStatement ) deleteStatement(m_attachTapesToStreamsStatement);
    if ( m_getTapeWithoutDriveReqStatement ) deleteStatement(m_getTapeWithoutDriveReqStatement); 
    if ( m_attachDriveReqToTapeStatement ) deleteStatement(m_attachDriveReqToTapeStatement);
    if ( m_getTapesWithDriveReqsStatement ) deleteStatement(m_getTapesWithDriveReqsStatement);
    if ( m_restartLostReqsStatement ) deleteStatement( m_restartLostReqsStatement);
    if ( m_getFileToMigrateStatement ) deleteStatement( m_getFileToMigrateStatement);
    if ( m_setFileMigratedStatement ) deleteStatement( m_setFileMigratedStatement);
    if ( m_getFileToRecallStatement ) deleteStatement( m_getFileToRecallStatement);
    if ( m_setFileRecalledStatement ) deleteStatement(m_setFileRecalledStatement);
    if ( m_getFailedMigrationsStatement ) deleteStatement(m_getFailedMigrationsStatement);
    if ( m_setMigRetryResultStatement ) deleteStatement(m_setMigRetryResultStatement);
    if ( m_getFailedRecallsStatement ) deleteStatement(m_getFailedRecallsStatement);
    if ( m_setRecRetryResultStatement ) deleteStatement(	m_setRecRetryResultStatement);
    if ( m_getRepackVidAndFileInfoStatement ) deleteStatement( m_getRepackVidAndFileInfoStatement);
    if ( m_startTapeSessionStatement ) deleteStatement(m_startTapeSessionStatement);
    if ( m_endTapeSessionStatement ) deleteStatement(m_endTapeSessionStatement);
    if ( m_getSegmentInfoStatement ) deleteStatement( m_getSegmentInfoStatement);
    if ( m_failFileTransferStatement ) deleteStatement(m_failFileTransferStatement);
    if ( m_invalidateFileStatement ) deleteStatement(m_invalidateFileStatement);
    if ( m_getTapeToReleaseStatement ) deleteStatement(m_getTapeToReleaseStatement);
    if ( m_checkConfigurationStatement ) deleteStatement(m_checkConfigurationStatement);
    if ( m_deleteStreamWithBadTapePoolStatement) deleteStatement(m_deleteStreamWithBadTapePoolStatement);    
    if ( m_deleteTapeRequestStatement) deleteStatement(m_deleteTapeRequestStatement);

  } catch (oracle::occi::SQLException e) {};

  // Now reset all pointers to 0


  m_getStreamsWithoutTapesStatement= 0; 
  m_attachTapesToStreamsStatement = 0;
  m_getTapeWithoutDriveReqStatement= 0;
  m_attachDriveReqToTapeStatement= 0;
  m_getTapesWithDriveReqsStatement = 0;
  m_restartLostReqsStatement = 0;
  m_getFileToMigrateStatement= 0;
  m_setFileMigratedStatement= 0; 
  m_getFileToRecallStatement= 0;
  m_setFileRecalledStatement= 0;
  m_getFailedMigrationsStatement = 0;
  m_setMigRetryResultStatement= 0;
  m_getFailedRecallsStatement= 0;
  m_setRecRetryResultStatement = 0;
  m_getRepackVidAndFileInfoStatement = 0;
  m_startTapeSessionStatement = 0;
  m_endTapeSessionStatement = 0;
  m_getSegmentInfoStatement = 0;
  m_failFileTransferStatement= 0;
  m_invalidateFileStatement = 0;
  m_getTapeToReleaseStatement=0;
  m_checkConfigurationStatement=0;
  m_deleteStreamWithBadTapePoolStatement=0;
  m_deleteTapeRequestStatement=0; 
}

//----------------------------------------------------------------------------
// getStreamsWithoutTapes
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getStreamsWithoutTapes(std::list<castor::stager::Stream>& streams,std::list<castor::stager::TapePool>& tapepools )
  throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok
    if (0 == m_getStreamsWithoutTapesStatement) {
      m_getStreamsWithoutTapesStatement =
        createStatement(s_getStreamsWithoutTapesStatementString);
      m_getStreamsWithoutTapesStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getStreamsWithoutTapesStatement->executeUpdate();

    if (0 == nb) {
      cnvSvc()->commit(); 
      return;
    }

    oracle::occi::ResultSet *rs =
      m_getStreamsWithoutTapesStatement->getCursor(1);

    // Run through the cursor
   
    while( rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::stager::Stream item;
      item.setId((u_signed64)rs->getDouble(1));
      item.setInitialSizeToTransfer((u_signed64)rs->getDouble(2));
      item.setStatus((castor::stager::StreamStatusCodes)rs->getInt(3));
      streams.push_back(item);

      castor::stager::TapePool tp;
      tp.setId((u_signed64)rs->getDouble(4));
      tp.setName(rs->getString(5));
      tapepools.push_back(tp);
    }

    m_getStreamsWithoutTapesStatement->closeResultSet(rs);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getStreamsWithoutTapes"
      << std::endl << e.what();
    throw ex;
  }

}

//----------------------------------------------------------------------------
// attachTapesToStreams
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::attachTapesToStreams(const std::list<u_signed64>& strIds,const std::list<std::string>& vids, const std::list<int>& fseqs)
          throw (castor::exception::Exception){
  
  unsigned char (*bufferFseqs)[21]=NULL;
  ub2 *lensFseqs=NULL;
  unsigned char (*bufferStrIds)[21]=NULL;
  ub2 *lensStrIds=NULL;
  char * bufferVids=NULL;
  ub2 *lensVids=NULL;
    

  try {

    if ( !strIds.size() || !vids.size() || !fseqs.size() ) {
      // just release the lock no result
      cnvSvc()->commit();
      return;
    }

    if (strIds.size() != vids.size() || strIds.size() != fseqs.size()) {
      // just release the lock no result
      cnvSvc()->commit();
      castor::exception::Exception e(EINVAL);
      throw e;
    }
     
    // Check whether the statements are ok
    if (0 == m_attachTapesToStreamsStatement) {
      m_attachTapesToStreamsStatement =
        createStatement(s_attachTapesToStreamsStatementString);
    }

    // input

    ub4 nb=strIds.size();

    // fseq
    bufferFseqs=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    lensFseqs=(ub2 *)malloc (sizeof(ub2)*nb);

    if ( lensFseqs  == 0 || bufferFseqs == 0 ) {
      if (lensFseqs != 0 ) free(lensFseqs);
      if (bufferFseqs != 0 ) free(bufferFseqs);
      castor::exception::OutOfMemory e; 
      throw e;
    }
     
    // strId

    bufferStrIds =(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    lensStrIds=(ub2 *)malloc (sizeof(ub2)*nb);
    if ( lensStrIds  == 0 || bufferStrIds == 0 ) {
      if (lensStrIds != 0 ) free(lensStrIds);
      if (bufferStrIds != 0) free(bufferStrIds);
      if (lensFseqs != 0 ) free(lensFseqs);
      if (bufferFseqs != 0 ) free(bufferFseqs);
      castor::exception::OutOfMemory e; 
      throw e;
    }

    // vids
    

    // get the maximum cell size
    unsigned int maxLen=0;
 
    for (std::list<std::string>::const_iterator vid = vids.begin();
	 vid != vids.end();
	 vid++){
      maxLen=maxLen > (*vid).length()?maxLen:(*vid).length();
    
    }

    if (maxLen == 0) {
      if (lensStrIds != 0 ) free(lensStrIds);
      if (bufferStrIds != 0) free(bufferStrIds);
      if (lensFseqs != 0 ) free(lensFseqs);
      if (bufferFseqs != 0 ) free(bufferFseqs);
      castor::exception::Internal ex;
      ex.getMessage() << "invalid VID in attachTapesToStreams"
		      << std::endl;
      throw ex;

    }
     

    unsigned int bufferCellSize = maxLen * sizeof(char);
    lensVids = (ub2*) malloc(nb * sizeof(ub2));
    bufferVids =
      (char*) malloc(nb * bufferCellSize);

    if ( lensVids  == 0 || bufferVids == 0 ) {
      if (lensStrIds != 0 ) free(lensStrIds);
      if (bufferStrIds != 0) free(bufferStrIds);
      if (lensVids != 0 ) free(lensVids);
      if (bufferVids != 0) free(bufferVids);
      if (lensFseqs != 0 ) free(lensFseqs);
      if (bufferFseqs != 0 ) free(bufferFseqs);
      castor::exception::OutOfMemory e; 
      throw e;
    }



    // DataBuffer with all the vid (one for each subrequest)
     

    // Fill in the structure
    
    std::list<std::string>::const_iterator vid;
    std::list<u_signed64>::const_iterator strId;
    std::list<int>::const_iterator  fseq;
    int i=0;

    for(vid = vids.begin(),
	  strId = strIds.begin(),
	  fseq = fseqs.begin();
	strId != strIds.end();
	vid++,strId++,fseq++,i++ ){
      // fseq
      
      oracle::occi::Number n = (double)(*fseq);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferFseqs[i],b.length());
      lensFseqs[i] = b.length();
       
      // stream Ids
       
      n = (double)(*strId);
      b = n.toBytes();
      b.getBytes(bufferStrIds[i],b.length());
      lensStrIds[i] = b.length();
      
      // vids
       
      lensVids[i]= (*vid).length();
      strncpy(bufferVids+(bufferCellSize*i),(*vid).c_str(),lensVids[i]);
       
	
    }

    ub4 unused=nb;
    m_attachTapesToStreamsStatement->setDataBufferArray(1,bufferFseqs, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensFseqs);
    m_attachTapesToStreamsStatement->setDataBufferArray(2,bufferStrIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensStrIds);
    ub4 len=nb;
    m_attachTapesToStreamsStatement->setDataBufferArray(3, bufferVids, oracle::occi::OCCI_SQLT_CHR,len, &len, maxLen, lensVids);

    // execute the statement and see whether we found something
 
    m_attachTapesToStreamsStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {

    if (lensStrIds != 0 ) free(lensStrIds);
    if (bufferStrIds != 0) free(bufferStrIds);
    if (lensVids != 0 ) free(lensVids);
    if (bufferVids != 0) free(bufferVids);
    if (lensFseqs != 0 ) free(lensFseqs);
    if (bufferFseqs != 0 ) free(bufferFseqs);

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in attachTapesToStreams"
      << std::endl << e.what();
    throw ex;
  }

  if (lensStrIds != 0 ) free(lensStrIds);
  if (bufferStrIds != 0) free(bufferStrIds);
  if (lensVids != 0 ) free(lensVids);
  if (bufferVids != 0) free(bufferVids);
  if (lensFseqs != 0 ) free(lensFseqs);
  if (bufferFseqs != 0 ) free(bufferFseqs);

}


//----------------------------------------------------------------------------
// getTapeWithoutDriveReq 
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapeWithoutDriveReq(castor::tape::tapegateway::VdqmTapeGatewayRequest& request)
  throw (castor::exception::Exception){



  try {
    // Check whether the statements are ok
    if (0 == m_getTapeWithoutDriveReqStatement) {
      m_getTapeWithoutDriveReqStatement =
        createStatement(s_getTapeWithoutDriveReqStatementString);

      m_getTapeWithoutDriveReqStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);

      m_getTapeWithoutDriveReqStatement->registerOutParam
        (2, oracle::occi::OCCIDOUBLE);

      m_getTapeWithoutDriveReqStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
     
      m_getTapeWithoutDriveReqStatement->registerOutParam
        (4, oracle::occi::OCCISTRING, 2048);

    }

    // execute the statement and see whether we found something
 
    m_getTapeWithoutDriveReqStatement->executeUpdate();
   
    u_signed64 reqId=(u_signed64)m_getTapeWithoutDriveReqStatement->getDouble(1);

    if (reqId==0){
      cnvSvc()->commit();
      return;
    }
    
    request.setTaperequest(reqId);
    request.setAccessMode((u_signed64)m_getTapeWithoutDriveReqStatement->getDouble(2));
    request.setVid(m_getTapeWithoutDriveReqStatement->getString(4));
  

  } catch (oracle::occi::SQLException e) {
    
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getTapeWithoutDriveReq"
      << std::endl << e.what();
    throw ex;
  }
  
}
 
//----------------------------------------------------------------------------
// attachDriveReqToTape
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::attachDriveReqToTape(const castor::tape::tapegateway::VdqmTapeGatewayRequest& tapeRequest, const castor::stager::Tape& tape) throw (castor::exception::Exception){ 
  
  try {
     
    // Check whether the statements are ok
    if (0 == m_attachDriveReqToTapeStatement) {
      m_attachDriveReqToTapeStatement =
        createStatement(s_attachDriveReqToTapeStatementString);
    }
    

    m_attachDriveReqToTapeStatement->setDouble(1, (double)tapeRequest.taperequest());
    m_attachDriveReqToTapeStatement->setDouble(2, (double)tapeRequest.mountTransactionId());
    m_attachDriveReqToTapeStatement->setString(3, tape.dgn());
    m_attachDriveReqToTapeStatement->setString(4, tape.label());
    m_attachDriveReqToTapeStatement->setString(5, tape.density());
 
    // execute the statement and see whether we found something
    
     m_attachDriveReqToTapeStatement->executeUpdate();
     
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in attachDriveReqToTape"
      << std::endl << e.what();
    throw ex;
  }
  
}

//----------------------------------------------------------------------------
// getTapesWithDriveReqs
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapesWithDriveReqs(std::list<castor::tape::tapegateway::TapeGatewayRequest>& requests,std::list<std::string>& vids, const u_signed64& timeOut) 
  throw (castor::exception::Exception){


  try {
    // Check whether the statements are ok
    if (0 == m_getTapesWithDriveReqsStatement) {
      m_getTapesWithDriveReqsStatement =
        createStatement(s_getTapesWithDriveReqsStatementString);
      m_getTapesWithDriveReqsStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR);
    }

    m_getTapesWithDriveReqsStatement->setDouble(1,(double)timeOut);

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getTapesWithDriveReqsStatement->executeUpdate();

    if (0 == nb) {
      // just release the lock no result
      cnvSvc()->commit();
      return;
    }

    oracle::occi::ResultSet *rs =
      m_getTapesWithDriveReqsStatement->getCursor(2);

    // Run through the cursor
    
    while(rs->next()  == oracle::occi::ResultSet::DATA_AVAILABLE) {
      tape::tapegateway::TapeGatewayRequest item;
      item.setAccessMode(rs->getInt(1));
      item.setId((u_signed64)rs->getDouble(2));
      item.setStartTime((u_signed64)rs->getDouble(3));
      item.setLastVdqmPingTime((u_signed64)rs->getDouble(4));
      item.setVdqmVolReqId((u_signed64)rs->getDouble(5));
      item.setStatus((TapeRequestStateCode)rs->getInt(6));
      requests.push_back(item);

      std::string vid(rs->getString(7));
      vids.push_back(vid);

    }

    m_getTapesWithDriveReqsStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getTapesWithDriveReqs"
      << std::endl << e.what();
    throw ex;
  }

}
 


//----------------------------------------------------------------------------
// restartLostReqs 
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::restartLostReqs(const std::list<castor::tape::tapegateway::TapeGatewayRequest>& tapeRequests)
          throw (castor::exception::Exception){

  unsigned char (*bufferTapes)[21]=NULL;
  ub2 *lensTapes=NULL;

  try {

    if ( !tapeRequests.size()) {
      cnvSvc()->commit();
      return;
    }
     
    // Check whether the statements are ok
    if (0 == m_restartLostReqsStatement) {
      m_restartLostReqsStatement =
        createStatement(s_restartLostReqsStatementString);
    }
    
    // input
    
    ub4 nb=tapeRequests.size();
    bufferTapes=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    
    lensTapes=(ub2 *)malloc (sizeof(ub2)*nb);

    
    if ( lensTapes == 0 || bufferTapes == 0 ) {
      if (lensTapes != 0 ) free(lensTapes);
      if (bufferTapes != 0) free(bufferTapes);
      castor::exception::OutOfMemory e; 
      throw e;
     }

    
    // Fill in the structure
    
    int i=0;
    for (std::list<castor::tape::tapegateway::TapeGatewayRequest>::const_iterator tapeRequest=tapeRequests.begin();
	 tapeRequest != tapeRequests.end();
	 tapeRequest++,i++){

      oracle::occi::Number n = (double)((*tapeRequest).id());
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferTapes[i],b.length());
      lensTapes[i] = b.length();
    }

    ub4 unused=nb;
    m_restartLostReqsStatement->setDataBufferArray(1,bufferTapes, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensTapes);
    // execute the statement and see whether we found something
 
    m_restartLostReqsStatement->executeUpdate();

    if (lensTapes != 0 ) free(lensTapes);
    if (bufferTapes != 0) free(bufferTapes);

  } catch (oracle::occi::SQLException e) {

    if (lensTapes != 0 ) free(lensTapes);
    if (bufferTapes != 0) free(bufferTapes);

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in restartLostReqs "
      << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// getFileToMigrate  
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFileToMigrate(const castor::tape::tapegateway::FileToMigrateRequest& req,castor::tape::tapegateway::FileToMigrate& file) throw (castor::exception::Exception){

  try {

    while (1) { // until we get a valid file
     
      std::string diskserver;
      std::string mountpoint;
      file.setMountTransactionId(0);
      // Check whether the statements are ok
      if (0 == m_getFileToMigrateStatement) {
	m_getFileToMigrateStatement =
	  createStatement(s_getFileToMigrateStatementString);
	m_getFileToMigrateStatement->registerOutParam
	  (2, oracle::occi::OCCIINT);
	m_getFileToMigrateStatement->registerOutParam
	  (3, oracle::occi::OCCISTRING, 2048 );
	m_getFileToMigrateStatement->registerOutParam
	  (4, oracle::occi::OCCICURSOR);
      }

      m_getFileToMigrateStatement->setDouble(1,(double)req.mountTransactionId());
      m_getFileToMigrateStatement->executeUpdate();

      int ret = m_getFileToMigrateStatement->getInt(2);
      if (ret == -1 ) {
	cnvSvc()->commit();
	return;
      }

      if (ret == -2 ) {// UNKNOWN request
	cnvSvc()->commit();
	castor::exception::Exception e(EINVAL);
	throw e;
      }


      if (ret == -3 ) {// no diskserver available
	cnvSvc()->commit();
	castor::exception::Internal ex;
	ex.getMessage()
	  << "no diskserver available"
	  << std::endl; 
	
	throw ex;
	
      }

      std::string vid=m_getFileToMigrateStatement->getString(3); 

      oracle::occi::ResultSet *rs =
	m_getFileToMigrateStatement->getCursor(4);

      // Run through the cursor 
      
      oracle::occi::ResultSet::Status status = rs->next();
      
      // one at the moment

      if  (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      
	file.setFileid((u_signed64)rs->getDouble(1));
	file.setNshost(rs->getString(2));
	file.setLastModificationTime((u_signed64)rs->getDouble(3));
	diskserver=rs->getString(4);
	mountpoint=rs->getString(5);
	file.setPath(diskserver.append(":").append(mountpoint).append(rs->getString(6))); 
	file.setLastKnownFilename(rs->getString(7));
	file.setFseq(rs->getInt(8));
	file.setFileSize((u_signed64)rs->getDouble(9));
	file.setFileTransactionId((u_signed64)rs->getDouble(10));
	file.setMountTransactionId(req.mountTransactionId());
	file.setPositionCommandCode(TPPOSIT_FSEQ);

	try {
	  NsTapeGatewayHelper nsHelper;
	  nsHelper.checkFileToMigrate(file,vid);
	} catch (castor::exception::Exception e) {

	  struct Cns_fileid castorFileId;
	  memset(&castorFileId,'\0',sizeof(castorFileId));
	  strncpy(
		  castorFileId.server,
		  file.nshost().c_str(),
		  sizeof(castorFileId.server)-1
		  );
	  castorFileId.fileid = file.fileid();
	  
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str()),
	     castor::dlf::Param("TPVID", vid.c_str())
	    };
	  
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, ORA_FILE_TO_MIGRATE_NS_ERROR, 3, params, &castorFileId);
	  

	  try {

	    
	    FileErrorReport failure;
	    failure.setMountTransactionId(file.mountTransactionId()); 
	    failure.setFileid(file.fileid());
	    failure.setNshost(file.nshost());
	    failure.setFseq(file.fseq());
	    failure.setErrorCode(e.code());
	    invalidateFile(failure);

	  } catch (castor::exception::Exception ex){

	    // just log the error
	    castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(ex.code())),
	     castor::dlf::Param("errorMessage",ex.getMessage().str()),
	     castor::dlf::Param("TPVID", vid)
	    };
    
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,ORA_DB_ERROR , 3, params, &castorFileId);
       
	    
	  }  

	  m_getFileToMigrateStatement->closeResultSet(rs);
	  continue; // get another tapecopy

	}
	
	// we have a valid candidate we send a report to RmMaster stream started
	try{
	  
	  RmMasterTapeGatewayHelper rmMasterHelper;
	  rmMasterHelper.sendStreamReport(diskserver,mountpoint,castor::monitoring::STREAMDIRECTION_WRITE,true);
	  
	} catch (castor::exception::Exception e){
	  
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str()),
	     castor::dlf::Param("diskserver",diskserver),
	     castor::dlf::Param("mountpoint",mountpoint)
	    };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, ORA_IMPOSSIBLE_TO_SEND_RMMASTER_REPORT, 4, params);
	}

      }
      	
      m_getFileToMigrateStatement->closeResultSet(rs);
      break; // end the loop

    }      
    // here we have a valid candidate or NULL
    cnvSvc()->commit();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFileToMigrate"
      << std::endl << e.what();
    throw ex;
  }
  // hardcoded umask
  file.setUmask(022);  
 
}

//----------------------------------------------------------------------------
// setFileMigrated  
//----------------------------------------------------------------------------
  
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::setFileMigrated(const castor::tape::tapegateway::FileMigratedNotification& resp)
  throw (castor::exception::Exception){
  std::string diskserver;
  std::string mountpoint;


  try {
    // Check whether the statements are ok

    if (0 == m_setFileMigratedStatement) {
      m_setFileMigratedStatement =
        createStatement(s_setFileMigratedStatementString);
      m_setFileMigratedStatement->registerOutParam
        (6, oracle::occi::OCCICURSOR);
    }

    m_setFileMigratedStatement->setDouble(1,(double)resp.mountTransactionId()); // transaction id
    m_setFileMigratedStatement->setDouble(2,(double)resp.fileid());
    m_setFileMigratedStatement->setString(3,resp.nshost());
    m_setFileMigratedStatement->setInt(4,resp.fseq()); 
    m_setFileMigratedStatement->setDouble(5,(double)resp.fileTransactionId()); 
    
    m_setFileMigratedStatement->executeUpdate();

    oracle::occi::ResultSet *rs =
      m_setFileMigratedStatement->getCursor(6);

    // Run through the cursor 

    oracle::occi::ResultSet::Status status = rs->next();
    // just one

    if (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      diskserver = rs->getString(1);
      mountpoint = rs->getString(2);

    
      // send report to RmMaster stream ended
  
      try{

	RmMasterTapeGatewayHelper rmMasterHelper;
	rmMasterHelper.sendStreamReport(diskserver, mountpoint, castor::monitoring::STREAMDIRECTION_WRITE,false);
  
      } catch (castor::exception::Exception e){

	castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str()),
	     castor::dlf::Param("diskserver",diskserver),
	     castor::dlf::Param("mountpoint",mountpoint)
	    };

	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, ORA_IMPOSSIBLE_TO_SEND_RMMASTER_REPORT, 4, params);
      }
    }
    
    m_setFileMigratedStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setFileMigrated"
      << std::endl << e.what();
    throw ex;
  }
 
  
}

//----------------------------------------------------------------------------
// getFileToRecall 
//----------------------------------------------------------------------------
   
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFileToRecall(const castor::tape::tapegateway::FileToRecallRequest& req, castor::tape::tapegateway::FileToRecall& file) throw (castor::exception::Exception)
{
    

  try {

    while (1) {

      std::string diskserver;
      std::string mountpoint;
      file.setMountTransactionId(0);

      // Check whether the statements are ok
      if (0 == m_getFileToRecallStatement) {
	m_getFileToRecallStatement =
	  createStatement(s_getFileToRecallStatementString);
	m_getFileToRecallStatement->registerOutParam
	  (2, oracle::occi::OCCIINT);
	m_getFileToRecallStatement->registerOutParam
	  (3, oracle::occi::OCCISTRING,  2048);
	m_getFileToRecallStatement->registerOutParam
	  (4, oracle::occi::OCCICURSOR);
      }
   
      m_getFileToRecallStatement->setDouble(1,(double)req.mountTransactionId());
      
      // execute the statement and see whether we found something
 
      m_getFileToRecallStatement->executeUpdate();

      
      int ret = m_getFileToRecallStatement->getInt(2);
      if (ret == -1 ) return;
     
      if (ret == -2 ) {// UNKNOWN request

	castor::exception::Exception e(ENOENT);
	throw e;

      }

      if (ret == -3 ) {// no diskserver available

	castor::exception::Internal ex;
	ex.getMessage()
	  << "no diskserver available"
	  << std::endl; 
	
	throw ex;
	
      }

      std::string vid= m_getFileToRecallStatement->getString(3);
   
      oracle::occi::ResultSet *rs =
	m_getFileToRecallStatement->getCursor(4);

      

      // one at the moment
    

      if  (rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {


	file.setFileid((u_signed64)rs->getDouble(1));
	file.setNshost(rs->getString(2));
	diskserver=rs->getString(3);
	mountpoint=rs->getString(4);

	file.setPath(diskserver.append(":").append(mountpoint).append(rs->getString(5)));

	file.setFseq(rs->getInt(6));
	file.setFileTransactionId((u_signed64)rs->getDouble(7));

        file.setMountTransactionId(req.mountTransactionId());
	file.setPositionCommandCode(TPPOSIT_BLKID);

	// it might be changed after the ns check to TPPOSIT_FSEQ

	// let's check if it is valid in the nameserver and let's retrieve the blockid

	try {
	  
	  NsTapeGatewayHelper nsHelper;
	  nsHelper.getBlockIdToRecall(file,vid);
	  break; // found a valid file

	}catch (castor::exception::Exception e) {
	  struct Cns_fileid castorFileId;
	  memset(&castorFileId,'\0',sizeof(castorFileId));
	  strncpy(
		  castorFileId.server,
		  file.nshost().c_str(),
		  sizeof(castorFileId.server)-1
		  );
	  castorFileId.fileid = file.fileid();

	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str()),
	     castor::dlf::Param("TPVID", vid)
	    };

	  
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_WARNING, ORA_FILE_TO_RECALL_NS_ERROR, 3, params,&castorFileId);

	  try {
	    FileErrorReport failure;
	    failure.setMountTransactionId(file.mountTransactionId()); 
	    failure.setFileid(file.fileid());
	    failure.setNshost(file.nshost());
	    failure.setFseq(file.fseq());
	    failure.setErrorCode(e.code());
	    invalidateFile(failure);
	    
	  } catch (castor::exception::Exception ex){

	    castor::dlf::Param params[] =
	      {castor::dlf::Param("errorCode",sstrerror(ex.code())),
	       castor::dlf::Param("errorMessage",ex.getMessage().str()),
	       castor::dlf::Param("TPVID", vid)
	      };
    
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, ORA_DB_ERROR, 2, params,&castorFileId);
	  }

	  
	  
	  m_getFileToRecallStatement->closeResultSet(rs);

	  continue; // Let's get another candidate, this one was not valid

	}
   
	// here we have a valid candidate and we send a report to RmMaster stream started    
	  
	try{

	  RmMasterTapeGatewayHelper rmMasterHelper;
	  rmMasterHelper.sendStreamReport(diskserver,mountpoint,castor::monitoring::STREAMDIRECTION_READ,true);
	
	} catch (castor::exception::Exception e){
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str()),
	     castor::dlf::Param("diskserver",diskserver),
	     castor::dlf::Param("mountpoint",mountpoint)
	    };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR,ORA_IMPOSSIBLE_TO_SEND_RMMASTER_REPORT, 4, params);
	}
	
      }
     
      m_getFileToRecallStatement->closeResultSet(rs);
      break;
    }
    
    cnvSvc()->commit();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFileToRecall"
      << std::endl << e.what();
    throw ex;
  }
 
  // hardcoded umask
  file.setUmask(077);  
 

}
 
//----------------------------------------------------------------------------
// setFileRecalled  
//----------------------------------------------------------------------------
   
void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::setFileRecalled(const castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception){
 std::string diskserver;
 std::string mountpoint;

  try {
    // Check whether the statements are ok

    if (0 == m_setFileRecalledStatement) {
      m_setFileRecalledStatement =
        createStatement(s_setFileRecalledStatementString);
      m_setFileRecalledStatement->registerOutParam
	(6, oracle::occi::OCCICURSOR); 
    }

    m_setFileRecalledStatement->setDouble(1,(double)resp.mountTransactionId());
    m_setFileRecalledStatement->setDouble(2,(double)resp.fileid());
    m_setFileRecalledStatement->setString(3,resp.nshost());
    m_setFileRecalledStatement->setInt(4,resp.fseq());
    m_setFileRecalledStatement->setDouble(5,(double)resp.fileTransactionId());

    m_setFileRecalledStatement->executeUpdate();
    
    
    
    oracle::occi::ResultSet *rs =
      m_setFileRecalledStatement->getCursor(6);

    // Run through the cursor 

    // just one

    if (rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {

      diskserver = rs->getString(1);
      mountpoint = rs->getString(2);

    // send report to RmMaster stream ended
      
      try {
      
	RmMasterTapeGatewayHelper  rmMasterHelper;
	rmMasterHelper.sendStreamReport(diskserver,mountpoint,castor::monitoring::STREAMDIRECTION_READ,false);
	
      } catch (castor::exception::Exception e){
	castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str()),
	     castor::dlf::Param("diskserver",diskserver),
	     castor::dlf::Param("mountpoint",mountpoint)
	    };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, ORA_IMPOSSIBLE_TO_SEND_RMMASTER_REPORT, 4, params);
      }
    }

    
    m_setFileRecalledStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setFileRecalled"
      << std::endl << e.what();
    throw ex;
  }

}
  


//----------------------------------------------------------------------------
// getFailedMigrations
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFailedMigrations(std::list<castor::tape::tapegateway::RetryPolicyElement>& candidates)
	  throw (castor::exception::Exception)
{
    
  try {
    // Check whether the statements are ok
    if (0 == m_getFailedMigrationsStatement) {
      m_getFailedMigrationsStatement =
        createStatement(s_getFailedMigrationsStatementString);
      m_getFailedMigrationsStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getFailedMigrationsStatement->executeUpdate();

    if (0 == nb) {
      cnvSvc()->commit(); 
      return;
    }


    oracle::occi::ResultSet *rs =
      m_getFailedMigrationsStatement->getCursor(1);

    // Run through the cursor 

   
    while (rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::tape::tapegateway::RetryPolicyElement item;
      item.tapeCopyId=(u_signed64)rs->getDouble(2);
      item.errorCode=rs->getInt(5);
      item.nbRetry=rs->getInt(6);
      candidates.push_back(item);
    }

    m_getFailedMigrationsStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFailedMigrations"
      << std::endl << e.what();
    throw ex;
  }


}


//----------------------------------------------------------------------------
// setMigRetryResult
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::setMigRetryResult(const std::list<u_signed64>& tcToRetry, const std::list<u_signed64>&  tcToFail ) throw (castor::exception::Exception) {

  
 unsigned char (*bufferRetry)[21]=NULL;
 ub2 *lensRetry=NULL;
 unsigned char (*bufferFail)[21]=NULL;
 ub2 *lensFail = NULL;

  try { 
    // Check whether the statements are ok
    if (0 == m_setMigRetryResultStatement) {
      m_setMigRetryResultStatement =
        createStatement(s_setMigRetryResultStatementString);
    }

    // success

    ub4 nbRetry= tcToRetry.size();
    nbRetry=nbRetry==0?1:nbRetry;
    bufferRetry=(unsigned char(*)[21]) calloc((nbRetry) * 21, sizeof(unsigned char));
    lensRetry=(ub2 *)malloc (sizeof(ub2)*nbRetry);

    if ( lensRetry == 0 || bufferRetry == 0 ) {
      if (lensRetry != 0 ) free(lensRetry);
      if (bufferRetry != 0) free(bufferRetry);
      castor::exception::OutOfMemory e; 
      throw e;
    }

    int i=0;
    
    for (std::list<u_signed64>::const_iterator elem= tcToRetry.begin(); 
	 elem != tcToRetry.end();
	 elem++, i++){
	oracle::occi::Number n = (double)(*elem);
	oracle::occi::Bytes b = n.toBytes();
	b.getBytes(bufferRetry[i],b.length());
	lensRetry[i] = b.length();
    }

    // if there where no successfull tapecopy
    if (tcToRetry.size() == 0){
      //let's put -1
      oracle::occi::Number n = (double)(-1);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferRetry[0],b.length());
      lensRetry[0] = b.length();
    }

    ub4 unusedRetry =nbRetry;
    m_setMigRetryResultStatement->setDataBufferArray(1,bufferRetry, oracle::occi::OCCI_SQLT_NUM, nbRetry, &unusedRetry, 21, lensRetry);

    // failures

    ub4 nbFail = tcToFail.size();
    nbFail = nbFail == 0 ? 1 : nbFail; 
    bufferFail=(unsigned char(*)[21]) calloc((nbFail) * 21, sizeof(unsigned char));
    lensFail = (ub2 *)malloc (sizeof(ub2)*nbFail);

    if ( lensFail == 0 || bufferFail == 0 ) {
      if (lensFail != 0 ) free(lensFail);
      if (bufferFail != 0) free(bufferFail);
      if (lensRetry != 0 ) free(lensRetry);
      if (bufferRetry != 0) free(bufferRetry);
      castor::exception::OutOfMemory e; 
      throw e;
    }
    
    i=0;

    for (std::list<u_signed64>::const_iterator elem=tcToFail.begin();
	  elem != tcToFail.end();
	  elem++,i++){
      
	oracle::occi::Number n = (double)(*elem);
	oracle::occi::Bytes b = n.toBytes();
	b.getBytes(bufferFail[i],b.length());
	lensFail[i] = b.length();

    }

    // if there where no failed tapecopy

    if (tcToFail.size() == 0){
      //let's put -1
      oracle::occi::Number n = (double)(-1);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferFail[0],b.length());
      lensFail[0] = b.length();
    }

    ub4 unusedFail = nbFail;
    m_setMigRetryResultStatement->setDataBufferArray(2,bufferFail, oracle::occi::OCCI_SQLT_NUM, nbFail, &unusedFail, 21, lensFail);

    m_setMigRetryResultStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {

    if (lensFail != 0 ) free(lensFail);
    if (bufferFail != 0) free(bufferFail);
    if (lensRetry != 0 ) free(lensRetry);
    if (bufferRetry != 0) free(bufferRetry);

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setMigRetryResult"
      << std::endl << e.what();
    throw ex;
  }

  if (lensFail != 0 ) free(lensFail);
  if (bufferFail != 0) free(bufferFail);
  if (lensRetry != 0 ) free(lensRetry);
  if (bufferRetry != 0) free(bufferRetry);

}


//----------------------------------------------------------------------------
// getFailedRecalls
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFailedRecalls( std::list<castor::tape::tapegateway::RetryPolicyElement>& candidates)
	  throw (castor::exception::Exception){
  
 
  try {
    // Check whether the statements are ok
    if (0 == m_getFailedRecallsStatement) {
      m_getFailedRecallsStatement =
        createStatement(s_getFailedRecallsStatementString);
      m_getFailedRecallsStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getFailedRecallsStatement->executeUpdate();

    if (0 == nb) {
      cnvSvc()->commit(); 
      return;
    }


   oracle::occi::ResultSet *rs =
      m_getFailedRecallsStatement->getCursor(1);

    // Run through the cursor 

    while (rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {   
      castor::tape::tapegateway::RetryPolicyElement item;
      item.tapeCopyId=(u_signed64)rs->getDouble(2);
      item.errorCode=rs->getInt(5);
      item.nbRetry=rs->getInt(6);
      candidates.push_back(item);
     
    }

    m_getFailedRecallsStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFailedRecalls"
      << std::endl << e.what();
    throw ex;
  }


}

//----------------------------------------------------------------------------
// setRecRetryResult
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::setRecRetryResult(const std::list<u_signed64>& tcToRetry,const std::list<u_signed64>& tcToFail) throw (castor::exception::Exception) {


  unsigned char (*bufferRetry)[21]=NULL;
  ub2 *lensRetry=NULL;
  unsigned char (*bufferFail)[21]=NULL;
  ub2 *lensFail = NULL;


 
  try {
     
    // Check whether the statements are ok
    if (0 == m_setRecRetryResultStatement) {
      m_setRecRetryResultStatement =
        createStatement(s_setRecRetryResultStatementString);
    }

    // success

    ub4 nbRetry= tcToRetry.size();
    nbRetry=nbRetry==0?1:nbRetry;
    bufferRetry=(unsigned char(*)[21]) calloc((nbRetry) * 21, sizeof(unsigned char));
    lensRetry=(ub2 *)malloc (sizeof(ub2)*nbRetry);
   
    if ( lensRetry == 0 || bufferRetry == 0 ) {
      if (lensRetry != 0 ) free(lensRetry);
      if (bufferRetry != 0) free(bufferRetry);
      castor::exception::OutOfMemory e; 
      throw e;
    }

    int i=0;
    for (std::list<u_signed64>::const_iterator elem=tcToRetry.begin();
	 elem != tcToRetry.end();
	 elem++, i++){
      
      oracle::occi::Number n = (double)(*elem);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferRetry[i],b.length());
      lensRetry[i] = b.length();
 

    }
  
    if (tcToRetry.size() == 0){
      //let's put -1
      oracle::occi::Number n = (double)(-1);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferRetry[0],b.length());
      lensRetry[0] = b.length();
    }
    

    ub4 unused=nbRetry;
    m_setRecRetryResultStatement->setDataBufferArray(1,bufferRetry, oracle::occi::OCCI_SQLT_NUM, nbRetry, &unused, 21, lensRetry);

    // failure 
    

    ub4 nbFail= tcToFail.size();
    nbFail=nbFail==0?1:nbFail;
    bufferFail = (unsigned char(*)[21]) calloc((nbFail) * 21, sizeof(unsigned char));
    lensFail = (ub2 *)malloc (sizeof(ub2)*nbFail);

    
    if ( lensFail == 0 || bufferFail == 0 ) {
      if (lensFail != 0 ) free(lensFail);
      if (bufferFail != 0) free(bufferFail);
      if (lensRetry != 0 ) free(lensRetry);
      if (bufferRetry != 0) free(bufferRetry);
      castor::exception::OutOfMemory e; 
      throw e;
    }
    
    i=0;

    for (std::list<u_signed64>::const_iterator elem=tcToFail.begin();
	 elem != tcToFail.end();
	 elem++, i++){
      oracle::occi::Number n = (double)(*elem);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferFail[i],b.length());
      lensFail[i] = b.length();
     
    }

    
    if (tcToFail.size() == 0){
      //let's put -1
      oracle::occi::Number n = (double)(-1);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferFail[0],b.length());
      lensFail[0] = b.length();
    }

    unused=nbFail;
    m_setRecRetryResultStatement->setDataBufferArray(2,bufferFail, oracle::occi::OCCI_SQLT_NUM, nbFail, &unused, 21, lensFail);
    m_setRecRetryResultStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {

    if (lensFail != 0 ) free(lensFail);
    if (bufferFail != 0) free(bufferFail);
    if (lensRetry != 0 ) free(lensRetry);
    if (bufferRetry != 0) free(bufferRetry);

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setRecRetryResult"
      << std::endl << e.what();
    throw ex;
  }

  if (lensFail != 0 ) free(lensFail);
  if (bufferFail != 0) free(bufferFail);
  if (lensRetry != 0 ) free(lensRetry);
  if (bufferRetry != 0) free(bufferRetry);


}


//----------------------------------------------------------------------------
// getRepackVidAndFileInfo
//----------------------------------------------------------------------------
  
void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getRepackVidAndFileInfo(const castor::tape::tapegateway::FileMigratedNotification& resp, std::string& vid, int& copyNumber, u_signed64& lastModificationTime, std::string& repackVid )
  throw (castor::exception::Exception){
  
  try {
    // Check whether the statements are ok

    if (0 == m_getRepackVidAndFileInfoStatement) {
      m_getRepackVidAndFileInfoStatement =
        createStatement(s_getRepackVidAndFileInfoStatementString);
      m_getRepackVidAndFileInfoStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048 );
      m_getRepackVidAndFileInfoStatement->registerOutParam
        (7, oracle::occi::OCCISTRING, 2048 );
      m_getRepackVidAndFileInfoStatement->registerOutParam
        (8, oracle::occi::OCCIINT);
      m_getRepackVidAndFileInfoStatement->registerOutParam
	(9, oracle::occi::OCCIDOUBLE);
      m_getRepackVidAndFileInfoStatement->registerOutParam
        (10, oracle::occi::OCCIINT);
    }

    m_getRepackVidAndFileInfoStatement->setDouble(1,(double)resp.fileid());
    m_getRepackVidAndFileInfoStatement->setString(2,resp.nshost());
    
    m_getRepackVidAndFileInfoStatement->setInt(3,resp.fseq());

    m_getRepackVidAndFileInfoStatement->setDouble(4,resp.mountTransactionId());
    m_getRepackVidAndFileInfoStatement->setDouble(5,resp.fileSize());
    
    // execute the statement 

    m_getRepackVidAndFileInfoStatement->executeUpdate();
    
    int ret=m_getRepackVidAndFileInfoStatement->getInt(10);
    if (ret==-1){
      //wrong file size
      castor::exception::Exception ex(ERTWRONGSIZE);
      ex.getMessage()<<"wrong file size given: "<<resp.fileSize();
      throw ex;
    }
      
    repackVid = m_getRepackVidAndFileInfoStatement->getString(6);
    vid = m_getRepackVidAndFileInfoStatement->getString(7);
    copyNumber = m_getRepackVidAndFileInfoStatement->getInt(8);
    lastModificationTime= (u_signed64)m_getRepackVidAndFileInfoStatement->getDouble(9);


  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getRepackVid"
      << std::endl << e.what();
    throw ex;
  }
  
}

//--------------------------------------------------------------------------
// startTapeSession
//-------------------------------------------------------------------------- 
	 

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::startTapeSession(const castor::tape::tapegateway::VolumeRequest& startRequest, castor::tape::tapegateway::Volume& volume ) throw (castor::exception::Exception){ 
  
  try {
    // Check whether the statements are ok
    if (0 == m_startTapeSessionStatement) {
      m_startTapeSessionStatement =
        createStatement(s_startTapeSessionStatementString);
      m_startTapeSessionStatement->registerOutParam
        (2, oracle::occi::OCCISTRING, 2048 );
      m_startTapeSessionStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_startTapeSessionStatement->registerOutParam
        (4, oracle::occi::OCCIINT);
      m_startTapeSessionStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048 );
      m_startTapeSessionStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048 );
      
    }

    m_startTapeSessionStatement->setDouble(1,(double)startRequest.mountTransactionId());


    // execute the statement
 
    m_startTapeSessionStatement->executeUpdate();


    int ret = m_startTapeSessionStatement->getInt(4);

    if (ret==-1) {
      //NOMOREFILES
      return;
    }
    if (ret == -2) {
      // UNKNOWN request
      castor::exception::Exception e(EINVAL);
      throw e;
    }
   
    
    volume.setClientType(TAPE_GATEWAY);
    volume.setVid(m_startTapeSessionStatement->getString(2));
    volume.setMode((castor::tape::tapegateway::VolumeMode)m_startTapeSessionStatement->getInt(3));
    volume.setDensity(m_startTapeSessionStatement->getString(5));
    volume.setLabel(m_startTapeSessionStatement->getString(6));
    volume.setMountTransactionId(startRequest.mountTransactionId());
    

  } catch (oracle::occi::SQLException e) {
    

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in startTapeSession"
      << std::endl << e.what();
    throw ex;
  }



} 

//----------------------------------------------------------------------------
// endTapeSession
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::endTapeSession(const castor::tape::tapegateway::EndNotification& endRequest) throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok

    if (0 == m_endTapeSessionStatement) {
      m_endTapeSessionStatement =
        createStatement(s_endTapeSessionStatementString);

    }

    m_endTapeSessionStatement->setDouble(1,(double)endRequest.mountTransactionId()); 
    m_endTapeSessionStatement->setInt(2,0);
    
    m_endTapeSessionStatement->executeUpdate();


  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in endTapeSession"
      << std::endl << e.what();
    throw ex;
  }
  
}

//----------------------------------------------------------------------------
// getSegmentInfo
//----------------------------------------------------------------------------
		
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getSegmentInfo(const FileRecalledNotification &fileRecalled, std::string& vid,int& copyNb ) throw (castor::exception::Exception){
  
 try {
    // Check whether the statements are ok

    if (0 == m_getSegmentInfoStatement) {
      m_getSegmentInfoStatement =
        createStatement(s_getSegmentInfoStatementString);
      m_getSegmentInfoStatement->registerOutParam
	(5, oracle::occi::OCCISTRING, 2048 ); 
      m_getSegmentInfoStatement->registerOutParam
	(6, oracle::occi::OCCIINT); 
    }

    m_getSegmentInfoStatement->setDouble(1,(double)fileRecalled.mountTransactionId()); 
    m_getSegmentInfoStatement->setDouble(2,(double)fileRecalled.fileid());
    m_getSegmentInfoStatement->setString(3,fileRecalled.nshost());
    m_getSegmentInfoStatement->setInt(4,fileRecalled.fseq());
    
    m_getSegmentInfoStatement->executeUpdate();

    vid= m_getSegmentInfoStatement->getString(5);
    copyNb=m_getSegmentInfoStatement->getInt(6);

  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getSegmentInfo"
      << std::endl << e.what();
    throw ex;
  }


}

	
//----------------------------------------------------------------------------
// failTapeSession
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::failTapeSession(const castor::tape::tapegateway::EndNotificationErrorReport& failure) throw (castor::exception::Exception){
  try {
    // Check whether the statements are ok

    if (0 == m_endTapeSessionStatement) {
      m_endTapeSessionStatement =
        createStatement(s_endTapeSessionStatementString);

    }

    m_endTapeSessionStatement->setDouble(1,(double)failure.mountTransactionId()); 
    m_endTapeSessionStatement->setInt(2,failure.errorCode());
    
    m_endTapeSessionStatement->executeUpdate();


  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in failTapeSession"
      << std::endl << e.what();
    throw ex;
  }
  
}


//----------------------------------------------------------------------------
// failFileTransfer
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::failFileTransfer(const FileErrorReport& failure) throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok

    if (0 == m_failFileTransferStatement) {
      m_failFileTransferStatement =
        createStatement(s_failFileTransferStatementString);
      m_failFileTransferStatement->setAutoCommit(true);
    }

    m_failFileTransferStatement->setDouble(1,(double)failure.mountTransactionId()); 
    m_failFileTransferStatement->setDouble(2,(double)failure.fileid());
    m_failFileTransferStatement->setString(3,failure.nshost());
    m_failFileTransferStatement->setInt(4,failure.fseq());
    m_failFileTransferStatement->setInt(5,failure.errorCode());
    
    m_failFileTransferStatement->executeUpdate();
   
  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in failFileTransfer"
      << std::endl << e.what();
    throw ex;
  }
}



//----------------------------------------------------------------------------
// invalidateFile
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::invalidateFile(const FileErrorReport& failure) throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok

    if (0 == m_invalidateFileStatement) {
      m_invalidateFileStatement =
        createStatement(s_invalidateFileStatementString);
      m_invalidateFileStatement->setAutoCommit(true);
    }

    m_invalidateFileStatement->setDouble(1,(double)failure.mountTransactionId()); 
    m_invalidateFileStatement->setDouble(2,(double)failure.fileid());
    m_invalidateFileStatement->setString(3,failure.nshost());
    m_invalidateFileStatement->setInt(4,failure.fseq());
    m_invalidateFileStatement->setInt(5,failure.errorCode());
    
    m_invalidateFileStatement->executeUpdate();
 

  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in invalidateFile"
      << std::endl << e.what();
    throw ex;
  }
}




//----------------------------------------------------------------------------
// getTapeToRelease
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapeToRelease(const u_signed64& mountTransactionId, castor::stager::Tape& tape ) throw (castor::exception::Exception){
  
  try {
    // Check whether the statements are ok

    if (0 == m_getTapeToReleaseStatement) {
      m_getTapeToReleaseStatement =
        createStatement(s_getTapeToReleaseStatementString);
      m_getTapeToReleaseStatement->registerOutParam
	      (2, oracle::occi::OCCISTRING, 2048 ); 
      m_getTapeToReleaseStatement->registerOutParam
	      (3, oracle::occi::OCCIINT); 
    }

    m_getTapeToReleaseStatement->setDouble(1,(double)mountTransactionId); 
    
    m_getTapeToReleaseStatement->executeUpdate();

    tape.setVid(m_getTapeToReleaseStatement->getString(2));
    tape.setTpmode(m_getTapeToReleaseStatement->getInt(3));

  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getTapeToRelease"
      << std::endl << e.what();
    throw ex;
  }

}


//----------------------------------------------------------------------------
// endTransaction
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::endTransaction() throw (castor::exception::Exception){
  cnvSvc()->commit();
}

//----------------------------------------------------------------------------
// checkConfiguration
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::checkConfiguration() throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok
    if (0 == m_checkConfigurationStatement) {
      m_checkConfigurationStatement =
        createStatement(s_checkConfigurationStatementString);
      m_checkConfigurationStatement->setAutoCommit(true);
    }
    // Execute the statement
    (void)m_checkConfigurationStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    if (1403 == e.getErrorCode()) {
      // no data found, I should run the tapegateway instead of rtcpclientd
      castor::exception::Internal e;
      e.getMessage()
	<< "the tape gateway cannot be run with such configuration, use rtcpclientd instead"
	<< std::endl;
      throw e;
    }
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in checkConfiguration."
      << std::endl << e.what();
    throw ex;
  }

  
}

//----------------------------------------------------------------------------
// deleteStreamWithBadTapePool
//----------------------------------------------------------------------------



void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::deleteStreamWithBadTapePool(const castor::stager::Stream& stream) 
  throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok
    if (0 == m_deleteStreamWithBadTapePoolStatement) {
      m_deleteStreamWithBadTapePoolStatement =
        createStatement(s_deleteStreamWithBadTapePoolStatementString);
    }

    m_deleteStreamWithBadTapePoolStatement->setDouble(1,(double)stream.id());

    // execute the statement and see whether we found something
 
    m_deleteStreamWithBadTapePoolStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in deleteStreamWithBadTapePool"
      << std::endl << e.what();
    throw ex;
  }

}
 
//----------------------------------------------------------------------------
// deleteTapeRequest
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::deleteTapeRequest(const u_signed64& tapeRequestId)throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok
    if (0 == m_deleteTapeRequestStatement) {
      m_deleteTapeRequestStatement =
        createStatement(s_deleteTapeRequestStatementString);
    }

    m_deleteTapeRequestStatement->setDouble(1,(double)tapeRequestId);

    // execute the statement and see whether we found something
 
    m_deleteTapeRequestStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in deleteTapeRequest"
      << std::endl << e.what();
    throw ex;
  }


}
