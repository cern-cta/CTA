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


#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"

#include "castor/BaseAddress.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/tape/tapegateway/ora/OraTapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/NsFileInformation.hpp"
#include "castor/tape/tapegateway/FileDiskLocation.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/TapeStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/tape/tapegateway/TapeFileNsAttribute.hpp"
#include "occi.h"
#include <string>
#include <sstream>
#include <vector>
#include <serrno.h>


//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::tape::tapegateway::ora::OraTapeGatewaySvc>* s_factoryOraTapeGatewaySvc =
  new castor::SvcFactory<castor::tape::tapegateway::ora::OraTapeGatewaySvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL statement for function getStreamsToResolve
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getStreamsToResolveStatementString= "BEGIN getStreamsToResolve(:1);END;";

/// SQL statement for function resolveStream
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_resolveStreamsStatementString="BEGIN resolveStreams(:1,:2);END;";

/// SQL statement for function getTapesToSubmit
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapesToSubmitStatementString="BEGIN getTapesToSubmit(:1);END;";

/// SQL statement for function updateSubmittedTapes
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateSubmittedTapesStatementString="BEGIN updateSubmittedTapes(:1,:2);END;";

/// SQL statement for function getTapesToCheck
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapesToCheckStatementString="BEGIN getTapesToCheck(:1,:2);END;";

/// SQL statement for function updateCheckedTapes
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateCheckedTapesStatementString="BEGIN updateCheckedTapes(:1);END;";
  
/// SQL statement for function fileToMigrate
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_fileToMigrateStatementString="BEGIN fileToMigrate(:1,:2,:3,:4,:5,:6,:7,:8);END;";
  
/// SQL statement for function fileMigrationUpdate
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_fileMigrationUpdateStatementString="BEGIN fileMigrationUpdate(:1,:2,:3,:4);END;";

/// SQL statement for function fileToRecall
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_fileToRecallStatementString="BEGIN fileToRecall(:1,:2,:3,:4,:5,:6);END;";

/// SQL statement for function fileRecallUpdate
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_fileRecallUpdateStatementString="BEGIN fileRecallUpdate(:1,:2,:3,:4,:5,:6,:7);END;";

/// SQL statement for function inputForMigrationRetryPolicy 
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_inputForMigrationRetryPolicyStatementString="BEGIN inputForMigrationRetryPolicy(:1);END;";
 
/// SQL statement for function updateWithMigrationRetryPolicyResult   
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateWithMigrationRetryPolicyResultStatementString="BEGIN updateMigRetryPolicy(:1,:2);END;";

/// SQL statement for function inputForRecallRetryPolicy
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_inputForRecallRetryPolicyStatementString="BEGIN inputForRecallRetryPolicy(:1);END;";

/// SQL statement for function updateWithRecallRetryPolicyResult   
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateWithRecallRetryPolicyResultStatementString="BEGIN updateRecRetryPolicy(:1,:2);END;";

/// SQL statement for function checkFileForRepack   
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getRepackVidStatementString="BEGIN getRepackVid(:1,:2,:3);END;";

/// SQL statement for function updateDbStartTape
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateDbStartTapeStatementString="BEGIN updateDbStartTape(:1,:2,:3,:4);END;";

/// SQL statement for function updateDbEndTape
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateDbEndTapeStatementString="BEGIN updateDbEndTape(:1,:2);END;";

/// SQL statement for function invalidateSegment
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_invalidateSegmentStatementString="BEGIN invalidateSegment(:1,:2,:3);END;";

// SQL statement object for function tapeGatewayCleanUp
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_tapeGatewayCleanUpStatementString="BEGIN  tapeGatewayCleanUp(:1);END;";


//------------------------------------------------------------------------------
// OraTapeGatewaySvc
//------------------------------------------------------------------------------
castor::tape::tapegateway::ora::OraTapeGatewaySvc::OraTapeGatewaySvc(const std::string name) :
  ITapeGatewaySvc(),
  OraCommonSvc(name),
  m_getStreamsToResolveStatement(0),
  m_resolveStreamsStatement(0),
  m_getTapesToSubmitStatement(0),
  m_updateSubmittedTapesStatement(0),
  m_getTapesToCheckStatement(0),
  m_updateCheckedTapesStatement(0),
  m_fileToMigrateStatement(0),
  m_fileMigrationUpdateStatement(0),
  m_fileToRecallStatement(0),
  m_fileRecallUpdateStatement(0),
  m_inputForMigrationRetryPolicyStatement(0),
  m_updateWithMigrationRetryPolicyResultStatement(0),
  m_inputForRecallRetryPolicyStatement(0),
  m_updateWithRecallRetryPolicyResultStatement(0),
  m_getRepackVidStatement(0),
  m_updateDbStartTapeStatement(0),
  m_updateDbEndTapeStatement(0),
  m_invalidateSegmentStatement(0),
  m_tapeGatewayCleanUpStatement(0){
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
const unsigned int castor::tape::tapegateway::ora::OraTapeGatewaySvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
const unsigned int castor::tape::tapegateway::ora::OraTapeGatewaySvc::ID() {
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

    if ( m_getStreamsToResolveStatement) deleteStatement(m_getStreamsToResolveStatement);
    if ( m_resolveStreamsStatement) deleteStatement(m_resolveStreamsStatement);
    if ( m_getTapesToSubmitStatement) deleteStatement( m_getTapesToSubmitStatement);
    if ( m_updateSubmittedTapesStatement ) deleteStatement( m_updateSubmittedTapesStatement);
    if ( m_getTapesToCheckStatement) deleteStatement(m_getTapesToCheckStatement);
    if ( m_updateCheckedTapesStatement) deleteStatement(m_updateCheckedTapesStatement);
    if ( m_fileToMigrateStatement) deleteStatement(m_fileToMigrateStatement);
    if ( m_fileMigrationUpdateStatement) deleteStatement( m_fileMigrationUpdateStatement);
    if ( m_fileToRecallStatement) deleteStatement( m_fileToRecallStatement);
    if ( m_fileRecallUpdateStatement) deleteStatement( m_fileRecallUpdateStatement);
    if ( m_inputForMigrationRetryPolicyStatement) deleteStatement(m_inputForMigrationRetryPolicyStatement);
    if ( m_updateWithMigrationRetryPolicyResultStatement) deleteStatement(m_updateWithMigrationRetryPolicyResultStatement);
    if ( m_inputForRecallRetryPolicyStatement) deleteStatement(m_inputForRecallRetryPolicyStatement);
    if ( m_updateWithRecallRetryPolicyResultStatement) deleteStatement(m_updateWithRecallRetryPolicyResultStatement);
    if ( m_getRepackVidStatement) deleteStatement(m_getRepackVidStatement);
    if ( m_updateDbStartTapeStatement) deleteStatement(m_updateDbStartTapeStatement);
    if ( m_updateDbEndTapeStatement) deleteStatement(m_updateDbEndTapeStatement);
    if ( m_invalidateSegmentStatement) deleteStatement(m_invalidateSegmentStatement);
    if ( m_tapeGatewayCleanUpStatement) deleteStatement( m_tapeGatewayCleanUpStatement);

  } catch (oracle::occi::SQLException e) {};

  // Now reset all pointers to 0

  m_getStreamsToResolveStatement = 0;
  m_resolveStreamsStatement = 0;
  m_getTapesToSubmitStatement = 0;
  m_updateSubmittedTapesStatement = 0;
  m_getTapesToCheckStatement = 0;
  m_updateCheckedTapesStatement = 0;
  m_fileToMigrateStatement = 0;
  m_fileMigrationUpdateStatement = 0;
  m_fileToRecallStatement = 0;
  m_fileRecallUpdateStatement = 0;
  m_inputForMigrationRetryPolicyStatement = 0;
  m_updateWithMigrationRetryPolicyResultStatement = 0;
  m_inputForRecallRetryPolicyStatement = 0;
  m_updateWithRecallRetryPolicyResultStatement = 0;
  m_getRepackVidStatement = 0;
  m_updateDbStartTapeStatement = 0;
  m_updateDbEndTapeStatement = 0; 
  m_invalidateSegmentStatement = 0;
  m_tapeGatewayCleanUpStatement = 0;
}

//----------------------------------------------------------------------------
// getStreamsToResolve
//----------------------------------------------------------------------------

std::vector<castor::stager::Stream*> castor::tape::tapegateway::ora::OraTapeGatewaySvc::getStreamsToResolve()
  throw (castor::exception::Exception){

  std::vector<castor::stager::Stream*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_getStreamsToResolveStatement) {
      m_getStreamsToResolveStatement =
        createStatement(s_getStreamsToResolveStatementString);
      m_getStreamsToResolveStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getStreamsToResolveStatement->executeUpdate();

    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "stream to resolve: no stream found";
      throw ex;
    }

    oracle::occi::ResultSet *rs =
      m_getStreamsToResolveStatement->getCursor(1);

    // Run through the cursor
    oracle::occi::ResultSet::Status status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::stager::Stream* item =
        new castor::stager::Stream();
      item->setId((u_signed64)rs->getDouble(1));
      item->setInitialSizeToTransfer((u_signed64)rs->getDouble(2));
      item->setStatus((castor::stager::StreamStatusCodes)rs->getInt(3));
      castor::stager::TapePool* tp=new castor::stager::TapePool();
      tp->setId((u_signed64)rs->getDouble(4));
      tp->setName(rs->getString(5));
      item->setTapePool(tp);
      result.push_back(item);
      status = rs->next();
    }
    m_getStreamsToResolveStatement->closeResultSet(rs);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getStreamsToResolve"
      << std::endl << e.what();
    throw ex;
  }
  return result;
}

//----------------------------------------------------------------------------
// resolveStreams 
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::resolveStreams(std::vector<u_signed64> strIds, std::vector<std::string> vids)
          throw (castor::exception::Exception){

  try {

    if ( !strIds.size() || !vids.size() || vids.size() != strIds.size()) {
        castor::exception::Internal ex;
	ex.getMessage() << "invalid input for resolveStreams"
			<< std::endl; 
	throw ex;

     }
     
    // Check whether the statements are ok
    if (0 == m_resolveStreamsStatement) {
      m_resolveStreamsStatement =
        createStatement(s_resolveStreamsStatementString);
    }

    // input

     ub4 nb=strIds.size();
     unsigned char (*bufferStrIds)[21]=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
     char * bufferVids=NULL;

     ub2 *lensStrIds=(ub2 *)malloc (sizeof(ub2)*nb);
     ub2 *lensVids=NULL;

     // get the maximum cell size
     unsigned int maxLen=0;
     unsigned int numTapes=0;
     for (unsigned int i=0;i<vids.size();i++){
	maxLen=maxLen > vids.at(i).length()?maxLen:vids.at(i).length();
	numTapes++;
     }

     if (maxLen == 0) {
        castor::exception::Internal ex;
	ex.getMessage() << "invalid VID in resolveStreams"
			<< std::endl;
	throw ex;

     }
     

     unsigned int bufferCellSize = maxLen * sizeof(char);
     lensVids = (ub2*) malloc(maxLen * sizeof(ub2));
     bufferVids =
	(char*) malloc(numTapes * bufferCellSize);



     // DataBuffer with all the vid (one for each subrequest)
     

     // Fill in the structure

     for (unsigned int i=0;i<vids.size();i++){

       // stream Ids
       
       oracle::occi::Number n = (double)(strIds[i]);
       oracle::occi::Bytes b = n.toBytes();
       b.getBytes(bufferStrIds[i],b.length());
       lensStrIds[i] = b.length();
        
       // vids
       
       lensVids[i]= vids[i].length();
       strncpy(bufferVids+(bufferCellSize*i),vids[i].c_str(),lensVids[i]);
	
     }

     ub4 unused=nb;
     m_resolveStreamsStatement->setDataBufferArray(1,bufferStrIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensStrIds);
     ub4 len=nb;
     m_resolveStreamsStatement->setDataBufferArray(2, bufferVids, oracle::occi::OCCI_SQLT_CHR,len, &len, maxLen, lensVids);

     // execute the statement and see whether we found something
 
     m_resolveStreamsStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in resolveStreams"
      << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// getTapesToSubmit 
//----------------------------------------------------------------------------

std::vector<castor::tape::tapegateway::TapeRequestState*> castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapesToSubmit()
          throw (castor::exception::Exception){

  std::vector<castor::tape::tapegateway::TapeRequestState*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_getTapesToSubmitStatement) {
      m_getTapesToSubmitStatement =
        createStatement(s_getTapesToSubmitStatementString);
      m_getTapesToSubmitStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getTapesToSubmitStatement->executeUpdate();

    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "stream to submit : no stream found";
      throw ex;
    }

    oracle::occi::ResultSet *rs =
      m_getTapesToSubmitStatement->getCursor(1);

    // Run through the cursor
    oracle::occi::ResultSet::Status status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::stager::Tape* tape= new  castor::stager::Tape();
      tape->setTpmode(rs->getInt(1));
      tape->setSide(rs->getInt(2));
      tape->setVid(rs->getString(3));
      castor::tape::tapegateway::TapeRequestState* item= new  castor::tape::tapegateway::TapeRequestState();
      item->setId((u_signed64)rs->getDouble(4));
      if (tape->tpmode() == 0){
	item->setAccessMode(0);
	item->setTapeRecall(tape);
      } else {
	item->setAccessMode(1);
	castor::stager::Stream* stream= new castor::stager::Stream();
	stream->setTape(tape);
	item->setStreamMigration(stream);
      }
      result.push_back(item);
      status = rs->next();
    }

    m_getTapesToSubmitStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getTapesToSubmit"
      << std::endl << e.what();
    throw ex;
  }
  return result;
}

//----------------------------------------------------------------------------
// updateSubmittedTapes
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateSubmittedTapes(std::vector<castor::tape::tapegateway::TapeRequestState*> tapeRequests){ 

  try {

    if ( !tapeRequests.size() ) {
        castor::exception::Internal ex;
	ex.getMessage() << "nothing to update"
			<< std::endl; 
	throw ex;

     }
     
    // Check whether the statements are ok
    if (0 == m_updateSubmittedTapesStatement) {
      m_updateSubmittedTapesStatement =
        createStatement(s_updateSubmittedTapesStatementString);
    }

    // input
     ub4 nb=tapeRequests.size();


     // id
     
     unsigned char (*bufferIds)[21]=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
     ub2 *lensIds=(ub2 *)malloc (sizeof(ub2)*nb);
 

      // vdqmId
     
     unsigned char (*bufferVdqmIds)[21]=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
     ub2 *lensVdqmIds=(ub2 *)malloc (sizeof(ub2)*nb);

     // DataBuffer with all the vid (one for each subrequest)

     
     // Fill in the structure

     for (unsigned int i=0;i<tapeRequests.size();i++){

	
       // ids

       oracle::occi::Number n = (double)(tapeRequests[i]->id());
       oracle::occi::Bytes b = n.toBytes();
       b.getBytes(bufferIds[i],b.length());
       lensIds[i] = b.length();

       // vdqmIds

       n = (double)(tapeRequests[i]->vdqmVolReqId());
       b = n.toBytes();
       b.getBytes(bufferVdqmIds[i],b.length());
       lensVdqmIds[i] = b.length();        

     }

     ub4 unused=nb;

     m_updateSubmittedTapesStatement->setDataBufferArray(1,bufferIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensIds);
     m_updateSubmittedTapesStatement->setDataBufferArray(2, bufferVdqmIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensVdqmIds);

     // execute the statement and see whether we found something

     m_updateSubmittedTapesStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateSubmittedTapes"
      << std::endl << e.what();
    throw ex;
  }
  
}

//----------------------------------------------------------------------------
// getTapesToCheck
//----------------------------------------------------------------------------

std::vector<castor::tape::tapegateway::TapeRequestState*> castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapesToCheck(u_signed64 timeOut) 
  throw (castor::exception::Exception){

  std::vector<tape::tapegateway::TapeRequestState*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_getTapesToSubmitStatement) {
      m_getTapesToCheckStatement =
        createStatement(s_getTapesToCheckStatementString);
      m_getTapesToCheckStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR);
    }

    m_getTapesToCheckStatement->setDouble(1,(double)timeOut);

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getTapesToCheckStatement->executeUpdate();

    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "tapes to check : no tape found";
      throw ex;
    }

    oracle::occi::ResultSet *rs =
      m_getTapesToCheckStatement->getCursor(2);

    // Run through the cursor
    oracle::occi::ResultSet::Status status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      tape::tapegateway::TapeRequestState* item =
        new tape::tapegateway::TapeRequestState();
      item->setAccessMode(rs->getInt(1));
      item->setId((u_signed64)rs->getDouble(2));
      item->setStartTime((u_signed64)rs->getDouble(3));
      item->setLastVdqmPingTime((u_signed64)rs->getDouble(4));
      item->setVdqmVolReqId((u_signed64)rs->getDouble(5));
      item->setStatus((TapeRequestStateCode)rs->getInt(6));
      
      castor::stager::Tape* tp=new castor::stager::Tape(); 
      tp->setVid(rs->getString(7));

      if (item->accessMode() == 0) {
	// read
	item->setTapeRecall(tp);

      } else {
	//write 
	castor::stager::Stream* str=new castor::stager::Stream();
	item->setStreamMigration(str);
	str->setTape(tp);
      }
      result.push_back(item);
      status = rs->next();
    }

    m_getTapesToCheckStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getTapesToCheck"
      << std::endl << e.what();
    throw ex;
  }
  return result;

}
 


//----------------------------------------------------------------------------
// updateCheckedTapes 
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateCheckedTapes(std::vector<castor::tape::tapegateway::TapeRequestState*> tapes)
          throw (castor::exception::Exception){

  try {

    if ( !tapes.size()) {
        castor::exception::Internal ex;
	ex.getMessage() << "invalid input for updateCheckedTapes"
			<< std::endl; 
	throw ex;

     }
     
    // Check whether the statements are ok
    if (0 == m_updateCheckedTapesStatement) {
      m_updateCheckedTapesStatement =
        createStatement(s_updateCheckedTapesStatementString);
    }

    // input

    ub4 nb=tapes.size();
    unsigned char (*bufferTapes)[21]=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    
    ub2 *lensTapes=(ub2 *)malloc (sizeof(ub2)*nb);

    // Fill in the structure

    for (unsigned int i=0;i<tapes.size();i++){

      oracle::occi::Number n = (double)(tapes[i]->id());
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferTapes[i],b.length());
      lensTapes[i] = b.length();

    }

    ub4 unused=nb;
    m_updateCheckedTapesStatement->setDataBufferArray(1,bufferTapes, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensTapes);
    // execute the statement and see whether we found something
 
    m_updateCheckedTapesStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateCheckedTapes "
      << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// fileToMigrate  
//----------------------------------------------------------------------------

castor::tape::tapegateway::FileToMigrateResponse* castor::tape::tapegateway::ora::OraTapeGatewaySvc::fileToMigrate(std::string vid) throw (castor::exception::Exception){
  castor::tape::tapegateway::FileToMigrateResponse*result=NULL;
  try {
    // Check whether the statements are ok
    if (0 == m_fileToMigrateStatement) {
      m_fileToMigrateStatement =
        createStatement(s_fileToMigrateStatementString);
      m_fileToMigrateStatement->registerOutParam
        (2, oracle::occi::OCCIDOUBLE);
      m_fileToMigrateStatement->registerOutParam
        (3, oracle::occi::OCCISTRING);
      m_fileToMigrateStatement->registerOutParam
        (4, oracle::occi::OCCIDOUBLE);
      m_fileToMigrateStatement->registerOutParam
        (5, oracle::occi::OCCISTRING);
      m_fileToMigrateStatement->registerOutParam
        (6, oracle::occi::OCCISTRING);
      m_fileToMigrateStatement->registerOutParam
        (7, oracle::occi::OCCISTRING);
      m_fileToMigrateStatement->registerOutParam
	(8, oracle::occi::OCCIDOUBLE);
    }

    m_fileToMigrateStatement->setString(1,vid);

    // execute the statement and see whether we found something 

    castor::tape::tapegateway::NsFileInformation* nsInfo=
      new castor::tape::tapegateway::NsFileInformation();
    nsInfo->setFileid((u_signed64)m_fileToMigrateStatement->getDouble(2));
    nsInfo->setNshost(m_fileToMigrateStatement->getString(3));
   
    castor::tape::tapegateway::FileDiskLocation* diskInfo= 
      new castor::tape::tapegateway::FileDiskLocation();
    diskInfo->setBytes((u_signed64)m_fileToMigrateStatement->getDouble(4));
    diskInfo->setDiskserverName(m_fileToMigrateStatement->getString(5));
    diskInfo->setMountPoint(m_fileToMigrateStatement->getString(6)); 
    diskInfo->setPath(m_fileToMigrateStatement->getString(7));
   m_fileToMigrateStatement->getDouble(8);
    result->setNsfileinformation(nsInfo);
    result->setFiledisklocation(diskInfo);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in fileToMigrate"
      << std::endl << e.what();
    throw ex;
  }
  return result;
}

//----------------------------------------------------------------------------
// fileMigrationUpdate  
//----------------------------------------------------------------------------
  
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::fileMigrationUpdate (castor::tape::tapegateway::FileMigratedResponse* resp)
  throw (castor::exception::Exception){


  try {
    // Check whether the statements are ok

    if (0 == m_fileMigrationUpdateStatement) {
      m_fileMigrationUpdateStatement =
        createStatement(s_fileMigrationUpdateStatementString);
    }

    m_fileMigrationUpdateStatement->setDouble(1,(double)resp->nsfileinformation()->fileid());
    m_fileMigrationUpdateStatement->setString(2,resp->nsfileinformation()->nshost());
    m_fileMigrationUpdateStatement->setInt(3,resp->nsfileinformation()->tapefilensattribute()->copyNo());
    m_fileMigrationUpdateStatement->setInt(4,resp->errorCode()); 
    
    // execute the statement 

    m_fileMigrationUpdateStatement->executeUpdate();
    


  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in fileMigrationUpdate"
      << std::endl << e.what();
    throw ex;
  }
  
}

//----------------------------------------------------------------------------
// fileToRecall
//----------------------------------------------------------------------------
   
castor::tape::tapegateway::FileToRecallResponse* castor::tape::tapegateway::ora::OraTapeGatewaySvc::fileToRecall
        (std::string vid)
          throw (castor::exception::Exception)
{
    
  castor::tape::tapegateway::FileToRecallResponse* result=NULL;
  try {
    // Check whether the statements are ok
    if (0 == m_fileToRecallStatement) {
      m_fileToRecallStatement =
        createStatement(s_fileToRecallStatementString);

      m_fileToRecallStatement->registerOutParam
        (2, oracle::occi::OCCIDOUBLE);
      m_fileToRecallStatement->registerOutParam
        (3, oracle::occi::OCCISTRING);
      m_fileToRecallStatement->registerOutParam
        (4, oracle::occi::OCCISTRING);
      m_fileToRecallStatement->registerOutParam
        (5, oracle::occi::OCCISTRING);
      m_fileToRecallStatement->registerOutParam
        (6, oracle::occi::OCCISTRING);
    }

    m_fileToRecallStatement->setString(1,vid);

    // execute the statement and see whether we found something
 
    m_fileToRecallStatement->executeUpdate();

    result= new  castor::tape::tapegateway::FileToRecallResponse();

    castor::tape::tapegateway::NsFileInformation* nsInfo=
      new castor::tape::tapegateway::NsFileInformation();
    nsInfo->setFileid((u_signed64)m_fileToMigrateStatement->getDouble(2));
    nsInfo->setNshost(m_fileToMigrateStatement->getString(3));
    castor::tape::tapegateway::FileDiskLocation* diskInfo= 
      new castor::tape::tapegateway::FileDiskLocation();
    diskInfo->setDiskserverName(m_fileToMigrateStatement->getString(4));
    diskInfo->setMountPoint(m_fileToMigrateStatement->getString(5)); 
    diskInfo->setPath(m_fileToMigrateStatement->getString(6));
    result->setNsfileinformation(nsInfo);
    result->setFiledisklocation(diskInfo);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in fileToRecall"
      << std::endl << e.what();
    throw ex;
  }
  return result;

}
 
//----------------------------------------------------------------------------
// fileRecallUpdate  
//----------------------------------------------------------------------------
   
void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::fileRecallUpdate(FileRecalledResponse* resp)
  throw (castor::exception::Exception){
 
 try {
    // Check whether the statements are ok

    if (0 == m_fileRecallUpdateStatement) {
      m_fileRecallUpdateStatement =
        createStatement(s_fileRecallUpdateStatementString);
 
    }

    m_fileRecallUpdateStatement->setDouble(1,(double)resp->nsfileinformation()->fileid());
    m_fileRecallUpdateStatement->setString(2,resp->nsfileinformation()->nshost());
    m_fileRecallUpdateStatement->setDouble(3,(double)resp->nsfileinformation()->fileSize());
    m_fileRecallUpdateStatement->setInt(4,resp->nsfileinformation()->tapefilensattribute()->copyNo());
    m_fileRecallUpdateStatement->setInt(5,resp->nsfileinformation()->tapefilensattribute()->fseq());
    m_fileRecallUpdateStatement->setString(6,resp->nsfileinformation()->tapefilensattribute()->vid());  
    m_fileRecallUpdateStatement->setInt(7,resp->errorCode());

    // execute the statement 

    m_fileToRecallStatement->executeUpdate();


  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in fileToRecall"
      << std::endl << e.what();
    throw ex;
  }

}
  
//----------------------------------------------------------------------------
// inputForMigrationRetryPolicy
//----------------------------------------------------------------------------

std::vector<castor::stager::TapeCopy*>  castor::tape::tapegateway::ora::OraTapeGatewaySvc::inputForMigrationRetryPolicy()
	  throw (castor::exception::Exception)
{
    
  std::vector<castor::stager::TapeCopy*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_inputForMigrationRetryPolicyStatement) {
      m_inputForMigrationRetryPolicyStatement =
        createStatement(s_inputForMigrationRetryPolicyStatementString);
      m_inputForMigrationRetryPolicyStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    m_inputForMigrationRetryPolicyStatement->executeUpdate();

   oracle::occi::ResultSet *rs =
      m_inputForMigrationRetryPolicyStatement->getCursor(1);

    // Run through the cursor 

    oracle::occi::ResultSet::Status status = rs->next();
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::stager::TapeCopy* item= new castor::stager::TapeCopy();
       item->setCopyNb(rs->getInt(1));
       item->setId((u_signed64)rs->getDouble(2));
       // rs->getDouble(3) not needed
       item->setStatus((castor::stager::TapeCopyStatusCodes)rs->getInt(4));
       item->setErrorCode(rs->getInt(5));
       item->setNbRetry(rs->getInt(6));
       result.push_back(item);
       status = rs->next();
    }

    m_inputForMigrationRetryPolicyStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in inputForMigrationRetryPolicy"
      << std::endl << e.what();
    throw ex;
  }
  return result;

}


//----------------------------------------------------------------------------
// updateWithMigrationRetryPolicy
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateWithMigrationRetryPolicyResult(std::vector<u_signed64> tcToRetry, std::vector<u_signed64> tcToFail ) throw (castor::exception::Exception) {

  try { 
    // Check whether the statements are ok
    if (0 == m_updateWithMigrationRetryPolicyResultStatement) {
      m_updateWithMigrationRetryPolicyResultStatement =
        createStatement(s_updateWithMigrationRetryPolicyResultStatementString);
    }

    // success

    ub4 nbRetry= tcToRetry.size();
    nbRetry=nbRetry==0?1:nbRetry;
    unsigned char (*bufferRetry)[21]=(unsigned char(*)[21]) calloc((nbRetry) * 21, sizeof(unsigned char));
    ub2 *lensRetry=(ub2 *)malloc (sizeof(ub2)*nbRetry);


    for (unsigned int i=0; i<tcToRetry.size();i++){
      
	oracle::occi::Number n = (double)(tcToRetry[i]);
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
    m_updateWithMigrationRetryPolicyResultStatement->setDataBufferArray(1,bufferRetry, oracle::occi::OCCI_SQLT_NUM, nbRetry, &unusedRetry, 21, lensRetry);

    // failures

    ub4 nbFail = tcToFail.size();
    nbFail = nbFail == 0 ? 1 : nbFail; 
    unsigned char (*bufferFail)[21]=(unsigned char(*)[21]) calloc((nbFail) * 21, sizeof(unsigned char));
    ub2 *lensFail = (ub2 *)malloc (sizeof(ub2)*nbFail);


    for (unsigned int i=0; i<tcToFail.size();i++){
      
	oracle::occi::Number n = (double)(tcToFail[i]);
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
    m_updateWithMigrationRetryPolicyResultStatement->setDataBufferArray(2,bufferFail, oracle::occi::OCCI_SQLT_NUM, nbFail, &unusedFail, 21, lensFail);

    m_updateWithMigrationRetryPolicyResultStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateWithMigrationRetryPolicy"
      << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// inputForRecallRetryPolicy
//----------------------------------------------------------------------------

std::vector<castor::stager::TapeCopy*>  castor::tape::tapegateway::ora::OraTapeGatewaySvc::inputForRecallRetryPolicy()
	  throw (castor::exception::Exception){
    
  std::vector<castor::stager::TapeCopy*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_inputForRecallRetryPolicyStatement) {
      m_inputForRecallRetryPolicyStatement =
        createStatement(s_inputForRecallRetryPolicyStatementString);
      m_inputForRecallRetryPolicyStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    m_inputForRecallRetryPolicyStatement->executeUpdate();

   oracle::occi::ResultSet *rs =
      m_inputForRecallRetryPolicyStatement->getCursor(1);

    // Run through the cursor 

    oracle::occi::ResultSet::Status status = rs->next();
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {   
      castor::stager::TapeCopy* item= new castor::stager::TapeCopy();
      item->setCopyNb(rs->getInt(1));
      item->setId((u_signed64)rs->getDouble(2));
      // rs->getDouble(3)) castorfile not needed 
      item->setStatus((castor::stager::TapeCopyStatusCodes)rs->getInt(4));
      item->setErrorCode(rs->getInt(5));
      item->setNbRetry(rs->getInt(6));
      result.push_back(item);
      status = rs->next();
    }

    m_inputForRecallRetryPolicyStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in inputForRecallRetryPolicy"
      << std::endl << e.what();
    throw ex;
  }
  return result;

}

//----------------------------------------------------------------------------
// updateWithRecallRetryPolicyResult
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateWithRecallRetryPolicyResult(std::vector<u_signed64> tcToRetry,std::vector<u_signed64> tcToFail) throw (castor::exception::Exception) {
 
  try {
     
    // Check whether the statements are ok
    if (0 == m_updateWithRecallRetryPolicyResultStatement) {
      m_updateWithRecallRetryPolicyResultStatement =
        createStatement(s_updateWithRecallRetryPolicyResultStatementString);
    }

    // success

    ub4 nbRetry= tcToRetry.size();
    nbRetry=nbRetry==0?1:nbRetry;
    unsigned char (*bufferRetry)[21]=(unsigned char(*)[21]) calloc((nbRetry) * 21, sizeof(unsigned char));
    ub2 *lensRetry=(ub2 *)malloc (sizeof(ub2)*nbRetry);


    for (unsigned int i=0; i<tcToRetry.size();i++){
      
	oracle::occi::Number n = (double)(tcToRetry[i]);
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
    m_updateWithRecallRetryPolicyResultStatement->setDataBufferArray(1,bufferRetry, oracle::occi::OCCI_SQLT_NUM, nbRetry, &unused, 21, lensRetry);

    // failure 
    

    ub4 nbFail= tcToFail.size();
    nbFail=nbFail==0?1:nbFail;
    unsigned char (*bufferFail)[21]=(unsigned char(*)[21]) calloc((nbFail) * 21, sizeof(unsigned char));
    ub2 *lensFail=(ub2 *)malloc (sizeof(ub2)*nbFail);

    for (unsigned int i=0; i<tcToFail.size();i++){
	oracle::occi::Number n = (double)(tcToFail[i]);
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
    m_updateWithRecallRetryPolicyResultStatement->setDataBufferArray(2,bufferFail, oracle::occi::OCCI_SQLT_NUM, nbFail, &unused, 21, lensFail);
    m_updateWithRecallRetryPolicyResultStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateWithRecallRetryPolicy"
      << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// getRepackVid 
//----------------------------------------------------------------------------
  
std::string  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getRepackVid(castor::tape::tapegateway::FileMigratedResponse* resp)
  throw (castor::exception::Exception){
  
  try {
    // Check whether the statements are ok

    if (0 == m_getRepackVidStatement) {
      m_getRepackVidStatement =
        createStatement(s_getRepackVidStatementString);
      m_getRepackVidStatement->registerOutParam
        (3, oracle::occi::OCCISTRING);
 
    }

    m_getRepackVidStatement->setDouble(1,(double)resp->nsfileinformation()->fileid());
    m_getRepackVidStatement->setString(2,resp->nsfileinformation()->nshost());
    
    
    // execute the statement 

    m_getRepackVidStatement->executeUpdate();
    
    std::string repackVid = m_getRepackVidStatement->getString(3);
    return repackVid;

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getRepackVid"
      << std::endl << e.what();
    throw ex;
  }
 return NULL; 
  
}

//--------------------------------------------------------------------------
// Update the database when the tape aggregator allows us to serve a request
//-------------------------------------------------------------------------- 
	 

castor::tape::tapegateway::TapeRequestState* castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateDbStartTape(castor::tape::tapegateway::StartWorkerRequest* startRequest, std::string  vwAddress) throw (castor::exception::Exception){ 
  
  castor::tape::tapegateway::TapeRequestState* result=NULL;  
  castor::stager::Tape* tape =NULL;
  castor::stager::Stream* stream=NULL;

  try {
    // Check whether the statements are ok
    if (0 == m_updateDbStartTapeStatement) {
      m_updateDbStartTapeStatement =
        createStatement(s_updateDbStartTapeStatementString);
      m_updateDbStartTapeStatement->registerOutParam
        (4, oracle::occi::OCCICURSOR);
    }

    m_updateDbStartTapeStatement->setInt(1,startRequest->vdqmVolReqId());
    m_updateDbStartTapeStatement->setInt(2,startRequest->mode());
    m_updateDbStartTapeStatement->setString(3,vwAddress);

    // execute the statement
 
    m_updateDbStartTapeStatement->executeUpdate();

   oracle::occi::ResultSet *rs =
      m_updateDbStartTapeStatement->getCursor(4);

    // Run through the cursor 

    oracle::occi::ResultSet::Status status = rs->next();
    // always just one request

    if (status == oracle::occi::ResultSet::DATA_AVAILABLE) {   
      result=new castor::tape::tapegateway::TapeRequestState();

      result->setAccessMode(rs->getInt(1));
      result->setId((u_signed64)rs->getDouble(2));
      result->setStartTime((u_signed64)rs->getDouble(3));
      result->setLastVdqmPingTime((u_signed64)rs->getDouble(4));
      result->setVdqmVolReqId(rs->getInt(5));
      result->setStatus((TapeRequestStateCode) rs->getInt(6));

      castor::stager::Tape* tape=new castor::stager::Tape();
      tape->setVid(rs->getString(7));
      
      if (result->accessMode() == 0){
	result->setTapeRecall(tape);
      } else {
	stream=new castor::stager::Stream();
	stream->setTape(tape);
	result->setStreamMigration(stream);
      }

    }
    
    m_updateDbStartTapeStatement->closeResultSet(rs);
    

  } catch (oracle::occi::SQLException e) {

    if (tape) delete tape;
    if (stream) delete stream;
    if (result) delete result;
    result=NULL;

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateDbStartTape"
      << std::endl << e.what();
    throw ex;
  }
  return result;


} 

//----------------------------------------------------------------------------
// updateDbEndTape
//----------------------------------------------------------------------------


void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateDbEndTape(castor::tape::tapegateway::TapeRequestState* tapeRequest) throw (castor::exception::Exception){


   try {
    // Check whether the statements are ok
    if (0 == m_updateDbEndTapeStatement) {
      m_updateDbEndTapeStatement =
	createStatement(s_updateDbEndTapeStatementString);
    }
    
    m_updateDbEndTapeStatement->setInt(1,tapeRequest->vdqmVolReqId());
    m_updateDbEndTapeStatement->setInt(2,tapeRequest->accessMode());

    // Execute the statement
    (void)m_updateDbEndTapeStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateDbEndTape."
      << std::endl << e.what();
    throw ex;
  }


} 


void castor::tape::tapegateway::ora::OraTapeGatewaySvc::invalidateSegment(castor::tape::tapegateway::FileToRecallResponse* file) throw (castor::exception::Exception){
 
 try {
    // Check whether the statements are ok

    if (0 == m_invalidateSegmentStatement) {
      m_invalidateSegmentStatement =
        createStatement(s_invalidateSegmentStatementString);
 
    }

    m_invalidateSegmentStatement->setDouble(1,(double)file->nsfileinformation()->fileid());
    m_invalidateSegmentStatement->setString(2,file->nsfileinformation()->nshost());
    m_invalidateSegmentStatement->setInt(3,file->nsfileinformation()->tapefilensattribute()->copyNo());

    // execute the statement 

    m_invalidateSegmentStatement->executeUpdate();


  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in invalidate segment"
      << std::endl << e.what();
    throw ex;
  }

}

//----------------------------------------------------------------------------
// tapeGatewayCleanUp
//----------------------------------------------------------------------------

std::vector<castor::stager::Tape*> castor::tape::tapegateway::ora::OraTapeGatewaySvc::tapeGatewayCleanUp() throw (castor::exception::Exception) {
  std::vector<castor::stager::Tape*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_tapeGatewayCleanUpStatement) {
      m_tapeGatewayCleanUpStatement =
	createStatement(s_tapeGatewayCleanUpStatementString);  
      m_tapeGatewayCleanUpStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // Execute the statement
    (void)m_tapeGatewayCleanUpStatement->executeUpdate();
    
    oracle::occi::ResultSet *rs =
      m_tapeGatewayCleanUpStatement->getCursor(1);

    // Run through the cursor 

    oracle::occi::ResultSet::Status status = rs->next();

    // surf the requests

    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {   
      castor::stager::Tape* item=new castor::stager::Tape(); 
      item->setVid(rs->getString(1));
      item->setSide(rs->getInt(2));
      item->setTpmode(rs->getInt(3));
      item->setErrMsgTxt(rs->getString(4));
      item->setErrorCode(rs->getInt(5)); 
      item->setSeverity(rs->getInt(6));
      item->setVwAddress(rs->getString(7));
      item->setId((u_signed64)rs->getDouble(8));
      // we don't need the stream id linked
      item->setStatus((castor::stager::TapeStatusCodes)rs->getInt(10));
      result.push_back(item);
      status = rs->next();
    }

    m_tapeGatewayCleanUpStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in tapeGatewayCleanUp."
      << std::endl << e.what();
    throw ex;
  }

  return result;

}
