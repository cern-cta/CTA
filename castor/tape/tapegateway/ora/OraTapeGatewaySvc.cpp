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
#include <vector>

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

#include "castor/stager/Stream.hpp"
#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/stager/TapeStatusCodes.hpp"

#include "castor/tape/tapegateway/NsTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/ora/OraTapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/PositionCommandCode.hpp"
#include "castor/tape/tapegateway/RmMasterTapeGatewayHelper.hpp"

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
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_resolveStreamsStatementString="BEGIN resolveStreams(:1,:2,:3);END;";

/// SQL statement for function getTapesToSubmit
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapesToSubmitStatementString="BEGIN getTapesToSubmit(:1);END;";

/// SQL statement for function updateSubmittedTapes
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateSubmittedTapesStatementString="BEGIN updateSubmittedTapes(:1,:2,:3,:4,:5);END;";

/// SQL statement for function getTapesToCheck
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapesToCheckStatementString="BEGIN getTapesToCheck(:1,:2);END;";

/// SQL statement for function updateCheckedTapes
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateCheckedTapesStatementString="BEGIN updateCheckedTapes(:1);END;";
  
/// SQL statement for function fileToMigrate
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_fileToMigrateStatementString="BEGIN fileToMigrate(:1,:2,:3,:4);END;";
  
/// SQL statement for function fileMigrationUpdate
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_fileMigrationUpdateStatementString="BEGIN fileMigrationUpdate(:1,:2,:3,:4,:5);END;";

/// SQL statement for function fileToRecall
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_fileToRecallStatementString="BEGIN fileToRecall(:1,:2,:3,:4);END;";

/// SQL statement for function segmentRecallUpdate
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_segmentRecallUpdateStatementString="BEGIN segmentRecallUpdate(:1,:2,:3,:4,:5,:6);END;";

/// SQL statement for function fileRecallUpdate
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_fileRecallUpdateStatementString="BEGIN fileRecallUpdate(:1,:2);END;";

/// SQL statement for function inputForMigrationRetryPolicy 
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_inputForMigrationRetryPolicyStatementString="BEGIN inputForMigrationRetryPolicy(:1);END;";
 
/// SQL statement for function updateWithMigrationRetryPolicyResult   
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateWithMigrationRetryPolicyResultStatementString="BEGIN updateMigRetryPolicy(:1,:2);END;";

/// SQL statement for function inputForRecallRetryPolicy
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_inputForRecallRetryPolicyStatementString="BEGIN inputForRecallRetryPolicy(:1);END;";

/// SQL statement for function updateWithRecallRetryPolicyResult   
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateWithRecallRetryPolicyResultStatementString="BEGIN updateRecRetryPolicy(:1,:2);END;";

/// SQL statement for function checkFileForRepackAndFileInformation   
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getRepackVidAndFileInformationStatementString="BEGIN getRepackVidAndFileInformation(:1,:2,:3,:4,:5,:6,:7,:8);END;";

/// SQL statement for function updateDbStartTape
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateDbStartTapeStatementString="BEGIN updateDbStartTape(:1,:2,:3,:4,:5,:6);END;";

/// SQL statement for function updateDbEndTape
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateDbEndTapeStatementString="BEGIN updateDbEndTape(:1,:2,:3);END;";

/// SQL statement for function getSegmentInformation
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getSegmentInformationStatementString="BEGIN getSegmentInformation(:1,:2,:3,:4,:5,:6,:7);END;";

// SQL statement  for function updateAfterFailure
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_updateAfterFailureStatementString="BEGIN updateAfterFailure(:1,:2,:3,:4,:5,:6,:7);END;";


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
  m_segmentRecallUpdateStatement(0),
  m_fileRecallUpdateStatement(0),
  m_inputForMigrationRetryPolicyStatement(0),
  m_updateWithMigrationRetryPolicyResultStatement(0),
  m_inputForRecallRetryPolicyStatement(0),
  m_updateWithRecallRetryPolicyResultStatement(0),
  m_getRepackVidAndFileInformationStatement(0),
  m_updateDbStartTapeStatement(0),
  m_updateDbEndTapeStatement(0),
  m_getSegmentInformationStatement(0),
  m_updateAfterFailureStatement(0)
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

    if ( m_getStreamsToResolveStatement ) deleteStatement(m_getStreamsToResolveStatement);
    if ( m_resolveStreamsStatement ) deleteStatement(m_resolveStreamsStatement);
    if ( m_getTapesToSubmitStatement ) deleteStatement( m_getTapesToSubmitStatement);
    if ( m_updateSubmittedTapesStatement ) deleteStatement( m_updateSubmittedTapesStatement);
    if ( m_getTapesToCheckStatement ) deleteStatement(m_getTapesToCheckStatement);
    if ( m_updateCheckedTapesStatement ) deleteStatement(m_updateCheckedTapesStatement);
    if ( m_fileToMigrateStatement ) deleteStatement(m_fileToMigrateStatement);
    if ( m_fileMigrationUpdateStatement ) deleteStatement( m_fileMigrationUpdateStatement);
    if ( m_fileToRecallStatement ) deleteStatement( m_fileToRecallStatement);
    if ( m_segmentRecallUpdateStatement ) deleteStatement( m_segmentRecallUpdateStatement);
    if ( m_fileRecallUpdateStatement ) deleteStatement( m_fileRecallUpdateStatement);
    if ( m_inputForMigrationRetryPolicyStatement ) deleteStatement(m_inputForMigrationRetryPolicyStatement);
    if ( m_updateWithMigrationRetryPolicyResultStatement ) deleteStatement(m_updateWithMigrationRetryPolicyResultStatement);
    if ( m_inputForRecallRetryPolicyStatement ) deleteStatement(m_inputForRecallRetryPolicyStatement);
    if ( m_updateWithRecallRetryPolicyResultStatement ) deleteStatement(m_updateWithRecallRetryPolicyResultStatement);
    if ( m_getRepackVidAndFileInformationStatement ) deleteStatement(m_getRepackVidAndFileInformationStatement);
    if ( m_updateDbStartTapeStatement ) deleteStatement(m_updateDbStartTapeStatement);
    if ( m_updateDbEndTapeStatement ) deleteStatement(m_updateDbEndTapeStatement);
    if ( m_getSegmentInformationStatement )  deleteStatement(m_getSegmentInformationStatement);
    if ( m_updateAfterFailureStatement ) deleteStatement(m_updateAfterFailureStatement);
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
  m_segmentRecallUpdateStatement = 0;
  m_fileRecallUpdateStatement = 0;
  m_inputForMigrationRetryPolicyStatement = 0;
  m_updateWithMigrationRetryPolicyResultStatement = 0;
  m_inputForRecallRetryPolicyStatement = 0;
  m_updateWithRecallRetryPolicyResultStatement = 0;
  m_getRepackVidAndFileInformationStatement = 0;
  m_updateDbStartTapeStatement = 0;
  m_updateDbEndTapeStatement = 0; 
  m_getSegmentInformationStatement = 0;
  m_updateAfterFailureStatement = 0;
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
      cnvSvc()->commit(); 
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

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::resolveStreams(std::vector<u_signed64> strIds, std::vector<std::string> vids, std::vector<int> fseqs)
          throw (castor::exception::Exception){

  try {

    if ( !strIds.size() || !vids.size() || !fseqs.size() ) {
      // just release the lock no result
      cnvSvc()->commit();
      return;
    }
     
    // Check whether the statements are ok
    if (0 == m_resolveStreamsStatement) {
      m_resolveStreamsStatement =
        createStatement(s_resolveStreamsStatementString);
    }

    // input

     ub4 nb=strIds.size();

     // fseq
     unsigned char (*bufferFseqs)[21]=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
     ub2 *lensFseqs=(ub2 *)malloc (sizeof(ub2)*nb);

     if ( lensFseqs  == 0 || bufferFseqs == 0 ) {
       if (lensFseqs != 0 ) free(lensFseqs);
       if (bufferFseqs != 0 ) free(bufferFseqs);
       castor::exception::OutOfMemory e; 
       throw e;
     }
     
     // strId

     unsigned char (*bufferStrIds)[21]=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
     ub2 *lensStrIds=(ub2 *)malloc (sizeof(ub2)*nb);
     if ( lensStrIds  == 0 || bufferStrIds == 0 ) {
       if (lensStrIds != 0 ) free(lensStrIds);
       if (bufferStrIds != 0) free(bufferStrIds);
       if (lensFseqs != 0 ) free(lensFseqs);
       if (bufferFseqs != 0 ) free(bufferFseqs);
       castor::exception::OutOfMemory e; 
       throw e;
     }

     // vids

     char * bufferVids=NULL;
     ub2 *lensVids=NULL;
     

     // get the maximum cell size
     unsigned int maxLen=0;
     unsigned int numTapes=0;
     for (unsigned int i=0;i<vids.size();i++){
	maxLen=maxLen > vids.at(i).length()?maxLen:vids.at(i).length();
	numTapes++;
     }

     if (maxLen == 0) {
       if (lensStrIds != 0 ) free(lensStrIds);
       if (bufferStrIds != 0) free(bufferStrIds);
       if (lensFseqs != 0 ) free(lensFseqs);
       if (bufferFseqs != 0 ) free(bufferFseqs);
       castor::exception::Internal ex;
       ex.getMessage() << "invalid VID in resolveStreams"
		       << std::endl;
       throw ex;

     }
     

     unsigned int bufferCellSize = maxLen * sizeof(char);
     lensVids = (ub2*) malloc(maxLen * sizeof(ub2));
     bufferVids =
	(char*) malloc(numTapes * bufferCellSize);

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

     for (unsigned int i=0;i<vids.size();i++){
       // fseq

       oracle::occi::Number n = (double)(fseqs[i]);
       oracle::occi::Bytes b = n.toBytes();
       b.getBytes(bufferFseqs[i],b.length());
       lensFseqs[i] = b.length();

       // stream Ids
       
       n = (double)(strIds[i]);
       b = n.toBytes();
       b.getBytes(bufferStrIds[i],b.length());
       lensStrIds[i] = b.length();
        
       // vids
       
       lensVids[i]= vids[i].length();
       strncpy(bufferVids+(bufferCellSize*i),vids[i].c_str(),lensVids[i]);
	
     }

     ub4 unused=nb;
     m_resolveStreamsStatement->setDataBufferArray(1,bufferFseqs, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensFseqs);
     m_resolveStreamsStatement->setDataBufferArray(2,bufferStrIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensStrIds);
     ub4 len=nb;
     m_resolveStreamsStatement->setDataBufferArray(3, bufferVids, oracle::occi::OCCI_SQLT_CHR,len, &len, maxLen, lensVids);

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
      cnvSvc()->commit();
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
      cnvSvc()->commit();
      return;
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

     if ( lensIds == 0 || bufferIds == 0 || lensVdqmIds ==0 || bufferVdqmIds == 0 ) {
       if (lensIds != 0 ) free(lensIds);
       if (bufferIds != 0) free(bufferIds);
       if (lensVdqmIds != 0 ) free(lensVdqmIds);
       if (bufferVdqmIds != 0) free(bufferVdqmIds);
       castor::exception::OutOfMemory e; 
       throw e;
     }

     // dgn

     char * bufferDgns=NULL;
     ub2 *lensDgns=NULL;

     unsigned int cellSizeDgn = (CA_MAXDGNLEN+1) * sizeof(char);
     lensDgns = (ub2*) malloc((CA_MAXDGNLEN+1) * sizeof(ub2));
     bufferDgns = (char*) malloc(nb * cellSizeDgn);

     if ( lensDgns  == 0 || bufferDgns == 0 ) {
       if (lensIds != 0 ) free(lensIds);
       if (bufferIds != 0) free(bufferIds);
       if (lensVdqmIds != 0 ) free(lensVdqmIds);
       if (bufferVdqmIds != 0) free(bufferVdqmIds);
       castor::exception::OutOfMemory e; 
       throw e;
     }

     // density

     char * bufferDensities=NULL;
     ub2 *lensDensities=NULL;

     unsigned int cellSizeDen = (CA_MAXDENLEN +1)* sizeof(char);
     lensDensities = (ub2*) malloc((CA_MAXDENLEN+1) * sizeof(ub2));
     bufferDensities = (char*) malloc(nb * cellSizeDen);

     if (lensDensities==0 || bufferDensities==0  ){
       if ( lensDgns  != 0 ) free(lensDgns);
       if ( bufferDgns != 0 ) free(bufferDgns);
       if ( lensIds != 0 ) free(lensIds);
       if ( bufferIds != 0) free(bufferIds);
       if ( lensVdqmIds != 0 ) free(lensVdqmIds);
       if ( bufferVdqmIds != 0) free(bufferVdqmIds);
       castor::exception::OutOfMemory e; 
       throw e;
     }

     // label

     char * bufferLabels=NULL;
     ub2 *lensLabels=NULL;

     unsigned int cellSizeLabel = (CA_MAXLBLTYPLEN+1) * sizeof(char);
     lensLabels = (ub2*) malloc((CA_MAXLBLTYPLEN+1) * sizeof(ub2));
     bufferLabels = (char*) malloc(nb * cellSizeLabel);

     if (lensLabels==0 || bufferLabels==0){
       if (lensDensities!=0 ) free (lensDensities);
       if (bufferDensities!=0) free (bufferDensities);
       if ( lensDgns  != 0 ) free(lensDgns);
       if ( bufferDgns != 0 ) free(bufferDgns);
       if ( lensIds != 0 ) free(lensIds);
       if ( bufferIds != 0) free(bufferIds);
       if ( lensVdqmIds != 0 ) free(lensVdqmIds);
       if ( bufferVdqmIds != 0) free(bufferVdqmIds);
       castor::exception::OutOfMemory e; 
       throw e;
     }


     
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

       castor::stager::Tape* inputTape = NULL;
       if (tapeRequests[i]->accessMode()) inputTape=tapeRequests[i]->streamMigration()->tape();
       else inputTape=tapeRequests[i]->tapeRecall();

       //dgn     
       
       lensDgns[i]= inputTape->dgn().length();
       strncpy(bufferDgns+(cellSizeDgn*i),inputTape->dgn().c_str(),lensDgns[i]);

       //label

       lensLabels[i]= inputTape->label().length();
       strncpy(bufferLabels+(cellSizeLabel*i),inputTape->label().c_str(),lensLabels[i]);

       //density

       lensDensities[i]= inputTape->density().length();
       strncpy(bufferDensities+(cellSizeDen*i),inputTape->density().c_str(),lensDensities[i]);

     }

     ub4 unused=nb;

     m_updateSubmittedTapesStatement->setDataBufferArray(1,bufferIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensIds);
     m_updateSubmittedTapesStatement->setDataBufferArray(2, bufferVdqmIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensVdqmIds);
     m_updateSubmittedTapesStatement->setDataBufferArray(3, bufferDgns, oracle::occi::OCCI_SQLT_CHR,nb, &nb, (CA_MAXDGNLEN+1), lensDgns);
     m_updateSubmittedTapesStatement->setDataBufferArray(4, bufferLabels, oracle::occi::OCCI_SQLT_CHR,nb, &nb, (CA_MAXLBLTYPLEN+1), lensLabels);
     m_updateSubmittedTapesStatement->setDataBufferArray(5, bufferDensities, oracle::occi::OCCI_SQLT_CHR,nb, &nb, (CA_MAXDENLEN+1), lensDensities);
 
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
    if (0 == m_getTapesToCheckStatement) {
      m_getTapesToCheckStatement =
        createStatement(s_getTapesToCheckStatementString);
      m_getTapesToCheckStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR);
    }

    m_getTapesToCheckStatement->setDouble(1,(double)timeOut);

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getTapesToCheckStatement->executeUpdate();

    if (0 == nb) {
      cnvSvc()->commit();
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
      cnvSvc()->commit();
      return;
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

    
    if ( lensTapes == 0 || bufferTapes == 0 ) {
      if (lensTapes != 0 ) free(lensTapes);
      if (bufferTapes != 0) free(bufferTapes);
      castor::exception::OutOfMemory e; 
      throw e;
     }


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

castor::tape::tapegateway::FileToMigrate* castor::tape::tapegateway::ora::OraTapeGatewaySvc::fileToMigrate(castor::tape::tapegateway::FileToMigrateRequest& req) throw (castor::exception::Exception){
  castor::tape::tapegateway::FileToMigrate* result=NULL;
  std::string diskserver;
  std::string mountpoint;
  try {

    while (1) { // until we get a valid file
    
      // Check whether the statements are ok
      if (0 == m_fileToMigrateStatement) {
	m_fileToMigrateStatement =
	  createStatement(s_fileToMigrateStatementString);
	m_fileToMigrateStatement->registerOutParam
	  (2, oracle::occi::OCCIINT);
	m_fileToMigrateStatement->registerOutParam
	  (3, oracle::occi::OCCISTRING, 2048 );
	m_fileToMigrateStatement->registerOutParam
	  (4, oracle::occi::OCCICURSOR);
      }

      m_fileToMigrateStatement->setDouble(1,(double)req.transactionId());
      m_fileToMigrateStatement->executeUpdate();

      int ret = m_fileToMigrateStatement->getInt(2);
      if (ret == -1 ) return NULL;

      if (ret == -2 ) {// UNKNOWN request
	castor::exception::Exception e(EINVAL);
	throw e;
      }

      std::string vid=m_fileToMigrateStatement->getString(3); 

      oracle::occi::ResultSet *rs =
	m_fileToMigrateStatement->getCursor(4);

      // Run through the cursor 
      
      oracle::occi::ResultSet::Status status = rs->next();
      
      // one at the moment

      if  (status == oracle::occi::ResultSet::DATA_AVAILABLE) {

	result= new  castor::tape::tapegateway::FileToMigrate();
      
	result->setFileid((u_signed64)rs->getDouble(1));
	result->setNshost(rs->getString(2));
	result->setLastModificationTime((u_signed64)rs->getDouble(3));
	diskserver=rs->getString(4);
	mountpoint=rs->getString(5);
	result->setPath(diskserver.append(":").append(mountpoint).append(rs->getString(6))); 
	result->setLastKnownFileName(rs->getString(7));
	result->setFseq(rs->getInt(8));
	result->setFileSize((u_signed64)rs->getDouble(9));

	result->setTransactionId(req.transactionId());
	result->setPositionCommandCode(TPPOSIT_FSEQ);				   

	try {
	  NsTapeGatewayHelper nsHelper;
	  nsHelper.checkFileToMigrate(*result,vid);
	} catch (castor::exception::Exception e) {

	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
    
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 86, 2, params);
	  

	  try {

	    
	    FileErrorReport failure;
	    failure.setTransactionId(result->transactionId()); 
	    failure.setFileid(result->fileid());
	    failure.setNshost(result->nshost());
	    failure.setFseq(result->fseq());
	    failure.setErrorCode(e.code());
	    updateAfterFailure(failure);

	  } catch (castor::exception::Exception ex){

	    // just log the error
	    
	    castor::dlf::Param params[] =
	      {castor::dlf::Param("errorCode",sstrerror(ex.code())),
	       castor::dlf::Param("errorMessage",ex.getMessage().str())
	      };
    
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 87, 2, params);
       
	    
	  }  

	  if (result) delete result;
	  result=NULL;
	  m_fileToMigrateStatement->closeResultSet(rs);
	  continue; // get another tapecopy

	}

	// we have a valid candidate we send a report to RmMaster stream started
	try{
      
	  RmMasterTapeGatewayHelper rmMasterHelper;
	  rmMasterHelper.sendStreamReport(diskserver,mountpoint,castor::monitoring::STREAMDIRECTION_WRITE,true);
	  
	} catch (castor::exception::Exception e){
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 73, 0, NULL);
	}

      }
      	
      m_fileToMigrateStatement->closeResultSet(rs);
      break; // end the loop

    }      
    // here we have a valid candidate or NULL
   
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
  
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::fileMigrationUpdate(castor::tape::tapegateway::FileMigratedNotification& resp)
  throw (castor::exception::Exception){
  std::string diskserver;
  std::string mountpoint;


  try {
    // Check whether the statements are ok

    if (0 == m_fileMigrationUpdateStatement) {
      m_fileMigrationUpdateStatement =
        createStatement(s_fileMigrationUpdateStatementString);
      m_fileMigrationUpdateStatement->registerOutParam
        (5, oracle::occi::OCCICURSOR);
    }

    m_fileMigrationUpdateStatement->setDouble(1,(double)resp.transactionId()); // transaction id
    m_fileMigrationUpdateStatement->setDouble(2,(double)resp.fileid());
    m_fileMigrationUpdateStatement->setString(3,resp.nshost());
    m_fileMigrationUpdateStatement->setInt(4,resp.fseq()); 
    
    m_fileMigrationUpdateStatement->executeUpdate();

    oracle::occi::ResultSet *rs =
      m_fileMigrationUpdateStatement->getCursor(5);

    // Run through the cursor 

    oracle::occi::ResultSet::Status status = rs->next();
    // just one

    if (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      diskserver = rs->getString(1);
      mountpoint = rs->getString(2);
    
      m_fileMigrationUpdateStatement->closeResultSet(rs);

    
      // send report to RmMaster stream ended
  
      try{

	RmMasterTapeGatewayHelper rmMasterHelper;
	rmMasterHelper.sendStreamReport(diskserver, mountpoint, castor::monitoring::STREAMDIRECTION_WRITE,false);
  
      } catch (castor::exception::Exception e){
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 73, 0, NULL);
      }
    }

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
   
castor::tape::tapegateway::FileToRecall* castor::tape::tapegateway::ora::OraTapeGatewaySvc::fileToRecall(castor::tape::tapegateway::FileToRecallRequest& req) throw (castor::exception::Exception)
{
    
  castor::tape::tapegateway::FileToRecall* result=NULL;
  std::string diskserver;
  std::string mountpoint;

  try {

    while (1) {
     
    
      // Check whether the statements are ok
      if (0 == m_fileToRecallStatement) {
	m_fileToRecallStatement =
	  createStatement(s_fileToRecallStatementString);
	m_fileToRecallStatement->registerOutParam
	  (2, oracle::occi::OCCIINT);
	m_fileToRecallStatement->registerOutParam
	  (3, oracle::occi::OCCISTRING,  2048);
	m_fileToRecallStatement->registerOutParam
	  (4, oracle::occi::OCCICURSOR);
      }
   
      m_fileToRecallStatement->setDouble(1,(double)req.transactionId());
      
      // execute the statement and see whether we found something
 
      m_fileToRecallStatement->executeUpdate();

      
      int ret = m_fileToRecallStatement->getInt(2);
      if (ret == -1 ) return NULL;
     
      if (ret == -2 ) {// UNKNOWN request
	castor::exception::Exception e(EINVAL);
	throw e;
      }
    

      std::string vid= m_fileToRecallStatement->getString(3);
   
      oracle::occi::ResultSet *rs =
	m_fileToRecallStatement->getCursor(4);

      // Run through the cursor 

      oracle::occi::ResultSet::Status status = rs->next();

      // one at the moment
    

      if  (status == oracle::occi::ResultSet::DATA_AVAILABLE) {

	result= new  castor::tape::tapegateway::FileToRecall();
	result->setFileid((u_signed64)rs->getDouble(1));
	result->setNshost(rs->getString(2));
	diskserver=rs->getString(3);
	mountpoint=rs->getString(4);

	result->setPath(diskserver.append(":").append(mountpoint).append(rs->getString(5)));

	result->setFseq(rs->getInt(6));
	result->setTransactionId(req.transactionId());
	result->setPositionCommandCode(TPPOSIT_BLKID);

	// it might be changed after the ns check to TPPOSIT_FSEQ

	// let's check if it is valid in the nameserver and let's retrieve the blockid

	try {
	  
	  NsTapeGatewayHelper nsHelper;
	  nsHelper.getBlockIdToRecall(*result,vid);
	  break; // found a valid file

	}catch (castor::exception::Exception e) {

	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str())
	    };
	
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 88, 2, params);

	  try {
	    FileErrorReport failure;
	    failure.setTransactionId(result->transactionId()); 
	    failure.setFileid(result->fileid());
	    failure.setNshost(result->nshost());
	    failure.setFseq(result->fseq());
	    failure.setErrorCode(e.code());
	    updateAfterFailure(failure);
	    
	  } catch (castor::exception::Exception ex){

	    castor::dlf::Param params[] =
	      {castor::dlf::Param("errorCode",sstrerror(ex.code())),
	       castor::dlf::Param("errorMessage",ex.getMessage().str())
	      };
    
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 89, 2, params);
	  }

	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 73, 0, NULL);
	  if (result) delete result;
	  m_fileToRecallStatement->closeResultSet(rs);

	  continue; // Let's get another candidate, this one was not valid

	}
   
	// here we have a valid candidate and we send a report to RmMaster stream started    
	  
	try{

	  RmMasterTapeGatewayHelper rmMasterHelper;
	  rmMasterHelper.sendStreamReport(diskserver,mountpoint,castor::monitoring::STREAMDIRECTION_READ,true);
	
	} catch (castor::exception::Exception e){
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 73, 0, NULL);
	}
	
      }
      // end the loop
      m_fileToRecallStatement->closeResultSet(rs);
      break;
    }

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
   
bool  castor::tape::tapegateway::ora::OraTapeGatewaySvc::segmentRecallUpdate(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception){
 std::string diskserver;
 std::string mountpoint;

  try {
    // Check whether the statements are ok

    if (0 == m_segmentRecallUpdateStatement) {
      m_segmentRecallUpdateStatement =
        createStatement(s_segmentRecallUpdateStatementString);
      m_segmentRecallUpdateStatement->registerOutParam
	(5, oracle::occi::OCCICURSOR); 
      m_segmentRecallUpdateStatement->registerOutParam
	(6, oracle::occi::OCCIINT);
    }

    m_segmentRecallUpdateStatement->setDouble(1,(double)resp.transactionId());
    m_segmentRecallUpdateStatement->setDouble(2,(double)resp.fileid());
    m_segmentRecallUpdateStatement->setString(3,resp.nshost());
    m_segmentRecallUpdateStatement->setInt(4,resp.fseq());

    m_segmentRecallUpdateStatement->executeUpdate();
    
    
    
    oracle::occi::ResultSet *rs =
      m_segmentRecallUpdateStatement->getCursor(5);

    // Run through the cursor 

    oracle::occi::ResultSet::Status status = rs->next();
    // just one

    if (status == oracle::occi::ResultSet::DATA_AVAILABLE) {

      diskserver = rs->getString(1);
      mountpoint = rs->getString(2);

    // send report to RmMaster stream ended
      
      try {
      
	RmMasterTapeGatewayHelper  rmMasterHelper;
	rmMasterHelper.sendStreamReport(diskserver,mountpoint,castor::monitoring::STREAMDIRECTION_READ,false);
	
      } catch (castor::exception::Exception e){
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 73, 0, NULL);
      }
    }

    bool ret=m_segmentRecallUpdateStatement->getInt(6);
    
    m_segmentRecallUpdateStatement->closeResultSet(rs);
    return ret;

  } catch (oracle::occi::SQLException e) {

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in segmentRecallUpdate"
      << std::endl << e.what();
    throw ex;
  }

}
  


//----------------------------------------------------------------------------
// fileRecallUpdate  
//----------------------------------------------------------------------------
   
void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::fileRecallUpdate(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception){
 std::string diskserver;
 std::string mountpoint;

  try {
    // Check whether the statements are ok

    if (0 == m_fileRecallUpdateStatement) {
      m_fileRecallUpdateStatement =
        createStatement(s_fileRecallUpdateStatementString);
    }
    m_fileRecallUpdateStatement->setDouble(1,(double)resp.fileid());
    m_fileRecallUpdateStatement->setString(2,resp.nshost());
    m_fileRecallUpdateStatement->executeUpdate();
    

  } catch (oracle::occi::SQLException e) {

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in fileRecallUpdate"
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

    if ( lensRetry == 0 || bufferRetry == 0 ) {
      if (lensRetry != 0 ) free(lensRetry);
      if (bufferRetry != 0) free(bufferRetry);
      castor::exception::OutOfMemory e; 
      throw e;
    }

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

    if ( lensFail == 0 || bufferFail == 0 ) {
      if (lensFail != 0 ) free(lensFail);
      if (bufferFail != 0) free(bufferFail);
      if (lensRetry != 0 ) free(lensRetry);
      if (bufferRetry != 0) free(bufferRetry);
      castor::exception::OutOfMemory e; 
      throw e;
    }

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
   
    if ( lensRetry == 0 || bufferRetry == 0 ) {
      if (lensRetry != 0 ) free(lensRetry);
      if (bufferRetry != 0) free(bufferRetry);
      castor::exception::OutOfMemory e; 
      throw e;
    }

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

    
    if ( lensFail == 0 || bufferFail == 0 ) {
      if (lensFail != 0 ) free(lensFail);
      if (bufferFail != 0) free(bufferFail);
      if (lensRetry != 0 ) free(lensRetry);
      if (bufferRetry != 0) free(bufferRetry);
      castor::exception::OutOfMemory e; 
      throw e;
    }

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
// getRepackVidAndFileInformation 
//----------------------------------------------------------------------------
  
std::string  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getRepackVidAndFileInformation(castor::tape::tapegateway::FileMigratedNotification& resp, std::string& vid, int& copyNumber, u_signed64& lastModificationTime )
  throw (castor::exception::Exception){
  
  try {
    // Check whether the statements are ok

    if (0 == m_getRepackVidAndFileInformationStatement) {
      m_getRepackVidAndFileInformationStatement =
        createStatement(s_getRepackVidAndFileInformationStatementString);
      m_getRepackVidAndFileInformationStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048 );
      m_getRepackVidAndFileInformationStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048 );
      m_getRepackVidAndFileInformationStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_getRepackVidAndFileInformationStatement->registerOutParam
	(8, oracle::occi::OCCIDOUBLE);
    }

    m_getRepackVidAndFileInformationStatement->setDouble(1,(double)resp.fileid());
    m_getRepackVidAndFileInformationStatement->setString(2,resp.nshost());
    
    m_getRepackVidAndFileInformationStatement->setInt(3,resp.fseq());

    m_getRepackVidAndFileInformationStatement->setDouble(4,resp.transactionId());
    // execute the statement 

    m_getRepackVidAndFileInformationStatement->executeUpdate();
    
    std::string repackVid = m_getRepackVidAndFileInformationStatement->getString(5);
    vid = m_getRepackVidAndFileInformationStatement->getString(6);
    copyNumber = m_getRepackVidAndFileInformationStatement->getInt(7);
    lastModificationTime= (u_signed64)m_getRepackVidAndFileInformationStatement->getDouble(8);

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
	 

castor::tape::tapegateway::Volume* castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateDbStartTape(castor::tape::tapegateway::VolumeRequest& startRequest) throw (castor::exception::Exception){ 
  
  castor::tape::tapegateway::Volume* result=NULL;  

  try {
    // Check whether the statements are ok
    if (0 == m_updateDbStartTapeStatement) {
      m_updateDbStartTapeStatement =
        createStatement(s_updateDbStartTapeStatementString);
      m_updateDbStartTapeStatement->registerOutParam
        (2, oracle::occi::OCCISTRING, 2048 );
      m_updateDbStartTapeStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_updateDbStartTapeStatement->registerOutParam
        (4, oracle::occi::OCCIINT);
      m_updateDbStartTapeStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048 );
      m_updateDbStartTapeStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048 );
      
    }

    m_updateDbStartTapeStatement->setDouble(1,(double)startRequest.vdqmVolReqId());


    // execute the statement
 
    m_updateDbStartTapeStatement->executeUpdate();


    int ret = m_updateDbStartTapeStatement->getInt(4);

    if (ret==-1) {
      //NOMOREFILES
      return NULL;
    }
    if (ret == -2) {
      // UNKNOWN request
      castor::exception::Exception e(EINVAL);
      throw e;
    }
   
    result = new Volume();
    result->setVid(m_updateDbStartTapeStatement->getString(2));
    result->setMode(m_updateDbStartTapeStatement->getInt(3));
    result->setDensity(m_updateDbStartTapeStatement->getString(5));
    result->setLabel(m_updateDbStartTapeStatement->getString(6));
    result->setTransactionId(startRequest.vdqmVolReqId());
    

  } catch (oracle::occi::SQLException e) {
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


castor::stager::Tape*  castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateDbEndTape(castor::tape::tapegateway::EndNotification& endRequest) throw (castor::exception::Exception){
  
  castor::stager::Tape* result=NULL;

   try {
    // Check whether the statements are ok
    if (0 == m_updateDbEndTapeStatement) {
      m_updateDbEndTapeStatement =
	createStatement(s_updateDbEndTapeStatementString);
      m_updateDbEndTapeStatement->registerOutParam
	(2, oracle::occi::OCCISTRING, 2048 );
      m_updateDbEndTapeStatement->registerOutParam
	(3, oracle::occi::OCCIINT);
    }
    
    m_updateDbEndTapeStatement->setInt(1,endRequest.transactionId());
 

    // Execute the statement
    (void)m_updateDbEndTapeStatement->executeUpdate();

    result= new castor::stager::Tape();

    result->setVid( m_updateDbEndTapeStatement->getString(2));
    result->setTpmode( m_updateDbEndTapeStatement->getInt(3));

  } catch (oracle::occi::SQLException e) {
    
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateDbEndTape."
      << std::endl << e.what();
    throw ex;
  }
  return result;

} 


//----------------------------------------------------------------------------
// getSegmentInformation
//----------------------------------------------------------------------------
		
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getSegmentInformation(FileRecalledNotification &fileRecalled, std::string& vid,int& copyNb ) throw (castor::exception::Exception){
  
 try {
    // Check whether the statements are ok

    if (0 == m_getSegmentInformationStatement) {
      m_getSegmentInformationStatement =
        createStatement(s_getSegmentInformationStatementString);
      m_getSegmentInformationStatement->registerOutParam
	(6, oracle::occi::OCCISTRING, 2048 ); 
      m_getSegmentInformationStatement->registerOutParam
	(7, oracle::occi::OCCIINT); 
    }

    m_getSegmentInformationStatement->setDouble(1,(double)fileRecalled.transactionId()); 
    m_getSegmentInformationStatement->setDouble(2,(double)fileRecalled.fileid());
    m_getSegmentInformationStatement->setString(3,fileRecalled.nshost());
    m_getSegmentInformationStatement->setInt(4,fileRecalled.fseq());
    m_getSegmentInformationStatement->setString(5,fileRecalled.path());
    
    m_getSegmentInformationStatement->executeUpdate();

    vid= m_getSegmentInformationStatement->getString(6);
    copyNb=m_getSegmentInformationStatement->getInt(7);

  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getSegmentInformation"
      << std::endl << e.what();
    throw ex;
  }


}

	
//----------------------------------------------------------------------------
// updateAfterFailure
//----------------------------------------------------------------------------

castor::stager::Tape castor::tape::tapegateway::ora::OraTapeGatewaySvc::updateAfterFailure(FileErrorReport& failure) throw (castor::exception::Exception){
  castor::stager::Tape result;
  try {
    // Check whether the statements are ok

    if (0 == m_updateAfterFailureStatement) {
      m_updateAfterFailureStatement =
        createStatement(s_updateAfterFailureStatementString);
      m_updateAfterFailureStatement->registerOutParam
	      (6, oracle::occi::OCCISTRING, 2048 ); 
      m_updateAfterFailureStatement->registerOutParam
	      (7, oracle::occi::OCCIINT); 
    }

    m_updateAfterFailureStatement->setDouble(1,(double)failure.transactionId()); 
    m_updateAfterFailureStatement->setDouble(2,(double)failure.fileid());
    m_updateAfterFailureStatement->setString(3,failure.nshost());
    m_updateAfterFailureStatement->setInt(4,failure.fseq());
    m_updateAfterFailureStatement->setInt(5,failure.errorCode());
    
    m_updateAfterFailureStatement->executeUpdate();

    result.setVid(m_updateAfterFailureStatement->getString(6));
    result.setTpmode(m_updateAfterFailureStatement->getInt(7));

  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in updateAfterFailure"
      << std::endl << e.what();
    throw ex;
  }
  return result;
}
