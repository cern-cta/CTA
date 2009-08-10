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

#include "castor/stager/StreamStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/stager/TapeStatusCodes.hpp"

#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/DlfCodes.hpp"
#include "castor/tape/tapegateway/NsTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/ora/OraTapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/PositionCommandCode.hpp"
#include "castor/tape/tapegateway/RmMasterTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"


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


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapesWithoutDriveReqsStatementString="BEGIN tg_getTapesWithoutDriveReqs(:1);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_attachDriveReqsToTapesStatementString="BEGIN tg_attachDriveReqsToTapes(:1,:2,:3,:4,:5);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapesWithDriveReqsStatementString="BEGIN tg_getTapesWithDriveReqs(:1,:2);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_restartLostReqsStatementString="BEGIN tg_restartLostReqs(:1);END;";
  

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getFileToMigrateStatementString="BEGIN tg_getFileToMigrate(:1,:2,:3,:4);END;";
  

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_setFileMigratedStatementString="BEGIN tg_setFileMigrated(:1,:2,:3,:4,:5);END;";


const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getFileToRecallStatementString="BEGIN tg_getFileToRecall(:1,:2,:3,:4);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_setFileRecalledStatementString="BEGIN tg_setFileRecalled(:1,:2,:3,:4,:5);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getFailedMigrationsStatementString="BEGIN tg_getFailedMigrations(:1);END;";
 
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_setMigRetryResultStatementString="BEGIN tg_setMigRetryResult(:1,:2);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getFailedRecallsStatementString="BEGIN tg_getFailedRecalls(:1);END;";
   
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_setRecRetryResultStatementString="BEGIN tg_setRecRetryResult(:1,:2);END;";
  
const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getRepackVidAndFileInfoStatementString="BEGIN tg_getRepackVidAndFileInfo(:1,:2,:3,:4,:5,:6,:7,:8);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_startTapeSessionStatementString="BEGIN tg_startTapeSession(:1,:2,:3,:4,:5,:6);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_endTapeSessionStatementString="BEGIN tg_endTapeSession(:1,:2);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getSegmentInfoStatementString="BEGIN tg_getSegmentInfo(:1,:2,:3,:4,:5,:6);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_failFileTransferStatementString="BEGIN tg_failFileTransfer(:1,:2,:3,:4,:5);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_invalidateFileStatementString="BEGIN tg_invalidateFile(:1,:2,:3,:4,:5);END;";

const std::string castor::tape::tapegateway::ora::OraTapeGatewaySvc::s_getTapeToReleaseStatementString="BEGIN tg_getTapeToRelease(:1,:2,:3);END;";

//------------------------------------------------------------------------------
// OraTapeGatewaySvc
//------------------------------------------------------------------------------
castor::tape::tapegateway::ora::OraTapeGatewaySvc::OraTapeGatewaySvc(const std::string name) :
  ITapeGatewaySvc(),
  OraCommonSvc(name),
  m_getStreamsWithoutTapesStatement(0),
  m_attachTapesToStreamsStatement(0),
  m_getTapesWithoutDriveReqsStatement(0),
  m_attachDriveReqsToTapesStatement(0),
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
  m_getTapeToReleaseStatement(0)
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

    if ( m_getStreamsWithoutTapesStatement ) deleteStatement(m_getStreamsWithoutTapesStatement);
    if ( m_attachTapesToStreamsStatement ) deleteStatement(m_attachTapesToStreamsStatement);
    if ( m_getTapesWithoutDriveReqsStatement ) deleteStatement(m_getTapesWithoutDriveReqsStatement); 
    if ( m_attachDriveReqsToTapesStatement ) deleteStatement(m_attachDriveReqsToTapesStatement);
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

  } catch (oracle::occi::SQLException e) {};

  // Now reset all pointers to 0


  m_getStreamsWithoutTapesStatement= 0; 
  m_attachTapesToStreamsStatement = 0;
  m_getTapesWithoutDriveReqsStatement= 0;
  m_attachDriveReqsToTapesStatement= 0;
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

}

//----------------------------------------------------------------------------
// getStreamsWithoutTapes
//----------------------------------------------------------------------------

std::vector<castor::stager::Stream*> castor::tape::tapegateway::ora::OraTapeGatewaySvc::getStreamsWithoutTapes()
  throw (castor::exception::Exception){

  std::vector<castor::stager::Stream*> result;
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
      castor::exception::Internal ex;
      ex.getMessage()
        << "stream to resolve: no stream found";
      throw ex;
    }

    oracle::occi::ResultSet *rs =
      m_getStreamsWithoutTapesStatement->getCursor(1);

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
    m_getStreamsWithoutTapesStatement->closeResultSet(rs);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getStreamsWithoutTapes"
      << std::endl << e.what();
    throw ex;
  }
  return result;
}

//----------------------------------------------------------------------------
// attachTapesToStreams
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::attachTapesToStreams(std::vector<u_signed64> strIds, std::vector<std::string> vids, std::vector<int> fseqs)
          throw (castor::exception::Exception){

  try {

    if ( !strIds.size() || !vids.size() || !fseqs.size() ) {
      // just release the lock no result
      cnvSvc()->commit();
      return;
    }
     
    // Check whether the statements are ok
    if (0 == m_attachTapesToStreamsStatement) {
      m_attachTapesToStreamsStatement =
        createStatement(s_attachTapesToStreamsStatementString);
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
       ex.getMessage() << "invalid VID in attachTapesToStreams"
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
     m_attachTapesToStreamsStatement->setDataBufferArray(1,bufferFseqs, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensFseqs);
     m_attachTapesToStreamsStatement->setDataBufferArray(2,bufferStrIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensStrIds);
     ub4 len=nb;
     m_attachTapesToStreamsStatement->setDataBufferArray(3, bufferVids, oracle::occi::OCCI_SQLT_CHR,len, &len, maxLen, lensVids);

     // execute the statement and see whether we found something
 
     m_attachTapesToStreamsStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in attachTapesToStreams"
      << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// getTapesWithoutDriveReqs 
//----------------------------------------------------------------------------

std::vector<castor::tape::tapegateway::TapeGatewayRequest*> castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapesWithoutDriveReqs()
          throw (castor::exception::Exception){

  std::vector<castor::tape::tapegateway::TapeGatewayRequest*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_getTapesWithoutDriveReqsStatement) {
      m_getTapesWithoutDriveReqsStatement =
        createStatement(s_getTapesWithoutDriveReqsStatementString);
      m_getTapesWithoutDriveReqsStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    unsigned int nb = m_getTapesWithoutDriveReqsStatement->executeUpdate();

    if (0 == nb) {
      cnvSvc()->commit();
      castor::exception::Internal ex;
      ex.getMessage()
        << "stream to submit : no stream found";
      throw ex;
    }

    oracle::occi::ResultSet *rs =
      m_getTapesWithoutDriveReqsStatement->getCursor(1);

    // Run through the cursor
    oracle::occi::ResultSet::Status status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::stager::Tape* tape= new  castor::stager::Tape();
      tape->setTpmode(rs->getInt(1));
      tape->setSide(rs->getInt(2));
      tape->setVid(rs->getString(3));
      castor::tape::tapegateway::TapeGatewayRequest* item= new  castor::tape::tapegateway::TapeGatewayRequest();
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

    m_getTapesWithoutDriveReqsStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getTapesWithoutDriveReqs"
      << std::endl << e.what();
    throw ex;
  }
  return result;
}

//----------------------------------------------------------------------------
// attachDriveReqsToTapes
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::attachDriveReqsToTapes(std::vector<castor::tape::tapegateway::TapeGatewayRequest*> tapeRequests) throw (castor::exception::Exception){ 

  try {

    if ( !tapeRequests.size() ) {
      cnvSvc()->commit();
      return;
    }
     
    // Check whether the statements are ok
    if (0 == m_attachDriveReqsToTapesStatement) {
      m_attachDriveReqsToTapesStatement =
        createStatement(s_attachDriveReqsToTapesStatementString);
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

     m_attachDriveReqsToTapesStatement->setDataBufferArray(1,bufferIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensIds);
     m_attachDriveReqsToTapesStatement->setDataBufferArray(2, bufferVdqmIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensVdqmIds);
     m_attachDriveReqsToTapesStatement->setDataBufferArray(3, bufferDgns, oracle::occi::OCCI_SQLT_CHR,nb, &nb, (CA_MAXDGNLEN+1), lensDgns);
     m_attachDriveReqsToTapesStatement->setDataBufferArray(4, bufferLabels, oracle::occi::OCCI_SQLT_CHR,nb, &nb, (CA_MAXLBLTYPLEN+1), lensLabels);
     m_attachDriveReqsToTapesStatement->setDataBufferArray(5, bufferDensities, oracle::occi::OCCI_SQLT_CHR,nb, &nb, (CA_MAXDENLEN+1), lensDensities);
 
     // execute the statement and see whether we found something

     m_attachDriveReqsToTapesStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in attachDriveReqsToTapes"
      << std::endl << e.what();
    throw ex;
  }
  
}

//----------------------------------------------------------------------------
// getTapesWithDriveReqs
//----------------------------------------------------------------------------

std::vector<castor::tape::tapegateway::TapeGatewayRequest*> castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapesWithDriveReqs(u_signed64 timeOut) 
  throw (castor::exception::Exception){

  std::vector<tape::tapegateway::TapeGatewayRequest*> result;
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
      cnvSvc()->commit();
      castor::exception::Internal ex;
      ex.getMessage()
        << "tapes to check : no tape found";
      throw ex;
    }

    oracle::occi::ResultSet *rs =
      m_getTapesWithDriveReqsStatement->getCursor(2);

    // Run through the cursor
    oracle::occi::ResultSet::Status status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      tape::tapegateway::TapeGatewayRequest* item =
        new tape::tapegateway::TapeGatewayRequest();
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

    m_getTapesWithDriveReqsStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getTapesWithDriveReqs"
      << std::endl << e.what();
    throw ex;
  }
  return result;

}
 


//----------------------------------------------------------------------------
// restartLostReqs 
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::restartLostReqs(std::vector<castor::tape::tapegateway::TapeGatewayRequest*> tapes)
          throw (castor::exception::Exception){

  try {

    if ( !tapes.size()) {
      cnvSvc()->commit();
      return;
    }
     
    // Check whether the statements are ok
    if (0 == m_restartLostReqsStatement) {
      m_restartLostReqsStatement =
        createStatement(s_restartLostReqsStatementString);
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
    m_restartLostReqsStatement->setDataBufferArray(1,bufferTapes, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensTapes);
    // execute the statement and see whether we found something
 
    m_restartLostReqsStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
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

castor::tape::tapegateway::FileToMigrate* castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFileToMigrate(castor::tape::tapegateway::FileToMigrateRequest& req) throw (castor::exception::Exception){
  castor::tape::tapegateway::FileToMigrate* result=NULL;
  std::string diskserver;
  std::string mountpoint;
  try {

    while (1) { // until we get a valid file
    
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
	return NULL;
      }

      if (ret == -2 ) {// UNKNOWN request
	cnvSvc()->commit();
	castor::exception::Exception e(EINVAL);
	throw e;
      }

      std::string vid=m_getFileToMigrateStatement->getString(3); 

      oracle::occi::ResultSet *rs =
	m_getFileToMigrateStatement->getCursor(4);

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
	result->setLastKnownFilename(rs->getString(7));
	result->setFseq(rs->getInt(8));
	result->setFileSize((u_signed64)rs->getDouble(9));
	result->setFileTransactionId((u_signed64)rs->getDouble(10));

	result->setMountTransactionId(req.mountTransactionId());
	result->setPositionCommandCode(TPPOSIT_FSEQ);

	try {
	  NsTapeGatewayHelper nsHelper;
	  nsHelper.checkFileToMigrate(*result,vid);
	} catch (castor::exception::Exception e) {

	  struct Cns_fileid castorFileId;
	  memset(&castorFileId,'\0',sizeof(castorFileId));
	  strncpy(
		  castorFileId.server,
		  result->nshost().c_str(),
		  sizeof(castorFileId.server)-1
		  );
	  castorFileId.fileid = result->fileid();

	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str()),
	     castor::dlf::Param("TPVID", vid)
	    };
    
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, ORA_FILE_TO_MIGRATE_NS_ERROR, 3, params, &castorFileId);
	  

	  try {

	    
	    FileErrorReport failure;
	    failure.setMountTransactionId(result->mountTransactionId()); 
	    failure.setFileid(result->fileid());
	    failure.setNshost(result->nshost());
	    failure.setFseq(result->fseq());
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

	  if (result) delete result;
	  result=NULL;
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
   
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFileToMigrate"
      << std::endl << e.what();
    throw ex;
  }
  cnvSvc()->commit();
  // hardcoded umask
  result->setUmask(022);  
  return result;
}

//----------------------------------------------------------------------------
// setFileMigrated  
//----------------------------------------------------------------------------
  
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::setFileMigrated(castor::tape::tapegateway::FileMigratedNotification& resp)
  throw (castor::exception::Exception){
  std::string diskserver;
  std::string mountpoint;


  try {
    // Check whether the statements are ok

    if (0 == m_setFileMigratedStatement) {
      m_setFileMigratedStatement =
        createStatement(s_setFileMigratedStatementString);
      m_setFileMigratedStatement->registerOutParam
        (5, oracle::occi::OCCICURSOR);
    }

    m_setFileMigratedStatement->setDouble(1,(double)resp.mountTransactionId()); // transaction id
    m_setFileMigratedStatement->setDouble(2,(double)resp.fileid());
    m_setFileMigratedStatement->setString(3,resp.nshost());
    m_setFileMigratedStatement->setInt(4,resp.fseq()); 
    
    m_setFileMigratedStatement->executeUpdate();

    oracle::occi::ResultSet *rs =
      m_setFileMigratedStatement->getCursor(5);

    // Run through the cursor 

    oracle::occi::ResultSet::Status status = rs->next();
    // just one

    if (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      diskserver = rs->getString(1);
      mountpoint = rs->getString(2);
    
      m_setFileMigratedStatement->closeResultSet(rs);

    
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
   
castor::tape::tapegateway::FileToRecall* castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFileToRecall(castor::tape::tapegateway::FileToRecallRequest& req) throw (castor::exception::Exception)
{
    
  castor::tape::tapegateway::FileToRecall* result=NULL;
  std::string diskserver;
  std::string mountpoint;

  try {

    while (1) {
     
    
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
      if (ret == -1 ) return NULL;
     
      if (ret == -2 ) {// UNKNOWN request
	castor::exception::Exception e(EINVAL);
	throw e;
      }
    

      std::string vid= m_getFileToRecallStatement->getString(3);
   
      oracle::occi::ResultSet *rs =
	m_getFileToRecallStatement->getCursor(4);

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
	result->setFileTransactionId((u_signed64)rs->getDouble(7));

	result->setMountTransactionId(req.mountTransactionId());
	result->setPositionCommandCode(TPPOSIT_BLKID);

	// it might be changed after the ns check to TPPOSIT_FSEQ

	// let's check if it is valid in the nameserver and let's retrieve the blockid

	try {
	  
	  NsTapeGatewayHelper nsHelper;
	  nsHelper.getBlockIdToRecall(*result,vid);
	  break; // found a valid file

	}catch (castor::exception::Exception e) {
	  struct Cns_fileid castorFileId;
	  memset(&castorFileId,'\0',sizeof(castorFileId));
	  strncpy(
		  castorFileId.server,
		  result->nshost().c_str(),
		  sizeof(castorFileId.server)-1
		  );
	  castorFileId.fileid = result->fileid();

	  castor::dlf::Param params[] =
	    {castor::dlf::Param("errorCode",sstrerror(e.code())),
	     castor::dlf::Param("errorMessage",e.getMessage().str()),
	     castor::dlf::Param("TPVID", vid)
	    };

	  
	  castor::dlf::dlf_writep(nullCuuid,ORA_FILE_TO_RECALL_NS_ERROR, 3, params,&castorFileId);

	  try {
	    FileErrorReport failure;
	    failure.setMountTransactionId(result->mountTransactionId()); 
	    failure.setFileid(result->fileid());
	    failure.setNshost(result->nshost());
	    failure.setFseq(result->fseq());
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

	  
	  if (result) delete result;
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
      // end the loop
      m_getFileToRecallStatement->closeResultSet(rs);
      break;
    }

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFileToRecall"
      << std::endl << e.what();
    throw ex;
  }

  cnvSvc()->commit();
 
  // hardcoded umask
  result->setUmask(077);  
  return result;

}
 
//----------------------------------------------------------------------------
// setFileRecalled  
//----------------------------------------------------------------------------
   
void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::setFileRecalled(castor::tape::tapegateway::FileRecalledNotification& resp) throw (castor::exception::Exception){
 std::string diskserver;
 std::string mountpoint;

  try {
    // Check whether the statements are ok

    if (0 == m_setFileRecalledStatement) {
      m_setFileRecalledStatement =
        createStatement(s_setFileRecalledStatementString);
      m_setFileRecalledStatement->registerOutParam
	(5, oracle::occi::OCCICURSOR); 
    }

    m_setFileRecalledStatement->setDouble(1,(double)resp.mountTransactionId());
    m_setFileRecalledStatement->setDouble(2,(double)resp.fileid());
    m_setFileRecalledStatement->setString(3,resp.nshost());
    m_setFileRecalledStatement->setInt(4,resp.fseq());

    m_setFileRecalledStatement->executeUpdate();
    
    
    
    oracle::occi::ResultSet *rs =
      m_setFileRecalledStatement->getCursor(5);

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

std::vector<castor::stager::TapeCopy*>  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFailedMigrations()
	  throw (castor::exception::Exception)
{
    
  std::vector<castor::stager::TapeCopy*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_getFailedMigrationsStatement) {
      m_getFailedMigrationsStatement =
        createStatement(s_getFailedMigrationsStatementString);
      m_getFailedMigrationsStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    m_getFailedMigrationsStatement->executeUpdate();

   oracle::occi::ResultSet *rs =
      m_getFailedMigrationsStatement->getCursor(1);

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

    m_getFailedMigrationsStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFailedMigrations"
      << std::endl << e.what();
    throw ex;
  }
  return result;

}


//----------------------------------------------------------------------------
// setMigRetryResult
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::setMigRetryResult(std::vector<u_signed64> tcToRetry, std::vector<u_signed64> tcToFail ) throw (castor::exception::Exception) {

  try { 
    // Check whether the statements are ok
    if (0 == m_setMigRetryResultStatement) {
      m_setMigRetryResultStatement =
        createStatement(s_setMigRetryResultStatementString);
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
    m_setMigRetryResultStatement->setDataBufferArray(1,bufferRetry, oracle::occi::OCCI_SQLT_NUM, nbRetry, &unusedRetry, 21, lensRetry);

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
    m_setMigRetryResultStatement->setDataBufferArray(2,bufferFail, oracle::occi::OCCI_SQLT_NUM, nbFail, &unusedFail, 21, lensFail);

    m_setMigRetryResultStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setMigRetryResult"
      << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// getFailedRecalls
//----------------------------------------------------------------------------

std::vector<castor::stager::TapeCopy*>  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFailedRecalls()
	  throw (castor::exception::Exception){
    
  std::vector<castor::stager::TapeCopy*> result;
  try {
    // Check whether the statements are ok
    if (0 == m_getFailedRecallsStatement) {
      m_getFailedRecallsStatement =
        createStatement(s_getFailedRecallsStatementString);
      m_getFailedRecallsStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }

    // execute the statement and see whether we found something
 
    m_getFailedRecallsStatement->executeUpdate();

   oracle::occi::ResultSet *rs =
      m_getFailedRecallsStatement->getCursor(1);

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

    m_getFailedRecallsStatement->closeResultSet(rs);

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFailedRecalls"
      << std::endl << e.what();
    throw ex;
  }
  return result;

}

//----------------------------------------------------------------------------
// setRecRetryResult
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::setRecRetryResult(std::vector<u_signed64> tcToRetry,std::vector<u_signed64> tcToFail) throw (castor::exception::Exception) {
 
  try {
     
    // Check whether the statements are ok
    if (0 == m_setRecRetryResultStatement) {
      m_setRecRetryResultStatement =
        createStatement(s_setRecRetryResultStatementString);
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
    m_setRecRetryResultStatement->setDataBufferArray(1,bufferRetry, oracle::occi::OCCI_SQLT_NUM, nbRetry, &unused, 21, lensRetry);

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
    m_setRecRetryResultStatement->setDataBufferArray(2,bufferFail, oracle::occi::OCCI_SQLT_NUM, nbFail, &unused, 21, lensFail);
    m_setRecRetryResultStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in setRecRetryResult"
      << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// getRepackVidAndFileInfo
//----------------------------------------------------------------------------
  
std::string  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getRepackVidAndFileInfo(castor::tape::tapegateway::FileMigratedNotification& resp, std::string& vid, int& copyNumber, u_signed64& lastModificationTime )
  throw (castor::exception::Exception){
  
  try {
    // Check whether the statements are ok

    if (0 == m_getRepackVidAndFileInfoStatement) {
      m_getRepackVidAndFileInfoStatement =
        createStatement(s_getRepackVidAndFileInfoStatementString);
      m_getRepackVidAndFileInfoStatement->registerOutParam
        (5, oracle::occi::OCCISTRING, 2048 );
      m_getRepackVidAndFileInfoStatement->registerOutParam
        (6, oracle::occi::OCCISTRING, 2048 );
      m_getRepackVidAndFileInfoStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_getRepackVidAndFileInfoStatement->registerOutParam
	(8, oracle::occi::OCCIDOUBLE);
    }

    m_getRepackVidAndFileInfoStatement->setDouble(1,(double)resp.fileid());
    m_getRepackVidAndFileInfoStatement->setString(2,resp.nshost());
    
    m_getRepackVidAndFileInfoStatement->setInt(3,resp.fseq());

    m_getRepackVidAndFileInfoStatement->setDouble(4,resp.mountTransactionId());
    // execute the statement 

    m_getRepackVidAndFileInfoStatement->executeUpdate();
    
    std::string repackVid = m_getRepackVidAndFileInfoStatement->getString(5);
    vid = m_getRepackVidAndFileInfoStatement->getString(6);
    copyNumber = m_getRepackVidAndFileInfoStatement->getInt(7);
    lastModificationTime= (u_signed64)m_getRepackVidAndFileInfoStatement->getDouble(8);

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
// startTapeSession
//-------------------------------------------------------------------------- 
	 

castor::tape::tapegateway::Volume* castor::tape::tapegateway::ora::OraTapeGatewaySvc::startTapeSession(castor::tape::tapegateway::VolumeRequest& startRequest) throw (castor::exception::Exception){ 
  
  castor::tape::tapegateway::Volume* result=NULL;  

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
      return NULL;
    }
    if (ret == -2) {
      // UNKNOWN request
      castor::exception::Exception e(EINVAL);
      throw e;
    }
   
    result = new Volume();
    result->setClientType(TAPE_GATEWAY);
    result->setVid(m_startTapeSessionStatement->getString(2));
    result->setMode((castor::tape::tapegateway::VolumeMode)m_startTapeSessionStatement->getInt(3));
    result->setDensity(m_startTapeSessionStatement->getString(5));
    result->setLabel(m_startTapeSessionStatement->getString(6));
    result->setMountTransactionId(startRequest.mountTransactionId());
    

  } catch (oracle::occi::SQLException e) {
    if (result) delete result;
    result=NULL;

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in startTapeSession"
      << std::endl << e.what();
    throw ex;
  }
  return result;


} 

//----------------------------------------------------------------------------
// endTapeSession
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::endTapeSession(castor::tape::tapegateway::EndNotification& endRequest) throw (castor::exception::Exception){

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
		
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getSegmentInfo(FileRecalledNotification &fileRecalled, std::string& vid,int& copyNb ) throw (castor::exception::Exception){
  
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

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::failTapeSession(castor::tape::tapegateway::EndNotificationErrorReport& failure) throw (castor::exception::Exception){
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

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::failFileTransfer(FileErrorReport& failure) throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok

    if (0 == m_failFileTransferStatement) {
      m_failFileTransferStatement =
        createStatement(s_failFileTransferStatementString);
    }

    m_failFileTransferStatement->setDouble(1,(double)failure.mountTransactionId()); 
    m_failFileTransferStatement->setDouble(2,(double)failure.fileid());
    m_failFileTransferStatement->setString(3,failure.nshost());
    m_failFileTransferStatement->setInt(4,failure.fseq());
    m_failFileTransferStatement->setInt(5,failure.errorCode());
    
    m_failFileTransferStatement->executeUpdate();
    cnvSvc()->commit(); 
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

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::invalidateFile(FileErrorReport& failure) throw (castor::exception::Exception){

  try {
    // Check whether the statements are ok

    if (0 == m_invalidateFileStatement) {
      m_invalidateFileStatement =
        createStatement(s_invalidateFileStatementString);
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

castor::stager::Tape castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapeToRelease(u_signed64 mountTransactionId) throw (castor::exception::Exception){
  castor::stager::Tape result;
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

    result.setVid(m_getTapeToReleaseStatement->getString(2));
    result.setTpmode(m_getTapeToReleaseStatement->getInt(3));

  } catch (oracle::occi::SQLException e) {
   
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getTapeToRelease"
      << std::endl << e.what();
    throw ex;
  }
  return result;
}


