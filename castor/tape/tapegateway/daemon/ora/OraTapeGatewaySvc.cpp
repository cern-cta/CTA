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
 * Implementation of the ITapeGatewaySvc for Oracle
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files

#include <serrno.h>
#include <string>
#include <list>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <memory>

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

#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/PositionCommandCode.hpp"
#include "castor/tape/tapegateway/RetryPolicyElement.hpp"
#include "castor/tape/tapegateway/TapeGatewayDlfMessageConstants.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/db/ora/SmartOcciResultSet.hpp"

#include "castor/tape/tapegateway/daemon/NsTapeGatewayHelper.hpp"
#include "castor/tape/tapegateway/daemon/ora/OraTapeGatewaySvc.hpp"


//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::tape::tapegateway::ora::OraTapeGatewaySvc>* s_factoryOraTapeGatewaySvc =
  new castor::SvcFactory<castor::tape::tapegateway::ora::OraTapeGatewaySvc>();

#if __GNUC__ >= 2
#define USE(var) void use_##var (void) {var = var;}
USE (s_factoryOraTapeGatewaySvc);
#endif

//------------------------------------------------------------------------------
// OraTapeGatewaySvc
//------------------------------------------------------------------------------
castor::tape::tapegateway::ora::OraTapeGatewaySvc::OraTapeGatewaySvc(const std::string name) :
  ITapeGatewaySvc(),
  OraCommonSvc(name),
  m_getMigrationMountsWithoutTapesStatement(0),
  m_attachTapesToMigMountsStatement(0),
  m_getTapeWithoutDriveReqStatement(0),
  m_attachDriveReqStatement(0),
  m_getTapesWithDriveReqsStatement(0),
  m_restartLostReqsStatement(0),
  m_getFailedMigrationsStatement(0),
  m_setMigRetryResultStatement(0),
  m_startTapeSessionStatement(0),
  m_endTapeSessionStatement(0),
  m_failFileTransferStatement(0),
  m_getTapeToReleaseStatement(0),
  m_cancelMigrationOrRecallStatement(0),
  m_deleteMigrationMountWithBadTapePoolStatement(0),
  m_flagTapeFullForMigrationSession(0),
  m_getMigrationMountVid(0),
  m_getBulkFilesToMigrate(0),
  m_getBulkFilesToRecall(0),
  m_setBulkFileMigrationResult(0),
  m_setBulkFileRecallResult(0)
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
    if ( m_getMigrationMountsWithoutTapesStatement ) deleteStatement(m_getMigrationMountsWithoutTapesStatement);
    if ( m_attachTapesToMigMountsStatement ) deleteStatement(m_attachTapesToMigMountsStatement);
    if ( m_getTapeWithoutDriveReqStatement ) deleteStatement(m_getTapeWithoutDriveReqStatement); 
    if ( m_attachDriveReqStatement ) deleteStatement(m_attachDriveReqStatement);
    if ( m_getTapesWithDriveReqsStatement ) deleteStatement(m_getTapesWithDriveReqsStatement);
    if ( m_restartLostReqsStatement ) deleteStatement( m_restartLostReqsStatement);
    if ( m_getFailedMigrationsStatement ) deleteStatement(m_getFailedMigrationsStatement);
    if ( m_setMigRetryResultStatement ) deleteStatement(m_setMigRetryResultStatement);
    if ( m_startTapeSessionStatement ) deleteStatement(m_startTapeSessionStatement);
    if ( m_endTapeSessionStatement ) deleteStatement(m_endTapeSessionStatement);
    if ( m_failFileTransferStatement ) deleteStatement(m_failFileTransferStatement);
    if ( m_getTapeToReleaseStatement ) deleteStatement(m_getTapeToReleaseStatement);
    if ( m_cancelMigrationOrRecallStatement ) deleteStatement(m_cancelMigrationOrRecallStatement);
    if ( m_deleteMigrationMountWithBadTapePoolStatement ) deleteStatement(m_deleteMigrationMountWithBadTapePoolStatement);    
    if ( m_flagTapeFullForMigrationSession ) deleteStatement(m_flagTapeFullForMigrationSession);
    if ( m_getMigrationMountVid ) deleteStatement(m_getMigrationMountVid);
    if ( m_getBulkFilesToMigrate ) deleteStatement(m_getBulkFilesToMigrate);
    if ( m_getBulkFilesToRecall ) deleteStatement(m_getBulkFilesToRecall);
    if ( m_setBulkFileMigrationResult ) deleteStatement(m_setBulkFileMigrationResult);
    if ( m_setBulkFileRecallResult ) deleteStatement(m_setBulkFileRecallResult);
  } catch (castor::exception::Exception& ignored) {};
  // Now reset all pointers to 0
  m_getMigrationMountsWithoutTapesStatement= 0; 
  m_attachTapesToMigMountsStatement = 0;
  m_getTapeWithoutDriveReqStatement= 0;
  m_attachDriveReqStatement= 0;
  m_getTapesWithDriveReqsStatement = 0;
  m_restartLostReqsStatement = 0;
  m_getFailedMigrationsStatement = 0;
  m_setMigRetryResultStatement= 0;
  m_startTapeSessionStatement = 0;
  m_endTapeSessionStatement = 0;
  m_failFileTransferStatement= 0;
  m_getTapeToReleaseStatement=0;
  m_cancelMigrationOrRecallStatement=0;
  m_deleteMigrationMountWithBadTapePoolStatement=0;
  m_flagTapeFullForMigrationSession=0;
  m_getMigrationMountVid=0;
  m_getBulkFilesToMigrate=0;
  m_getBulkFilesToRecall=0;
  m_setBulkFileMigrationResult=0;
  m_setBulkFileRecallResult=0;
}

//----------------------------------------------------------------------------
// getMigrationMountsWithoutTapes
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getMigrationMountsWithoutTapes
(std::list<castor::tape::tapegateway::ITapeGatewaySvc::migrationMountParameters>& migrationMounts)
  throw (castor::exception::Exception){
  oracle::occi::ResultSet *rs = NULL;
  try {
    // Check whether the statements are ok
    if (!m_getMigrationMountsWithoutTapesStatement) {
      m_getMigrationMountsWithoutTapesStatement =
        createStatement("BEGIN tg_getMigMountsWithoutTapes(:1);END;");
      m_getMigrationMountsWithoutTapesStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }
    // execute the statement and see whether we found something
    unsigned int nb = m_getMigrationMountsWithoutTapesStatement->executeUpdate();
    if (0 == nb) {
      cnvSvc()->commit(); 
      return;
    }
    rs = m_getMigrationMountsWithoutTapesStatement->getCursor(1);
    // Identify the columns of the cursor
    resultSetIntrospector resIntros (rs);
    int idIdx       = resIntros.findColumnIndex(  "ID", oracle::occi::OCCI_SQLT_NUM);
    int TPNameIdx   = resIntros.findColumnIndex("NAME", oracle::occi::OCCI_SQLT_CHR);
    // Run through the cursor
    while( rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::tape::tapegateway::ITapeGatewaySvc::migrationMountParameters item;
      item.migrationMountId     = (u_signed64) (double) rs->getNumber(idIdx);
      // Note that we hardcode 1 for the initialSizeToTransfer. This parameter should actually
      // be dropped and the call to VMGR changed accordingly.
      item.initialSizeToTransfer = 1;
      item.tapePoolName = rs->getString(TPNameIdx);
      migrationMounts.push_back(item);
    }
    m_getMigrationMountsWithoutTapesStatement->closeResultSet(rs);
  } catch (oracle::occi::SQLException& e) {
    if (rs) m_getMigrationMountsWithoutTapesStatement->closeResultSet(rs);
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getMigrationMountsWithoutTapes"
      << std::endl << e.what();
    throw ex;
  } catch (std::exception& e) {
    if (rs) m_getMigrationMountsWithoutTapesStatement->closeResultSet(rs);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getMigrationMountsWithoutTapes"
      << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// attachTapesToMigMounts
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::attachTapesToMigMounts(const std::list<u_signed64>& MMIds,const std::list<std::string>& vids, const std::list<int>& fseqs)
          throw (castor::exception::Exception){
  unsigned char (*bufferFseqs)[21]=NULL;
  ub2 *lensFseqs=NULL;
  unsigned char (*bufferMigrationMountIds)[21]=NULL;
  ub2 *lensMMIds=NULL;
  char * bufferVids=NULL;
  ub2 *lensVids=NULL;
  try {
    if ( !MMIds.size() || !vids.size() || !fseqs.size() ) {
      // just release the lock no result
      cnvSvc()->commit();
      return;
    }
    if (MMIds.size() != vids.size() || MMIds.size() != fseqs.size()) {
      // just release the lock no result
      cnvSvc()->commit();
      castor::exception::Exception e(EINVAL);
      throw e;
    }
    // Check whether the statements are ok
    if (0 == m_attachTapesToMigMountsStatement) {
      m_attachTapesToMigMountsStatement =
        createStatement("BEGIN tg_attachTapesToMigMounts(:1,:2,:3);END;");
      m_attachTapesToMigMountsStatement->setAutoCommit(true);
    }
    // input
    ub4 nb=MMIds.size();
    // fseq
    bufferFseqs=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    lensFseqs=(ub2 *)malloc (sizeof(ub2)*nb);
    if ( lensFseqs  == 0 || bufferFseqs == 0 ) {
      if (lensFseqs != 0 ) free(lensFseqs);
      if (bufferFseqs != 0 ) free(bufferFseqs);
      castor::exception::OutOfMemory e; 
      throw e;
    }
    // MMIds
    bufferMigrationMountIds =(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    lensMMIds=(ub2 *)malloc (sizeof(ub2)*nb);
    if ( lensMMIds  == 0 || bufferMigrationMountIds == 0 ) {
      if (lensMMIds != 0 ) free(lensMMIds);
      if (bufferMigrationMountIds != 0) free(bufferMigrationMountIds);
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
      if (lensMMIds != 0 ) free(lensMMIds);
      if (bufferMigrationMountIds != 0) free(bufferMigrationMountIds);
      if (lensFseqs != 0 ) free(lensFseqs);
      if (bufferFseqs != 0 ) free(bufferFseqs);
      castor::exception::Internal ex;
      ex.getMessage() << "invalid VID in attachTapesToMigMounts"
                      << std::endl;
      throw ex;
    }
    unsigned int bufferCellSize = maxLen * sizeof(char);
    lensVids = (ub2*) malloc(nb * sizeof(ub2));
    bufferVids =
      (char*) malloc(nb * bufferCellSize);
    if ( lensVids  == 0 || bufferVids == 0 ) {
      if (lensMMIds != 0 ) free(lensMMIds);
      if (bufferMigrationMountIds != 0) free(bufferMigrationMountIds);
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
          strId = MMIds.begin(),
          fseq = fseqs.begin();
        strId != MMIds.end();
        vid++,strId++,fseq++,i++ ){
      // fseq
      oracle::occi::Number n = (double)(*fseq);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferFseqs[i],b.length());
      lensFseqs[i] = b.length();
      // Migration mount Ids
      n = (double)(*strId);
      b = n.toBytes();
      b.getBytes(bufferMigrationMountIds[i],b.length());
      lensMMIds[i] = b.length();
      // vids
      lensVids[i]= (*vid).length();
      strncpy(bufferVids+(bufferCellSize*i),(*vid).c_str(),lensVids[i]);
    }
    ub4 unused=nb;
    /* Attach buffer to inStartFseqs */
    m_attachTapesToMigMountsStatement->setDataBufferArray(1,bufferFseqs, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensFseqs);
    /* Attach array to inMountIds */
    m_attachTapesToMigMountsStatement->setDataBufferArray(2,bufferMigrationMountIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensMMIds);
    ub4 len=nb;
    /* Attach array to inTapeVids */
    m_attachTapesToMigMountsStatement->setDataBufferArray(3, bufferVids, oracle::occi::OCCI_SQLT_CHR,len, &len, maxLen, lensVids);
    // execute the statement and see whether we found something
    m_attachTapesToMigMountsStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    if (lensMMIds != 0 ) free(lensMMIds);
    if (bufferMigrationMountIds != 0) free(bufferMigrationMountIds);
    if (lensVids != 0 ) free(lensVids);
    if (bufferVids != 0) free(bufferVids);
    if (lensFseqs != 0 ) free(lensFseqs);
    if (bufferFseqs != 0 ) free(bufferFseqs);
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in attachTapesToMigMounts"
      << std::endl << e.what();
    throw ex;
  }
  if (lensMMIds != 0 ) free(lensMMIds);
  if (bufferMigrationMountIds != 0) free(bufferMigrationMountIds);
  if (lensVids != 0 ) free(lensVids);
  if (bufferVids != 0) free(bufferVids);
  if (lensFseqs != 0 ) free(lensFseqs);
  if (bufferFseqs != 0 ) free(bufferFseqs);
}


//----------------------------------------------------------------------------
// getTapeWithoutDriveReq 
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapeWithoutDriveReq
(std::string &vid, int &vdqmPriority, int &mode)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_getTapeWithoutDriveReqStatement) {
      m_getTapeWithoutDriveReqStatement =
        createStatement("BEGIN tg_getTapeWithoutDriveReq(:1,:2,:3);END;");
      m_getTapeWithoutDriveReqStatement->registerOutParam(1, oracle::occi::OCCISTRING, 2048);
      m_getTapeWithoutDriveReqStatement->registerOutParam(2, oracle::occi::OCCIINT);
      m_getTapeWithoutDriveReqStatement->registerOutParam(3, oracle::occi::OCCIINT);
    }
    // execute the statement
    m_getTapeWithoutDriveReqStatement->executeUpdate();
    // get output values
    vid = m_getTapeWithoutDriveReqStatement->getString(1);
    vdqmPriority = m_getTapeWithoutDriveReqStatement->getInt(2);
    mode = m_getTapeWithoutDriveReqStatement->getInt(3);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in getTapeWithoutDriveReq"
                    << std::endl << e.what();
    throw ex;
  }
}
 
//----------------------------------------------------------------------------
// attachDriveReq
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::attachDriveReq
(const std::string &vid, const u_signed64 mountTransactionId, const int mode,
 const char *label, const char *density)
  throw (castor::exception::Exception){
  try {
    // Check whether the statements are ok
    if (0 == m_attachDriveReqStatement) {
      m_attachDriveReqStatement =
        createStatement("BEGIN tg_attachDriveReq(:1,:2,:3,:4,:5);END;");
      m_attachDriveReqStatement->setAutoCommit(true);
    }
    // execute the statement
    m_attachDriveReqStatement->setString(1, vid);
    m_attachDriveReqStatement->setDouble(2, (double)mountTransactionId);
    m_attachDriveReqStatement->setInt(3, mode);
    m_attachDriveReqStatement->setString(4, label);
    m_attachDriveReqStatement->setString(5, density);
    m_attachDriveReqStatement->executeUpdate(); 
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in attachDriveReq"
      << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// getTapesWithDriveReqs
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapesWithDriveReqs
(std::list<TapeRequest>& requests,
 const u_signed64& timeOut) 
  throw (castor::exception::Exception){
  try {
    // Check whether the statements are ok
    if (0 == m_getTapesWithDriveReqsStatement) {
      m_getTapesWithDriveReqsStatement =
        createStatement("BEGIN tg_getTapesWithDriveReqs(:1,:2);END;");
      // This cursor has 3 columns : mode, mountTransactionId, VID
      m_getTapesWithDriveReqsStatement->registerOutParam(2, oracle::occi::OCCICURSOR);
    }
    // execute the statement and see whether we found something
    m_getTapesWithDriveReqsStatement->setDouble(1,(double)timeOut);
    unsigned int nb = m_getTapesWithDriveReqsStatement->executeUpdate();
    if (0 == nb) {
      // nothing found, return
      return;
    }
    // Run through the results
    oracle::occi::ResultSet *rs = m_getTapesWithDriveReqsStatement->getCursor(2);
    while(rs->next()  == oracle::occi::ResultSet::DATA_AVAILABLE) {
      TapeRequest tr;
      tr.mode = rs->getInt(1);
      tr.mountTransactionId = (u_signed64)rs->getDouble(2);
      tr.vid = rs->getString(3);
      requests.push_back(tr);
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
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::restartLostReqs
(const std::list<int>& mountTransactionIds)
  throw (castor::exception::Exception){
  unsigned char (*bufferMountTransactionIds)[21]=NULL;
  ub2 *lensMountTransactionIds=NULL;
  try {
    if (!mountTransactionIds.size()) {
      cnvSvc()->commit();
      return;
    }
    // Check whether the statements are ok
    if (0 == m_restartLostReqsStatement) {
      m_restartLostReqsStatement = createStatement("BEGIN tg_restartLostReqs(:1);END;");
      m_restartLostReqsStatement->setAutoCommit(true);
    }
    // input
    ub4 nb = mountTransactionIds.size();
    bufferMountTransactionIds=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    lensMountTransactionIds=(ub2 *)malloc (sizeof(ub2)*nb);
    if ( lensMountTransactionIds == 0 || bufferMountTransactionIds == 0 ) {
      if (lensMountTransactionIds != 0 ) free(lensMountTransactionIds);
      if (bufferMountTransactionIds != 0) free(bufferMountTransactionIds);
      castor::exception::OutOfMemory e; 
      throw e;
     }
    // Fill in the structure
    int i=0;
    for (std::list<int>::const_iterator mountTransactionId = mountTransactionIds.begin();
	 mountTransactionId != mountTransactionIds.end();
	 mountTransactionId++,i++){
      oracle::occi::Number n = (double)(*mountTransactionId);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferMountTransactionIds[i],b.length());
      lensMountTransactionIds[i] = b.length();
    }
    ub4 unused=nb;
    m_restartLostReqsStatement->setDataBufferArray
      (1,bufferMountTransactionIds, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensMountTransactionIds);
    // execute the statement
    m_restartLostReqsStatement->executeUpdate();
    // free memory
    if (lensMountTransactionIds != 0 ) free(lensMountTransactionIds);
    if (bufferMountTransactionIds != 0) free(bufferMountTransactionIds);
  } catch (oracle::occi::SQLException e) {
    // free momery
    if (lensMountTransactionIds != 0 ) free(lensMountTransactionIds);
    if (bufferMountTransactionIds != 0) free(bufferMountTransactionIds);
    // handle exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in restartLostReqs "
      << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// getFailedMigrations
//----------------------------------------------------------------------------

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::getFailedMigrations(
    std::list<castor::tape::tapegateway::RetryPolicyElement>& candidates)
          throw (castor::exception::Exception)
{
  oracle::occi::ResultSet *rs = NULL;
  try {
    // Check whether the statements are ok
    if (!m_getFailedMigrationsStatement) {
      m_getFailedMigrationsStatement =
        createStatement("BEGIN tg_getFailedMigrations(:1);END;");
      m_getFailedMigrationsStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR);
    }
    // execute the statement and see whether we found something
    unsigned int nb = m_getFailedMigrationsStatement->executeUpdate();
    if (0 == nb) {
      cnvSvc()->commit(); 
      return;
    }
    rs = m_getFailedMigrationsStatement->getCursor(1);
    // Find columns in the cursor
    resultSetIntrospector resIntros (rs);
    int MigrationJobIdIndex = resIntros.findColumnIndex(       "ID", oracle::occi::OCCI_SQLT_NUM);
    int ErrorCodeIndex  =     resIntros.findColumnIndex("ERRORCODE", oracle::occi::OCCI_SQLT_NUM);
    int NbRetryIndex    =     resIntros.findColumnIndex(  "NBRETRY", oracle::occi::OCCI_SQLT_NUM);
    int NsHostIndex     =     resIntros.findColumnIndex(   "NSHOST", oracle::occi::OCCI_SQLT_CHR);
    int FileIdIndex     =     resIntros.findColumnIndex(   "FILEID", oracle::occi::OCCI_SQLT_NUM);
    // Run through the cursor
    while (rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::tape::tapegateway::RetryPolicyElement item;
      item.migrationOrRecallJobId = (u_signed64) (double) occiNumber(rs->getNumber(MigrationJobIdIndex));
      item.errorCode              =                 (int) occiNumber(rs->getNumber(ErrorCodeIndex));
      item.nbRetry                =                 (int) occiNumber(rs->getNumber(NbRetryIndex));
      item.nsHost                 =                                  rs->getString(NsHostIndex);
      item.fileId                 = (u_signed64) (double) occiNumber(rs->getNumber(FileIdIndex));
      item.tape                   = "";
      item.fSeq                   = 0;
      candidates.push_back(item);
    }
    m_getFailedMigrationsStatement->closeResultSet(rs);
  } catch (oracle::occi::SQLException& e) {
    if (rs) m_getFailedMigrationsStatement->closeResultSet(rs);
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getFailedMigrations"
      << std::endl << e.what();
    throw ex;
  } catch (std::exception &e) { // This case is almost identical to the previous one, but does not call handleException
    if (rs) m_getFailedMigrationsStatement->closeResultSet(rs);
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

void  castor::tape::tapegateway::ora::OraTapeGatewaySvc::setMigRetryResult(const std::list<u_signed64>& mjToRetry, const std::list<u_signed64>&  mjToFail ) throw (castor::exception::Exception) {

  
 unsigned char (*bufferRetry)[21]=NULL;
 ub2 *lensRetry=NULL;
 unsigned char (*bufferFail)[21]=NULL;
 ub2 *lensFail = NULL;

  try { 
    // Check whether the statements are ok
    if (0 == m_setMigRetryResultStatement) {
      m_setMigRetryResultStatement =
        createStatement("BEGIN tg_setMigRetryResult(:1,:2);END;");
    }

    // success

    ub4 nbRetry= mjToRetry.size();
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
    
    for (std::list<u_signed64>::const_iterator elem= mjToRetry.begin(); 
         elem != mjToRetry.end();
         elem++, i++){
        oracle::occi::Number n = (double)(*elem);
        oracle::occi::Bytes b = n.toBytes();
        b.getBytes(bufferRetry[i],b.length());
        lensRetry[i] = b.length();
    }

    // if there where no successfull migration
    if (mjToRetry.size() == 0){
      //let's put -1
      oracle::occi::Number n = (double)(-1);
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferRetry[0],b.length());
      lensRetry[0] = b.length();
    }

    ub4 unusedRetry =nbRetry;
    m_setMigRetryResultStatement->setDataBufferArray(1,bufferRetry, oracle::occi::OCCI_SQLT_NUM, nbRetry, &unusedRetry, 21, lensRetry);

    // failures

    ub4 nbFail = mjToFail.size();
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

    for (std::list<u_signed64>::const_iterator elem=mjToFail.begin();
          elem != mjToFail.end();
          elem++,i++){
      
        oracle::occi::Number n = (double)(*elem);
        oracle::occi::Bytes b = n.toBytes();
        b.getBytes(bufferFail[i],b.length());
        lensFail[i] = b.length();

    }

    // if there where no failed migration

    if (mjToFail.size() == 0){
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

//--------------------------------------------------------------------------
// startTapeSession
//--------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::startTapeSession(const castor::tape::tapegateway::VolumeRequest& startRequest, castor::tape::tapegateway::Volume& volume ) throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_startTapeSessionStatement) {
      m_startTapeSessionStatement =
        createStatement("BEGIN tg_startTapeSession(:1,:2,:3,:4,:5,:6);END;");
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
      m_startTapeSessionStatement->setAutoCommit(true);
    }
    m_startTapeSessionStatement->setDouble(1,(double)startRequest.mountTransactionId());
    // execute the statement
    m_startTapeSessionStatement->executeUpdate();
    int ret = m_startTapeSessionStatement->getInt(4);
    if (ret == -1) {
      // No more files
      return;
    }
    if (ret == -2) {
      // Unknown request
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
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::endTapeSession
(const u_signed64 mountTransactionId, const int errorCode)
  throw (castor::exception::Exception){
  try {
    // Check whether the statements are ok
    if (0 == m_endTapeSessionStatement) {
      m_endTapeSessionStatement = createStatement("BEGIN tg_endTapeSession(:1,:2);END;");
      m_endTapeSessionStatement->setAutoCommit(true);
    }
    // run statement
    m_endTapeSessionStatement->setDouble(1, (double)mountTransactionId); 
    m_endTapeSessionStatement->setInt(2, errorCode);
    m_endTapeSessionStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in failTapeSession" << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// getTapeToRelease
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getTapeToRelease
(const u_signed64& mountTransactionId, 
 castor::tape::tapegateway::ITapeGatewaySvc::TapeToReleaseInfo& tape)
  throw (castor::exception::Exception){  
  try {
    // Check whether the statements are ok
    if (0 == m_getTapeToReleaseStatement) {
      m_getTapeToReleaseStatement =
        createStatement("BEGIN tg_getTapeToRelease(:1,:2,:3,:4);END;");
      m_getTapeToReleaseStatement->registerOutParam(2, oracle::occi::OCCISTRING, 2048 ); 
      m_getTapeToReleaseStatement->registerOutParam(3, oracle::occi::OCCIINT);
      m_getTapeToReleaseStatement->registerOutParam(4, oracle::occi::OCCIINT);
    }
    m_getTapeToReleaseStatement->setDouble(1,(double)mountTransactionId); 
    m_getTapeToReleaseStatement->executeUpdate();
    tape.vid = m_getTapeToReleaseStatement->getString(2);
    tape.mode = (TapeMode) m_getTapeToReleaseStatement->getInt(3);
    tape.full = m_getTapeToReleaseStatement->getInt(4);
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
// cancelMigrationOrRecall
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::cancelMigrationOrRecall
(const int mode,
 const std::string &vid,
 const int errorCode,
 const std::string &errorMsg)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_cancelMigrationOrRecallStatement) {
      m_cancelMigrationOrRecallStatement =
        createStatement("BEGIN cancelMigrationOrRecall(:1,:2,:3,:4);END;");
      m_cancelMigrationOrRecallStatement->setAutoCommit(true);
    }
    m_cancelMigrationOrRecallStatement->setInt(1, mode);
    m_cancelMigrationOrRecallStatement->setString(2, vid);
    m_cancelMigrationOrRecallStatement->setInt(3, errorCode);
    m_cancelMigrationOrRecallStatement->setString(4, errorMsg);
    // execute the statement and see whether we found something
    m_cancelMigrationOrRecallStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in cancelMigrationOrRecall"
      << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// deleteMigrationMountWithBadTapePool
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::deleteMigrationMountWithBadTapePool(const u_signed64 migrationMountId) 
  throw (castor::exception::Exception){
  try {
    // Check whether the statements are ok
    if (0 == m_deleteMigrationMountWithBadTapePoolStatement) {
      m_deleteMigrationMountWithBadTapePoolStatement =
        createStatement("BEGIN tg_deleteMigrationMount(:1);END;");
    }
    m_deleteMigrationMountWithBadTapePoolStatement->setDouble(1,(double)migrationMountId);
    // execute the statement and see whether we found something
    m_deleteMigrationMountWithBadTapePoolStatement->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in deleteMigrationMountWithBadTapePool"
      << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// flagTapeFullForMigrationSession
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::flagTapeFullForMigrationSession(const u_signed64& tapeRequestId)throw (castor::exception::Exception){
  try {
    // Check whether the statements are ok

    /* The name of the SQL procedure is shorter, but not really ambiguous:
       only migration session can lead to a full tape (obviously) */
    if (!m_flagTapeFullForMigrationSession) {
      m_flagTapeFullForMigrationSession =
        createStatement("BEGIN tg_flagTapeFull(:1);END;");
      m_flagTapeFullForMigrationSession->setAutoCommit(true);
    }

    m_flagTapeFullForMigrationSession->setDouble(1,(double)tapeRequestId);
    // execute the statement
    m_flagTapeFullForMigrationSession->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in flagTapeFullForMigrationSession"
      << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// getMigrationMountVid
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getMigrationMountVid(const u_signed64&  mountTransactionId,
    std::string &vid, std::string &tapePool)
{
  try {
    if (!m_getMigrationMountVid) {
      m_getMigrationMountVid =
          createStatement("BEGIN tg_getMigrationMountVid(:1,:2,:3);END;");
      m_getMigrationMountVid->registerOutParam
      (2, oracle::occi::OCCISTRING, 2048 );
      m_getMigrationMountVid->registerOutParam
      (3, oracle::occi::OCCISTRING, 2048 );
    }
    m_getMigrationMountVid->setDouble(1,(double)mountTransactionId);
    m_getMigrationMountVid->executeUpdate();
    vid      = m_getMigrationMountVid->getString(2);
    tapePool = m_getMigrationMountVid->getString(3);
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getMigrationMountVid"
      << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// getBulkFilesToMigrate
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getBulkFilesToMigrate (
    const std::string & context,
    u_signed64 mountTransactionId, u_signed64 maxFiles, u_signed64 maxBytes,
    std::queue<castor::tape::tapegateway::FileToMigrateStruct>& filesToMigrate)
  throw (castor::exception::Exception){
  // container for result should be clean!
  if (!filesToMigrate.empty()) {
    castor::exception::Internal ex;
    ex.getMessage()
          << "Error in getBulkFilesToMigrate: filesToMigrate container not empty on call";
    throw ex;
  }
  try {
    if (!m_getBulkFilesToMigrate) {
      m_getBulkFilesToMigrate =
        createStatement("BEGIN tg_getBulkFilesToMigrate(:1,:2,:3,:4,:5); END;");
      m_getBulkFilesToMigrate->registerOutParam(5, oracle::occi::OCCICURSOR);
    }
    /* Call the SQL to retrieve files to migrate */
    m_getBulkFilesToMigrate->setString(1, context);
    m_getBulkFilesToMigrate->setNumber(2,(double)mountTransactionId);
    m_getBulkFilesToMigrate->setNumber(3,(double)maxFiles);
    m_getBulkFilesToMigrate->setNumber(4,(double)maxBytes);
    m_getBulkFilesToMigrate->executeUpdate();
    /* Convert the cursor's contents into a collection of FileToMigrateStruct, to be sent to
     * tape server.
     */
    castor::db::ora::SmartOcciResultSet rs (m_getBulkFilesToMigrate, m_getBulkFilesToMigrate->getCursor(5));
    // Find columns for the cursor
    resultSetIntrospector resIntros (rs.get());
    int fileTransIdIdx   = resIntros.findColumnIndex(   "FILETRANSACTIONID", oracle::occi::OCCI_SQLT_NUM);
    int nsFileIdIdx      = resIntros.findColumnIndex(              "FILEID", oracle::occi::OCCI_SQLT_NUM);
    int nsHostIdx        = resIntros.findColumnIndex(              "NSHOST", oracle::occi::OCCI_SQLT_CHR);
    int fSeqIdx          = resIntros.findColumnIndex(                "FSEQ", oracle::occi::OCCI_SQLT_NUM);
    int fileSizeIdx      = resIntros.findColumnIndex(            "FILESIZE", oracle::occi::OCCI_SQLT_NUM);
    int lastKnownNameIdx = resIntros.findColumnIndex(   "LASTKNOWNFILENAME", oracle::occi::OCCI_SQLT_CHR);
    int pathIdx          = resIntros.findColumnIndex(            "FILEPATH", oracle::occi::OCCI_SQLT_CHR);
    // Run through the cursor
    while (rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::tape::tapegateway::FileToMigrateStruct ftm;
      ftm.setFileTransactionId   (occiNumber(rs->getNumber(  fileTransIdIdx)));
      ftm.setFileid              (occiNumber(rs->getNumber(     nsFileIdIdx)));
      ftm.setNshost              (           rs->getString(       nsHostIdx));
      ftm.setFseq                (occiNumber(rs->getNumber(         fSeqIdx)));
      ftm.setFileSize            (occiNumber(rs->getNumber(     fileSizeIdx)));
      ftm.setLastKnownFilename   (           rs->getString(lastKnownNameIdx));
      ftm.setPath                (           rs->getString(         pathIdx));
      ftm.setLastModificationTime(0);
      ftm.setUmask               (022);
      ftm.setPositionCommandCode (TPPOSIT_FSEQ);
      filesToMigrate.push(ftm);
    }
    // Close result set and transaction.
    rs.close();
    cnvSvc()->commit();
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->rollback();
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getBulkFilesToMigrate"
      << std::endl << e.what();
    throw ex;
  } catch (...) {
    cnvSvc()->rollback();
    throw;
  }
}

//----------------------------------------------------------------------------
// getBulkFilesToRecall
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::getBulkFilesToRecall (
    const std::string & context,
    u_signed64 mountTransactionId, u_signed64 maxFiles, u_signed64 maxBytes,
    std::queue<castor::tape::tapegateway::FileToRecallStruct>& filesToRecall)
  throw (castor::exception::Exception){
  // container for result should be clean!
  if (!filesToRecall.empty()) {
    castor::exception::Internal ex;
    ex.getMessage()
          << "Error in getBulkFilesToRecall: filesToRecall container not empty on call";
    throw ex;
  }
  try {
    if (!m_getBulkFilesToRecall) {
      m_getBulkFilesToRecall =
        createStatement("BEGIN tg_getBulkFilesToRecall(:1,:2,:3,:4,:5);END;");
      m_getBulkFilesToRecall->registerOutParam(5, oracle::occi::OCCICURSOR);
    }
    /* Call the SQL to retrieve files to migrate */
    m_getBulkFilesToRecall->setString(1, context);
    m_getBulkFilesToRecall->setNumber(2,(double)mountTransactionId);
    m_getBulkFilesToRecall->setNumber(3,(double)maxFiles);
    m_getBulkFilesToRecall->setNumber(4,(double)maxBytes);
    m_getBulkFilesToRecall->executeUpdate();
    /* Convert the cursor's contents into a collection of FileToMigrateStruct, to be sent to
     * tape server.
     */
    castor::db::ora::SmartOcciResultSet rs (m_getBulkFilesToRecall, m_getBulkFilesToRecall->getCursor(5));
    // Find columns for the cursor
    resultSetIntrospector resIntros (rs.get());
    int fileTransIdIdx   = resIntros.findColumnIndex(   "FILETRANSACTIONID", oracle::occi::OCCI_SQLT_NUM);
    int nsFileIdIdx      = resIntros.findColumnIndex(              "FILEID", oracle::occi::OCCI_SQLT_NUM);
    int nsHostIdx        = resIntros.findColumnIndex(              "NSHOST", oracle::occi::OCCI_SQLT_CHR);
    int fSeqIdx          = resIntros.findColumnIndex(                "FSEQ", oracle::occi::OCCI_SQLT_NUM);
    int blockIdIdx       = resIntros.findColumnIndex(             "BLOCKID", oracle::occi::OCCI_SQLT_BIN);
    int pathIdx          = resIntros.findColumnIndex(            "FILEPATH", oracle::occi::OCCI_SQLT_CHR);
    // Run through the cursor
    while (rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::tape::tapegateway::FileToRecallStruct ftr;
      ftr.setFileTransactionId   (occiNumber(rs->getNumber(  fileTransIdIdx)));
      ftr.setFileid              (occiNumber(rs->getNumber(     nsFileIdIdx)));
      ftr.setNshost              (           rs->getString(       nsHostIdx));
      ftr.setFseq                (occiNumber(rs->getNumber(         fSeqIdx)));
      ftr.setPath                (           rs->getString(         pathIdx));
      ftr.setUmask               (022);
      ftr.setPositionCommandCode (TPPOSIT_FSEQ);
      // Extract the block ID. The query passes us the raw bytes, which we just
      // chop into the 4-bytes representation.
      oracle::occi::Bytes blockId(            rs->getBytes(      blockIdIdx));
      ftr.setBlockId0(blockId.byteAt(0));
      ftr.setBlockId1(blockId.byteAt(1));
      ftr.setBlockId2(blockId.byteAt(2));
      ftr.setBlockId3(blockId.byteAt(3));
      // And done
      filesToRecall.push(ftr);
    }
    // Close result set and transaction.
    rs.close();
    cnvSvc()->commit();
  } catch (oracle::occi::SQLException e) {
    cnvSvc()->rollback();
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getBulkFilesToRecall"
      << std::endl << e.what();
    throw ex;
  } catch (...) {
    cnvSvc()->rollback();
    throw;
  }
}

//----------------------------------------------------------------------------
// setBulkFileMigrationResult
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::setBulkFileMigrationResult (
    const std::string & context, u_signed64 mountTransactionId,
    std::vector<FileMigratedNotificationStruct *>& successes,
    std::vector<FileErrorReportStruct *>& failures)
throw (castor::exception::Exception){
  try {
    if (!m_setBulkFileMigrationResult) {
      m_setBulkFileMigrationResult =
        createStatement("BEGIN tg_setBulkFileMigrationResult(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12); END;");
      m_setBulkFileMigrationResult->setAutoCommit(true);
    }
    // Prepare the arays of data to be sent to PL/SQL
    std::vector<oracle::occi::Number> fileIds;
    std::vector<oracle::occi::Number> fileTransactionIds;
    std::vector<oracle::occi::Number> fSeqs;
    std::vector<std::string> blockIds;
    std::vector<std::string> checksumNames;
    std::vector<oracle::occi::Number> checksums;
    std::vector<oracle::occi::Number> compressedFileSizes;
    std::vector<oracle::occi::Number> fileSizes;
    std::vector<oracle::occi::Number> errorCodes;
    std::vector<std::string> errorMessages;

    // Fill arrays with data
    for (std::vector<FileMigratedNotificationStruct *>::iterator s = successes.begin();
         s < successes.end();
         s++) {
      // We cast the u_signed64 values for double first as the occi::Number type lacks a long long
      // case operator. This limits us to 52 bits, or ids, sizes and checksums below ~10^15.
      fileIds.push_back(occiNumber((*s)->fileid()));
      fileTransactionIds.push_back(occiNumber((*s)->fileTransactionId()));
      fSeqs.push_back(occiNumber((*s)->fseq()));
      std::stringstream blockIdHex;
      // Special treatment for blockId array. It is an array of 4 bytes, passed as an hex string.
      // This "interpretation" of the hex string happens to print out the block id as an intelligible
      // number (as in nsls, for example) because it is stored in big endian order in the SCSI
      // command buffer and always stored that way without interpretation outside of SCSI commands.
      blockIdHex << std::hex << std::noshowbase << std::uppercase << std::setfill('0')
                 << std::setw(2) << (int)(*s)->blockId0() << std::setw(2) << (int)(*s)->blockId1()
                 << std::setw(2) << (int)(*s)->blockId2() << std::setw(2) << (int)(*s)->blockId3();
      blockIds.push_back(blockIdHex.str());
      checksumNames.push_back((*s)->checksumName());
      checksums.push_back(occiNumber((*s)->checksum()));
      compressedFileSizes.push_back(occiNumber((*s)->compressedFileSize()));
      fileSizes.push_back(occiNumber((*s)->fileSize()));
      errorCodes.push_back(0);
      errorMessages.push_back(std::string(""));
    }
    for (std::vector<FileErrorReportStruct *>::iterator f = failures.begin();
        f < failures.end();
        f++) {
      // We cast the u_signed64 values to double first as the occi::Number type lacks a long long
      // cast operator. This limits us to 52 bits, or ids, sizes and checksums below ~10^15.
      fileIds.push_back(occiNumber((*f)->fileid()));
      fileTransactionIds.push_back(occiNumber((*f)->fileTransactionId()));
      fSeqs.push_back(occiNumber((*f)->fseq()));
      blockIds.push_back(std::string(""));
      checksumNames.push_back(std::string(""));
      checksums.push_back(0);
      compressedFileSizes.push_back(0);
      fileSizes.push_back(0);
      // The called PL/SQL relies on a 0 error code to differentiate successes
      // from failures. Getting an error code of 0 here is hence an issue.
      // We treat it as an internal error (it can originate from as far back as
      // rtcpd on the tape server.
      errorCodes.push_back((*f)->errorCode() ? (*f)->errorCode() : SEINTERNAL);
      errorMessages.push_back((*f)->errorMessage());
    }

    // Attach arrays to parameters
    m_setBulkFileMigrationResult->setString(1, context);
    m_setBulkFileMigrationResult->setNumber(2, occiNumber(mountTransactionId));
    oracle::occi::setVector(m_setBulkFileMigrationResult, 3, fileIds,       "numList");
    oracle::occi::setVector(m_setBulkFileMigrationResult, 4, fileTransactionIds,  "numList");
    oracle::occi::setVector(m_setBulkFileMigrationResult, 5, fSeqs,         "numList");
    oracle::occi::setVector(m_setBulkFileMigrationResult, 6, blockIds,      "STRLISTTABLE");
    oracle::occi::setVector(m_setBulkFileMigrationResult, 7, checksumNames, "STRLISTTABLE");
    oracle::occi::setVector(m_setBulkFileMigrationResult, 8, checksums,     "numList");
    oracle::occi::setVector(m_setBulkFileMigrationResult, 9, compressedFileSizes, "numList");
    oracle::occi::setVector(m_setBulkFileMigrationResult,10, fileSizes,     "numList");
    oracle::occi::setVector(m_setBulkFileMigrationResult,11, errorCodes,    "numList");
    oracle::occi::setVector(m_setBulkFileMigrationResult,12, errorMessages, "STRLISTTABLE");
    
    // DB update and get result.
    m_setBulkFileMigrationResult->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
          << "Error caught in setBulkFileMigrationResult"
          << std::endl << e.what();
    throw ex;
  }
}

//----------------------------------------------------------------------------
// setBulkFileRecallResult
//----------------------------------------------------------------------------
void castor::tape::tapegateway::ora::OraTapeGatewaySvc::setBulkFileRecallResult (
    const std::string & context, u_signed64 mountTransactionId,
    std::vector<FileRecalledNotificationStruct *>& successes,
    std::vector<FileErrorReportStruct *>& failures)
throw (castor::exception::Exception){
  try {
    if (!m_setBulkFileRecallResult) {
      m_setBulkFileRecallResult =
        createStatement("BEGIN tg_setBulkFileRecallResult(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11); END;");
      m_setBulkFileRecallResult->setAutoCommit(true);
    }
    // Prepare the arays of data to be sent to PL/SQL
    std::vector<oracle::occi::Number> fileIds;
    std::vector<oracle::occi::Number> fileTransactionIds;
    std::vector<std::string> pathes;
    std::vector<oracle::occi::Number> fSeqs;
    std::vector<std::string> checksumNames;
    std::vector<oracle::occi::Number> checksums;
    std::vector<oracle::occi::Number> fileSizes;
    std::vector<oracle::occi::Number> errorCodes;
    std::vector<std::string> errorMessages;

    // Fill arrays with data
    for (std::vector<FileRecalledNotificationStruct *>::iterator s = successes.begin();
         s < successes.end();
         s++) {
      // We cast the u_signed64 values for double first as the occi::Number type lacks a long long
      // case operator. This limits us to 52 bits, or ids, sizes and checksums below ~10^15.
      fileIds.push_back(occiNumber((*s)->fileid()));
      fileTransactionIds.push_back(occiNumber((*s)->fileTransactionId()));
      fSeqs.push_back(occiNumber((*s)->fseq()));
      pathes.push_back((*s)->path());
      checksumNames.push_back((*s)->checksumName());
      checksums.push_back(occiNumber((*s)->checksum()));
                          fileSizes.push_back(occiNumber((*s)->fileSize()));
      errorCodes.push_back(0);
      errorMessages.push_back(std::string(""));
    }
    for (std::vector<FileErrorReportStruct *>::iterator f = failures.begin();
        f < failures.end();
        f++) {
      // We cast the u_signed64 values to double first as the occi::Number type lacks a long long
      // cast operator. This limits us to 52 bits, or ids, sizes and checksums below ~10^15.
      fileIds.push_back(occiNumber((*f)->fileid()));
      fileTransactionIds.push_back(occiNumber((*f)->fileTransactionId()));
      fSeqs.push_back(occiNumber((*f)->fseq()));
      checksumNames.push_back(std::string(""));
      checksums.push_back(0);
      fileSizes.push_back(0);
      // The called PL/SQL relies on a 0 error code to differentiate successes
      // from failures. Getting an error code of 0 here is hence an issue.
      // We treat it as an internal error (it can originate from as far back as
      // rtcpd on the tape server.
      errorCodes.push_back((*f)->errorCode() ? (*f)->errorCode() : SEINTERNAL);
      errorMessages.push_back((*f)->errorMessage());
    }

    // Attach arrays to parameters
    m_setBulkFileRecallResult->setString(1, context);
    m_setBulkFileRecallResult->setNumber(2, occiNumber(mountTransactionId));
    oracle::occi::setVector(m_setBulkFileRecallResult,  3, fileIds,       "numList");
    oracle::occi::setVector(m_setBulkFileRecallResult,  4, fileTransactionIds,  "numList");
    oracle::occi::setVector(m_setBulkFileRecallResult,  5, pathes,        "STRLISTTABLE");
    oracle::occi::setVector(m_setBulkFileRecallResult,  6, fSeqs,         "numList");
    oracle::occi::setVector(m_setBulkFileRecallResult,  7, checksumNames, "STRLISTTABLE");
    oracle::occi::setVector(m_setBulkFileRecallResult,  8, checksums,     "numList");
    oracle::occi::setVector(m_setBulkFileRecallResult,  9, fileSizes,     "numList");
    oracle::occi::setVector(m_setBulkFileRecallResult, 10, errorCodes,    "numList");
    oracle::occi::setVector(m_setBulkFileRecallResult, 11, errorMessages, "STRLISTTABLE");

    // DB update and get result.
    m_setBulkFileRecallResult->executeUpdate();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
          << "Error caught in setBulkFileRecallResult"
          << std::endl << e.what();
    throw ex;
  }
}


//----------------------------------------------------------------------------
// commit
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::commit()
  throw (castor::exception::Exception)
{
  cnvSvc()->commit();
}

//----------------------------------------------------------------------------
// rollback
//----------------------------------------------------------------------------

void castor::tape::tapegateway::ora::OraTapeGatewaySvc::rollback()
  throw (castor::exception::Exception)
{
  cnvSvc()->rollback();
}
