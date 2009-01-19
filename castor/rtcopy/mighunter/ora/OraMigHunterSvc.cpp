/******************************************************************************
 *                      OraMigHunterSvc.cpp
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
 * @(#)OraPolicySvc.cpp,v 1.20 $Release$ 2007/04/13 11:58:53 sponcec3
 *
 * Implementation of the IMigHunterSvc for Oracle
 *
 * @author Giulia Taurelli
 *****************************************************************************/

// Include Files
#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/stager/Tape.hpp"
#include "castor/stager/Stream.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/TapeCopy.hpp"
#include "castor/stager/TapePool.hpp"
#include "castor/BaseAddress.hpp"

#include "castor/rtcopy/mighunter/ora/OraMigHunterSvc.hpp"


#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "occi.h"

#include "castor/infoPolicy/DbInfoMigrationPolicy.hpp"
#include "castor/infoPolicy/DbInfoStreamPolicy.hpp"
#include "castor/infoPolicy/DbInfoPolicy.hpp"

#include <iostream>
#include <vector>
#include <string>


// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::rtcopy::mighunter::ora::OraMigHunterSvc>* s_factoryOraMigHunterSvc =
  new castor::SvcFactory<castor::rtcopy::mighunter::ora::OraMigHunterSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// DATABASE FOR MIGHUNTER

/// SQL statement for inputForMigrationPolicy
const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_inputForMigrationPolicyStatementString = 
  "BEGIN inputForMigrationPolicy(:1,:2,:3,:4); END;";


/// SQL statement for createOrUpdateStream
const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_createOrUpdateStreamStatementString = 
  "BEGIN createOrUpdateStream(:1,:2,:3,:4,:5,:6,:7); END;";

/// SQL statement to attach the selected tapecopy

const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_attachTapeCopiesToStreamsStatementString = 
  "BEGIN attachTapeCopiesToStreams(:1,:2); END;";

/// SQL statement for inputForStreamPolicy
const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_inputForStreamPolicyStatementString = 
  "BEGIN inputForStreamPolicy(:1,:2,:3,:4,:5); END;";

/// SQL statement for startChosenStreams

const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_startChosenStreamsStatementString = 
  "BEGIN startChosenStreams(:1,:2); END;";

/// SQL statement for startChosenStreams

const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_stopChosenStreamsStatementString = 
  "BEGIN stopChosenStreams(:1); END;";

/// SQL resurrect candidates
const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_resurrectCandidatesStatementString = 
  "BEGIN  resurrectCandidates(:1);END;";

/// SQL invalidate tapecopies
const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_invalidateTapeCopiesStatementString = 
  "BEGIN  invalidateTapeCopies(:1);END;";

/// SQL select tapepoolnames
const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_selectTapePoolNamesStatementString =
  "SELECT TapePool.name, TapePool.id FROM TapePool,SvcClass2TapePool WHERE TapePool.id= SvcClass2TapePool.child AND SvcClass2TapePool.parent=:1";

/// MigHunter Db Clean up

const std::string castor::rtcopy::mighunter::ora::OraMigHunterSvc::s_migHunterCleanUpStatementString = 
  "BEGIN migHunterCleanUp(:1); END;";





// -----------------------------------------------------------------------
// OraMigHunterSvc
// -----------------------------------------------------------------------
castor::rtcopy::mighunter::ora::OraMigHunterSvc::OraMigHunterSvc(const std::string name) :
  IMigHunterSvc(),  
  OraCommonSvc(name), 
  m_inputForMigrationPolicyStatement(0),
  m_createOrUpdateStreamStatement(0),
  m_inputForStreamPolicyStatement(0),
  m_startChosenStreamsStatement(0),
  m_stopChosenStreamsStatement(0),
  m_resurrectCandidatesStatement(0),
  m_invalidateTapeCopiesStatement(0),
  m_selectTapePoolNamesStatement(0),
  m_attachTapeCopiesToStreamsStatement(0),
  m_migHunterCleanUpStatement(0){
}

// -----------------------------------------------------------------------
// ~OraMigHunterSvcy
// -----------------------------------------------------------------------
castor::rtcopy::mighunter::ora::OraMigHunterSvc::~OraMigHunterSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::rtcopy::mighunter::ora::OraMigHunterSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::rtcopy::mighunter::ora::OraMigHunterSvc::ID() {
  return castor::SVC_ORAMIGHUNTERSVC;
}

//-----------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::rtcopy::mighunter::ora::OraMigHunterSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
 
    if  (m_inputForMigrationPolicyStatement) deleteStatement(m_inputForMigrationPolicyStatement);
    if  (m_createOrUpdateStreamStatement) deleteStatement(m_createOrUpdateStreamStatement);
    if  (m_inputForStreamPolicyStatement) deleteStatement(m_inputForStreamPolicyStatement);
    if  (m_startChosenStreamsStatement) deleteStatement(m_startChosenStreamsStatement);
    if  (m_stopChosenStreamsStatement) deleteStatement(m_stopChosenStreamsStatement);
    if (m_resurrectCandidatesStatement) deleteStatement(m_resurrectCandidatesStatement);
    if (m_attachTapeCopiesToStreamsStatement) deleteStatement(m_attachTapeCopiesToStreamsStatement); 
    if ( m_selectTapePoolNamesStatement )deleteStatement(m_selectTapePoolNamesStatement);
    if ( m_migHunterCleanUpStatement )deleteStatement(m_migHunterCleanUpStatement);

  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0

  m_inputForMigrationPolicyStatement=0;
  m_createOrUpdateStreamStatement=0;
  m_inputForStreamPolicyStatement=0;
  m_startChosenStreamsStatement=0; 
  m_stopChosenStreamsStatement=0;
  m_resurrectCandidatesStatement=0;
  m_attachTapeCopiesToStreamsStatement=0;
  m_selectTapePoolNamesStatement=0;
  m_migHunterCleanUpStatement=0;
}



//--------------------------------------------------------------------------
// inputForMigrationPolicy
//--------------------------------------------------------------------------
std::vector<castor::infoPolicy::PolicyObj*> castor::rtcopy::mighunter::ora::OraMigHunterSvc::inputForMigrationPolicy(std::string  svcClassName, u_signed64* byteThres) throw (castor::exception::Exception) {
  
  std::vector<castor::infoPolicy::PolicyObj*> result;
  u_signed64 totalSize=0;
  try {
    // Check whether the statements are ok
    if (0 == m_inputForMigrationPolicyStatement) {
      m_inputForMigrationPolicyStatement  =
        createStatement(s_inputForMigrationPolicyStatementString);
      m_inputForMigrationPolicyStatement->registerOutParam
        (2, oracle::occi::OCCISTRING, 2048);
      m_inputForMigrationPolicyStatement->registerOutParam
        (3, oracle::occi::OCCIDOUBLE);
      m_inputForMigrationPolicyStatement->registerOutParam
	(4, oracle::occi::OCCICURSOR);
      m_inputForMigrationPolicyStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
    m_inputForMigrationPolicyStatement->setString(1,svcClassName);
    m_inputForMigrationPolicyStatement->executeUpdate();
   
    std::string policyName=m_inputForMigrationPolicyStatement->getString(2);  
    u_signed64 svcClassId=(u_signed64)m_inputForMigrationPolicyStatement->getDouble(3);  
    
    // let's get first the tapepools name and Id 

    if (0 == m_selectTapePoolNamesStatement) {
      m_selectTapePoolNamesStatement  =
        createStatement(s_selectTapePoolNamesStatementString);
    }
    m_selectTapePoolNamesStatement->setDouble(1,(double)svcClassId);
    oracle::occi::ResultSet *tapePoolResults = m_selectTapePoolNamesStatement->executeQuery();
    std::vector<std::string> listTapePoolName;
    std::vector<u_signed64>  listTapePoolId;
    while(tapePoolResults->next() != oracle::occi::ResultSet::END_OF_FETCH ) {
      std::string tapePoolName = tapePoolResults->getString(1);
      u_signed64  tapePoolId = (u_signed64) tapePoolResults->getDouble(2);
      listTapePoolName.push_back(tapePoolName);
      listTapePoolId.push_back(tapePoolId);
    }
    
    // Now let's analyse the first query result with the tapepool information

    oracle::occi::ResultSet *rs = m_inputForMigrationPolicyStatement->getCursor(4);

    // Run through the cursor

    while(rs->next() == oracle::occi::ResultSet::DATA_AVAILABLE) {
      u_signed64 tapecopyid=(u_signed64)rs->getDouble(1);
      u_signed64 copynb = (u_signed64)rs->getDouble(2);
      std::string castorfilename = rs->getString(3);
      std::string nshost = rs->getString(4);
      u_signed64 fileid =(u_signed64)rs->getDouble(5);
      totalSize += (u_signed64)rs->getDouble(6);

      // moltiplicate the object by different number of tapepool
      std::vector<std::string>::iterator tapePoolNameToAdd=listTapePoolName.begin();
      std::vector<u_signed64>::iterator tapePoolIdToAdd=listTapePoolId.begin();
      while( tapePoolNameToAdd != listTapePoolName.end()){
        castor::infoPolicy::DbInfoMigrationPolicy* item = new castor::infoPolicy::DbInfoMigrationPolicy();

	item->setTapeCopyId(tapecopyid);
        item->setCopyNb(copynb);
        item->setCastorFileName(castorfilename);
        item->setNsHost(nshost);
        item->setFileId(fileid);
        item->setTapePoolName(*tapePoolNameToAdd);
        item->setTapePoolId(*tapePoolIdToAdd);

	castor::infoPolicy::PolicyObj* resultItem= new castor::infoPolicy::PolicyObj();
        resultItem->setSvcClassName(svcClassName);
        resultItem->setPolicyName(policyName);
	resultItem->addDbInfoPolicy(item);
	result.push_back(resultItem);

        tapePoolNameToAdd++;
	tapePoolIdToAdd++;
      }
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in inputForMigrationPolicy."
      << std::endl << e.what();
    throw ex;
  }
  if (byteThres) *byteThres=totalSize;
  return result;

}

//--------------------------------------------------------------------------
// createOrUpdateStream
//--------------------------------------------------------------------------
int castor::rtcopy::mighunter::ora::OraMigHunterSvc::createOrUpdateStream(std::string svcClassName, u_signed64 initialSizeToTransfer, u_signed64 volumeThreashold, u_signed64 initialSizeCeiling,bool doClone, std::vector<castor::infoPolicy::PolicyObj*> tapeCopiesInfo) throw (castor::exception::Exception){

  unsigned char (*buffer)[21] = 0;
  ub2 *lens =0;
  
  try {
    // Check whether the statements are ok
    if (0 == m_createOrUpdateStreamStatement) {
      m_createOrUpdateStreamStatement  =
        createStatement(s_createOrUpdateStreamStatementString);
      m_createOrUpdateStreamStatement->registerOutParam
        (7, oracle::occi::OCCIINT);
      m_createOrUpdateStreamStatement->setAutoCommit(true);
    }
    // set input param

    m_createOrUpdateStreamStatement->setString(1,svcClassName);
    m_createOrUpdateStreamStatement->setDouble(2,(double)initialSizeToTransfer);
    m_createOrUpdateStreamStatement->setDouble(3, (double)volumeThreashold);
    m_createOrUpdateStreamStatement->setDouble(4,(double)initialSizeCeiling);
    int doCloneInt=doClone?1:0;
    m_createOrUpdateStreamStatement->setInt(5,doCloneInt);
    m_createOrUpdateStreamStatement->setInt(6, (int)tapeCopiesInfo.size());
    m_createOrUpdateStreamStatement->executeUpdate();
    int retcode= m_createOrUpdateStreamStatement->getInt(7);
    
    // retcode used to have logged the proper error message 
    // retcode=-1 RTCPCLD_MSG_NOTPPOOLS
    // retcode=-2 RTCPCLD_MSG_DATALIMIT
    // retcode=-3 Fatal Error 
    
    return retcode;


  } catch (oracle::occi::SQLException e) {

    // free allocated memory
    if (lens) {free(lens);lens=0;}
    if (buffer){free(buffer);buffer=0;}

    handleException(e);
    return -1;
  }
}

//-------------------------------------------------------------------------
// attach tapecopies to streams
//------------------------------------------------------------------------
void castor::rtcopy::mighunter::ora::OraMigHunterSvc::attachTapeCopiesToStreams(std::vector<castor::infoPolicy::PolicyObj*> outputFromMigrationPolicy) throw (castor::exception::Exception){
  unsigned char (*bufferTapeCopyId)[21] = 0;
  unsigned char (*bufferTapePoolId)[21] = 0;
  ub2 *lens1=NULL;
  ub2 *lens2=NULL;

  if (outputFromMigrationPolicy.size() == 0){
    // no tapecopy to attach
    return;
  }
  try {
    // Check whether the statements are ok
    if (0 == m_attachTapeCopiesToStreamsStatement) {
      m_attachTapeCopiesToStreamsStatement  =
        createStatement(s_attachTapeCopiesToStreamsStatementString);
      m_attachTapeCopiesToStreamsStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
     
    castor::infoPolicy::DbInfoMigrationPolicy* realInfo;

    // two data buffer input
   
    unsigned int nb = outputFromMigrationPolicy.size();

    bufferTapeCopyId=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    bufferTapePoolId=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    lens1=(ub2 *)malloc (sizeof(ub2)*nb);
    lens2=(ub2 *)malloc (sizeof(ub2)*nb);

    for (unsigned int i = 0; i < nb; i++) {
      realInfo=dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy*>(outputFromMigrationPolicy[i]->dbInfoPolicy()[0]);
      oracle::occi::Number n1 = (double)(realInfo->tapeCopyId());
      oracle::occi::Bytes b1 = n1.toBytes();
      b1.getBytes(bufferTapeCopyId[i],b1.length());
      lens1[i] = b1.length();

      oracle::occi::Number n2 = (double)(realInfo->tapePoolId());
      oracle::occi::Bytes b2 = n2.toBytes();
      b2.getBytes(bufferTapePoolId[i],b2.length());
      lens2[i] = b2.length();
    }
    ub4 unused = nb;
    m_attachTapeCopiesToStreamsStatement->setDataBufferArray
      (1, bufferTapeCopyId, oracle::occi::OCCI_SQLT_NUM,nb, &unused, 21, lens1);
    m_attachTapeCopiesToStreamsStatement->setDataBufferArray
      (2, bufferTapePoolId, oracle::occi::OCCI_SQLT_NUM,nb, &unused, 21, lens2);
    
    m_attachTapeCopiesToStreamsStatement->executeUpdate();

    //free allocated memory needed because of oracle unfriendly interface
    if (lens1) {free(lens1);lens1=0;}
    if (bufferTapeCopyId){free(bufferTapeCopyId);bufferTapeCopyId=0;}
    if (lens2)  {free(lens2);lens2=0;}
    if (bufferTapePoolId){free(bufferTapePoolId);bufferTapePoolId=0;}
    

  } catch (oracle::occi::SQLException e) {

    //free allocated memory needed because of oracle unfriendly interface
    if (lens1) {free(lens1);lens1=0;}
    if (bufferTapeCopyId){free(bufferTapeCopyId);bufferTapeCopyId=0;}
    if (lens2)  {free(lens2);lens2=0;}
    if (bufferTapePoolId){free(bufferTapePoolId);bufferTapePoolId=0;}
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in attach tapecopies to streams"
      << std::endl << e.what();
    throw ex;
  }
}


//--------------------------------------------------------------------------
// inputForStreamPolicy
//--------------------------------------------------------------------------

std::vector<castor::infoPolicy::PolicyObj*> castor::rtcopy::mighunter::ora::OraMigHunterSvc::inputForStreamPolicy(std::string svcClassName) throw (castor::exception::Exception){
  std::vector<castor::infoPolicy::PolicyObj*> result;

  try {
    // Check whether the statements are ok
    if (0 == m_inputForStreamPolicyStatement) {
      m_inputForStreamPolicyStatement  =
        createStatement(s_inputForStreamPolicyStatementString);       
      m_inputForStreamPolicyStatement->registerOutParam
        (2, oracle::occi::OCCISTRING,2048);
      m_inputForStreamPolicyStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_inputForStreamPolicyStatement->registerOutParam
        (4, oracle::occi::OCCIINT);
      m_inputForStreamPolicyStatement->registerOutParam
        (5, oracle::occi::OCCICURSOR);

      m_inputForStreamPolicyStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
     

    m_inputForStreamPolicyStatement->setString(1, svcClassName);
    
    m_inputForStreamPolicyStatement->executeUpdate();
    
      
    std::string policyName= m_inputForStreamPolicyStatement->getString(2);
    u_signed64 runningStream= m_inputForStreamPolicyStatement->getInt(3);
    u_signed64 nbDrives= m_inputForStreamPolicyStatement->getInt(4);
    
    oracle::occi::ResultSet *rs =
      m_inputForStreamPolicyStatement->getCursor(5);
    // Run through the cursor
    
    oracle::occi::ResultSet::Status status = rs->next();
    while(status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::infoPolicy::DbInfoStreamPolicy* item = new castor::infoPolicy::DbInfoStreamPolicy();
      castor::infoPolicy::PolicyObj* resultItem = new castor::infoPolicy::PolicyObj();
      resultItem->setSvcClassName(svcClassName);
      resultItem->setPolicyName(policyName);
      item->setMaxNumStreams(nbDrives);
      item->setStreamId((u_signed64)rs->getDouble(1));
      item->setNumFiles((u_signed64)rs->getDouble(2));
      item->setNumBytes((u_signed64)rs->getDouble(3));
      item->setRunningStream(runningStream);
      resultItem->addDbInfoPolicy(item); 
      result.push_back(resultItem);
      status = rs->next();
    } 

  } catch (oracle::occi::SQLException e) {

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in inputForStreamPolicy"
      << std::endl << e.what();
    throw ex;
  }
  return result;

}


//--------------------------------------------------------------------------
// startChosenStreams
//--------------------------------------------------------------------------

void  castor::rtcopy::mighunter::ora::OraMigHunterSvc::startChosenStreams(std::vector<castor::infoPolicy::PolicyObj*> outputFromStreamPolicy,u_signed64 initialSize) throw (castor::exception::Exception){
  unsigned char (*buffer)[21] = 0;
  ub2 *lens =0;
  try {
    // Check whether the statements are ok
    if (0 ==  m_startChosenStreamsStatement) {
      m_startChosenStreamsStatement  =
        createStatement(s_startChosenStreamsStatementString); 
      m_startChosenStreamsStatement->setAutoCommit(true);
    }
    unsigned int  nb = outputFromStreamPolicy.size();

    if (nb == 0 ) return;

    lens = (ub2 *)malloc (sizeof(ub2)*nb);
    buffer=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    lens = (ub2 *)malloc (sizeof(ub2)*nb);

    for (unsigned int i = 0; i < nb; i++) {
      oracle::occi::Number n = (double)(dynamic_cast<castor::infoPolicy::DbInfoStreamPolicy*>(outputFromStreamPolicy.at(i)->dbInfoPolicy()[0])->streamId());
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(buffer[i],b.length());
      lens[i] = b.length();
    }
    ub4 unused = nb;
    m_startChosenStreamsStatement->setDataBufferArray
      (1, buffer, oracle::occi::OCCI_SQLT_NUM,nb, &unused, 21, lens);
    m_startChosenStreamsStatement->setDouble(2,(double)initialSize);
   
    nb= m_startChosenStreamsStatement->executeUpdate();

    //free allocated memory needed because of oracle unfriendly interface
    if (lens) {free(lens);lens=0;}
    if (buffer){free(buffer);buffer=0;}

  } catch (oracle::occi::SQLException e) {
    if (lens) {free(lens);lens=0;}
    if (buffer){free(buffer);buffer=0;}
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in startChosensStreams."
      << std::endl << e.what();
    throw ex;
  }
        
}

//--------------------------------------------------------------------------
// stopChosenStreams
//--------------------------------------------------------------------------

void  castor::rtcopy::mighunter::ora::OraMigHunterSvc::stopChosenStreams(std::vector<castor::infoPolicy::PolicyObj*> outputFromStreamPolicy) throw (castor::exception::Exception){
  unsigned char (*buffer)[21] = 0;
  ub2 *lens =0;
  try {
    // Check whether the statements are ok
    if (0 ==  m_stopChosenStreamsStatement) {
      m_stopChosenStreamsStatement  =
        createStatement(s_stopChosenStreamsStatementString); 
      m_stopChosenStreamsStatement->setAutoCommit(true);
    }
    unsigned int  nb = outputFromStreamPolicy.size();

    if (nb == 0 ) return;

    lens = (ub2 *)malloc (sizeof(ub2)*nb);
    buffer=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
    lens = (ub2 *)malloc (sizeof(ub2)*nb);

    for (unsigned int i = 0; i < nb; i++) {
      oracle::occi::Number n = (double)(dynamic_cast<castor::infoPolicy::DbInfoStreamPolicy*>(outputFromStreamPolicy.at(i)->dbInfoPolicy()[0])->streamId());
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(buffer[i],b.length());
      lens[i] = b.length();
    }
    ub4 unused = nb;
    m_stopChosenStreamsStatement->setDataBufferArray
      (1, buffer, oracle::occi::OCCI_SQLT_NUM,nb, &unused, 21, lens);
   
    nb= m_stopChosenStreamsStatement->executeUpdate();

    //free allocated memory needed because of oracle unfriendly interface
    if (lens) {free(lens);lens=0;}
    if (buffer){free(buffer);buffer=0;}

  } catch (oracle::occi::SQLException e) {
    if (lens) {free(lens);lens=0;}
    if (buffer){free(buffer);buffer=0;}
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in stopChosensStreams."
      << std::endl << e.what();
    throw ex;
  }
        
}




//---------------------------------------------------------------------
//    resurrectTapeCopies 
//---------------------------------------------------------------------


void  castor::rtcopy::mighunter::ora::OraMigHunterSvc::resurrectTapeCopies(std::vector<castor::infoPolicy::PolicyObj*> tapeCopiesInfo) throw (castor::exception::Exception) {
  unsigned char (*buffer)[21] = 0;
  ub2 *lens =0;
  try{
    if (0 == m_resurrectCandidatesStatement) {
      m_resurrectCandidatesStatement  =
	createStatement(s_resurrectCandidatesStatementString);
      m_resurrectCandidatesStatement->setAutoCommit(true);

    }
     
    //  DataBuffer needed operations

    unsigned int nb = tapeCopiesInfo.size();

    if (nb!=0){
      buffer=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
      lens=(ub2 *)malloc (sizeof(ub2)*nb);
      for (unsigned int i = 0; i < nb; i++) {
	oracle::occi::Number n = (double)(dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy*>(tapeCopiesInfo.at(i)->dbInfoPolicy()[0])->tapeCopyId());
	oracle::occi::Bytes b = n.toBytes();
	b.getBytes(buffer[i],b.length());
	lens[i] = b.length();
      }
      ub4 unused = nb;
      m_resurrectCandidatesStatement->setDataBufferArray(1, buffer, oracle::occi::OCCI_SQLT_NUM,nb, &unused, 21, lens);
      m_resurrectCandidatesStatement->executeUpdate();
    }
  } catch(oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in resurrectTapeCopies."
      << std::endl << e.what();
    throw ex;
  }

}

//---------------------------------------------------------------------
//    invalidateTapeCopies 
//---------------------------------------------------------------------


void   castor::rtcopy::mighunter::ora::OraMigHunterSvc::invalidateTapeCopies(std::vector<castor::infoPolicy::PolicyObj*> tapeCopiesInfo) throw (castor::exception::Exception) {
  unsigned char (*buffer)[21] = 0;
  ub2 *lens =0;
  try{
    if (0 == m_invalidateTapeCopiesStatement) {
      m_invalidateTapeCopiesStatement  =
	createStatement(s_invalidateTapeCopiesStatementString);
      m_invalidateTapeCopiesStatement->setAutoCommit(true);

    }
     
    //  DataBuffer needed operations

    unsigned int nb = tapeCopiesInfo.size();

    if (nb!=0){
      buffer=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
      lens=(ub2 *)malloc (sizeof(ub2)*nb);
      for (unsigned int i = 0; i < nb; i++) {
	oracle::occi::Number n = (double)(dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy*>(tapeCopiesInfo.at(i)->dbInfoPolicy()[0])->tapeCopyId());
	oracle::occi::Bytes b = n.toBytes();
	b.getBytes(buffer[i],b.length());
	lens[i] = b.length();
      }
      ub4 unused = nb;
      m_invalidateTapeCopiesStatement->setDataBufferArray(1, buffer, oracle::occi::OCCI_SQLT_NUM,nb, &unused, 21, lens);
      m_invalidateTapeCopiesStatement->executeUpdate();
    }
  } catch(oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in invalidate tape copies."
      << std::endl << e.what();
    throw ex;
  }

}

//---------------------------------------------------------------------
//    migHunterCleanUp
//---------------------------------------------------------------------

void   castor::rtcopy::mighunter::ora::OraMigHunterSvc::migHunterCleanUp(std::string svcClassName) throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_migHunterCleanUpStatement) {
      m_migHunterCleanUpStatement  =
        createStatement(s_migHunterCleanUpStatementString);       

      m_migHunterCleanUpStatement->setAutoCommit(true);
    }
    // execute the statement and see whether we found something
     

    m_migHunterCleanUpStatement->setString(1, svcClassName);
    
    m_migHunterCleanUpStatement->executeUpdate();

  } catch (oracle::occi::SQLException e) {

    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in migHunterCleanUp"
      << std::endl << e.what();
    throw ex;
  }
}

