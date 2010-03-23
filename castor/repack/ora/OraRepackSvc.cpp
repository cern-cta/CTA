/******************************************************************************
 *                      OraRepackSvc.cpp
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
 * Implementation of the IRepackSvc for Oracle
 *
 * @author Giulia Taurelli
 *****************************************************************************/



#include "castor/repack/ora/OraRepackSvc.hpp"
#include "castor/Services.hpp"
#include "castor/repack/RepackUtility.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/OutOfMemory.hpp"
#include "castor/repack/RepackResponse.hpp"

#include <iostream>
#include <string>
#include "occi.h"

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::repack::ora::OraRepackSvc>* s_factoryOraRepackSvc =
  new castor::SvcFactory<castor::repack::ora::OraRepackSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL to store a new request
const std::string castor::repack::ora::OraRepackSvc::s_storeRequestStatementString =
  "BEGIN storeRequest(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15);END;";

/// SQL to store all the segment for a request
const std::string castor::repack::ora::OraRepackSvc::s_updateSubRequestSegmentsStatementString=
  "BEGIN updateSubRequestSegments(:1,:2,:3,:4);END;";

/// SQL optimized fillObj
const std::string castor::repack::ora::OraRepackSvc::s_getSegmentsForSubRequestStatementString =
  "BEGIN getSegmentsForSubRequest(:1,:2);END;";

/// SQL to get RepackSubRequest with a certain vid
const std::string castor::repack::ora::OraRepackSvc::s_getSubRequestByVidStatementString =
  "BEGIN getSubRequestByVid(:1,:2);END;";

/// SQL to get RepackSubRequests attached to a request  by status
const std::string castor::repack::ora::OraRepackSvc::s_getSubRequestsByStatusStatementString =
  "BEGIN getSubRequestsByStatus(:1,:2);END;"; 

/// SQL to get all  RepackSubRequests (which are not archived)
const std::string castor::repack::ora::OraRepackSvc::s_getAllSubRequestsStatementString =
  "BEGIN getAllSubRequests(:1); END;"; 

/// SQL to validate a RepackSubrequest to submit to the stager
const std::string castor::repack::ora::OraRepackSvc::s_validateRepackSubRequestStatementString =
  "BEGIN validateRepackSubRequest(:1,:2, :3, :4); END;"; 

/// SQL to resurrect a RepackSubrequest to submit to the stager
const std::string castor::repack::ora::OraRepackSvc::s_resurrectTapesOnHoldStatementString =
  "BEGIN resurrectTapesOnHold(:1, :2); END;"; 

/// SQL to restart a failed RepackSubRequest
const std::string castor::repack::ora::OraRepackSvc::s_restartSubRequestStatementString="BEGIN restartSubRequest(:1); END;";

/// SQL to changeSubRequestsStatus
const std::string castor::repack::ora::OraRepackSvc::s_changeSubRequestsStatusStatementString="BEGIN changeSubRequestsStatus(:1,:2,:3); END;";

/// SQL to changeAllSubRequestsStatus
const std::string castor::repack::ora::OraRepackSvc::s_changeAllSubRequestsStatusStatementString="BEGIN changeAllSubRequestsStatus(:1,:2); END;";

/// SQL for getting lattest information of a tapy copy to do a repack undo
const std::string castor::repack::ora::OraRepackSvc::s_selectLastSegmentsSituationStatementString= 
"BEGIN selectLastSegmentsSituation(:1, :2, :3); END;";


//------------------------------------------------------------------------------
// OraRepackSvc
//------------------------------------------------------------------------------
castor::repack::ora::OraRepackSvc::OraRepackSvc(const std::string name) :
  IRepackSvc(),  
  OraCommonSvc(name),
  m_storeRequestStatement(0),
  m_updateSubRequestSegmentsStatement(0),
  m_getSegmentsForSubRequestStatement(0),
  m_getSubRequestByVidStatement(0),
  m_getSubRequestsByStatusStatement(0),
  m_getAllSubRequestsStatement(0),
  m_validateRepackSubRequestStatement(0),
  m_resurrectTapesOnHoldStatement(0),	
  m_restartSubRequestStatement(0),
  m_changeSubRequestsStatusStatement(0),
  m_changeAllSubRequestsStatusStatement(0),
  m_selectLastSegmentsSituationStatement(0){
}

//------------------------------------------------------------------------------
// ~OraRepackSvc
//------------------------------------------------------------------------------
castor::repack::ora::OraRepackSvc::~OraRepackSvc() throw() {
  reset();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
unsigned int castor::repack::ora::OraRepackSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
unsigned int castor::repack::ora::OraRepackSvc::ID() {
  return castor::SVC_ORAREPACKSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::repack::ora::OraRepackSvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it

  OraCommonSvc::reset();

  try {
    if (m_storeRequestStatement)  deleteStatement(m_storeRequestStatement);
    if (m_updateSubRequestSegmentsStatement)  deleteStatement(m_updateSubRequestSegmentsStatement);
    if (m_getSegmentsForSubRequestStatement) deleteStatement(m_getSegmentsForSubRequestStatement);
   if (m_getSubRequestByVidStatement) deleteStatement(m_getSubRequestByVidStatement);
    if (m_getSubRequestsByStatusStatement) deleteStatement(m_getSubRequestsByStatusStatement);
    if (m_getAllSubRequestsStatement) deleteStatement(m_getAllSubRequestsStatement);
    if (m_validateRepackSubRequestStatement) deleteStatement(m_validateRepackSubRequestStatement);
    if (m_resurrectTapesOnHoldStatement)deleteStatement(m_resurrectTapesOnHoldStatement); 	
    if (m_restartSubRequestStatement) deleteStatement(m_restartSubRequestStatement);
    if (m_changeSubRequestsStatusStatement) deleteStatement(m_changeSubRequestsStatusStatement);
    if (m_changeAllSubRequestsStatusStatement)deleteStatement(m_changeAllSubRequestsStatusStatement);
    if (m_selectLastSegmentsSituationStatement)deleteStatement(m_selectLastSegmentsSituationStatement);     
  } catch (castor::exception::SQLError e) {};
  // Now reset all pointers to 0
  m_storeRequestStatement=0; 
  m_updateSubRequestSegmentsStatement=0;
  m_getSegmentsForSubRequestStatement=0;
  m_getSubRequestByVidStatement=0; 
  m_getSubRequestsByStatusStatement=0;
  m_getAllSubRequestsStatement=0;
  m_validateRepackSubRequestStatement=0;
  m_resurrectTapesOnHoldStatement=0; 	
  m_restartSubRequestStatement=0;
  m_changeSubRequestsStatusStatement=0;
  m_changeAllSubRequestsStatusStatement=0;
  m_selectLastSegmentsSituationStatement=0; 
}


//------------------------------------------------------------------------------
// private: endTransation
//------------------------------------------------------------------------------
void castor::repack::ora::OraRepackSvc::endTransation() throw (castor::exception::Exception){
    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV); 
    svcs()->commit(&ad);
}


//------------------------------------------------------------------------------
// storeRequest 
//------------------------------------------------------------------------------
castor::repack::RepackAck* castor::repack::ora::OraRepackSvc::storeRequest(castor::repack::RepackRequest* rreq)
  throw (castor::exception::Exception)
{
  RepackAck* ack=NULL;
  oracle::occi::ResultSet *rset =NULL;
  ub2 *lens = NULL;
  char *buffer = NULL;

  if (rreq != NULL) {
    try {

      if ( m_storeRequestStatement == NULL ) {
	m_storeRequestStatement = 
	  createStatement(s_storeRequestStatementString);
        m_storeRequestStatement->registerOutParam
	  (15, oracle::occi::OCCICURSOR);
     
      }
     
      // Give all the request information
     
      m_storeRequestStatement->setString(1,rreq->machine());
      m_storeRequestStatement->setString(2,rreq->userName());
      m_storeRequestStatement->setDouble(3,(double)rreq->creationTime());
      m_storeRequestStatement->setString(4,rreq->pool());
      m_storeRequestStatement->setDouble(5,(double)rreq->pid());
      m_storeRequestStatement->setString(6,rreq->svcclass());
      m_storeRequestStatement->setInt(7,rreq->command());
      m_storeRequestStatement->setString(8,rreq->stager());
      m_storeRequestStatement->setDouble(9,rreq->userId());
      m_storeRequestStatement->setDouble(10,(double)rreq->groupId());
      m_storeRequestStatement->setDouble(11,(double)rreq->retryMax());
      m_storeRequestStatement->setInt(12,(int)rreq->reclaim());
      m_storeRequestStatement->setString(13,rreq->finalPool());

      // DataBuffer with all the vid (one for each subrequest)

      // loop 

      std::vector<std::string> listOfVids;
      
      std::vector<RepackSubRequest*> listOfSubRequest=rreq->repacksubrequest();
      std::vector<RepackSubRequest*>::iterator subReq=listOfSubRequest.begin();
      std::map<std::string,int> hitMissMap;
      unsigned int maxLen=0;
      unsigned int numTapes=0;

      while (subReq != listOfSubRequest.end()) {
	if (*subReq) {
	  listOfVids.push_back((*subReq)->vid());
          hitMissMap[(*subReq)->vid()]=0;
	  maxLen=maxLen>((*subReq)->vid()).length()?maxLen:((*subReq)->vid()).length();
	  numTapes++;
	}
	subReq++;
      }
      
      if (numTapes==0 || maxLen==0){

	ack=new RepackAck();
	RepackResponse* resp=new RepackResponse();
	resp->setErrorCode(-1);
	resp->setErrorMessage("No Tape given");
	ack->addRepackresponse(resp);
	return ack;
	
      }

      unsigned int bufferCellSize = maxLen * sizeof(char);

      lens = (ub2*) malloc(maxLen * sizeof(ub2));
      buffer =
	(char*) malloc(numTapes * bufferCellSize);

      
      if ( buffer == 0 || lens == 0   ) {
	if (buffer != 0) free(buffer);
	if (lens != 0) free(lens);
	castor::exception::OutOfMemory e; 
	throw e;
      }

      // Fill in the structure

      std::vector<std::string>::iterator vid= listOfVids.begin();
      int i=0;

      while (vid != listOfVids.end()){
	lens[i]=(*vid).length();
        strncpy(buffer+(bufferCellSize*i),(*vid).c_str(),lens[i]);
	vid++;
	i++;
      }

      ub4 len=numTapes;
      m_storeRequestStatement->setDataBufferArray
	(14, buffer, oracle::occi::OCCI_SQLT_CHR,
	 len, &len, maxLen, lens);

      m_storeRequestStatement->executeUpdate();

      rset=m_storeRequestStatement->getCursor(15);

      // Run through the cursor
      
      ack=new RepackAck(); // to answer back
        
      oracle::occi::ResultSet::Status status = rset->next();
      
      while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
	RepackResponse* resp = new castor::repack::RepackResponse();
	RepackSubRequest* sub=new RepackSubRequest();
	sub->setVid(rset->getString(1));
	sub->setRepackrequest(NULL);

	resp->setErrorCode(0); // if it is here it has been inserted

	resp->setRepacksubrequest(sub);
	ack->addRepackresponse(resp);

        hitMissMap[sub->vid()]=1; // to optimize the research of failure
	status = rset->next();
      }
      
      if (ack->repackresponse().size() !=  listOfVids.size()) {
	// add miss response	
        std::vector<std::string>::iterator elem=listOfVids.begin();
	while (elem !=listOfVids.end()){
	  if (hitMissMap[*elem] == 0){
	    // failure the request exists
	    RepackResponse* resp = new RepackResponse();
	    RepackSubRequest* sub= new RepackSubRequest(); 
	    sub->setVid(*elem);
	    sub->setRepackrequest(NULL);
	    resp->setRepacksubrequest(sub);
	    resp->setErrorCode(-1);
	    resp->setErrorMessage("An old repack process is still active");
            ack->addRepackresponse(resp);
	  }
	  elem++;
	}

      }
      endTransation();
      m_storeRequestStatement->closeResultSet(rset);

    } catch (oracle::occi::SQLException e) {
      // clean up
      if (buffer) free(buffer);
      buffer=NULL;
      if (lens) free(lens);
      lens=NULL;
      if (ack) freeRepackObj(ack);
      // forward the exception
      
      handleException(e);
      castor::exception::Internal ex;
      ex.getMessage()
	<< "Error caught in storeRequest"
	<< std::endl << e.what();
      throw ex;

    } catch (castor::exception::Exception ex){
      // clean up
      if (buffer) free(buffer);
      buffer=NULL;
      if (lens) free(lens);
      lens=NULL;
      if (ack) freeRepackObj(ack);
      // forward the exception
      throw ex;
    }
    
  }  

  //clean up

  if (buffer) free(buffer);
  buffer=NULL;
  if (lens) free(lens);
  lens=NULL;
  return ack;
}


//------------------------------------------------------------------------------
// updateSubRequest
//------------------------------------------------------------------------------
void castor::repack::ora::OraRepackSvc::updateSubRequest(
							 castor::repack::RepackSubRequest* obj) 
  throw (castor::exception::Exception)
{
 
    /// stores it into the database

    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    
    svcs()->updateRep(&ad, obj, true);

}


//------------------------------------------------------------------------------
// updateSubRequestSegments
//------------------------------------------------------------------------------
void castor::repack::ora::OraRepackSvc::updateSubRequestSegments(castor::repack::RepackSubRequest* obj,std::vector<RepackSegment*> listToUpdate) throw (castor::exception::Exception)
{

  if (listToUpdate.empty()) { 
    updateSubRequest(obj);
    return;
  }

  ub4 nb=listToUpdate.size();

  unsigned char (*bufferFileId)[21]=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
  unsigned char (*bufferErrorCode)[21]=(unsigned char(*)[21]) calloc((nb) * 21, sizeof(unsigned char));
  char * bufferErrorMessage=NULL;

  ub2 *lensFileId=(ub2 *)malloc (sizeof(ub2)*nb);

  ub2 *lensErrorCode=(ub2 *)malloc (sizeof(ub2)*nb);
  ub2 *lensErrorMessage=NULL;

  if ( bufferFileId ==0 || lensFileId == 0 || bufferErrorCode == 0 || lensErrorCode == 0  ) {
       if (bufferFileId != 0) free(bufferFileId);
       if (bufferErrorCode != 0) free(bufferErrorCode);
       castor::exception::OutOfMemory e; 
       throw e;
    }



  try {
    /// stores it into the database

    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);    
    svcs()->updateRep(&ad, obj, false);
    
    if ( m_updateSubRequestSegmentsStatement == NULL ) {
      m_updateSubRequestSegmentsStatement = 
	createStatement(s_updateSubRequestSegmentsStatementString);
    }

    // DataBuffer
    std::vector<RepackSegment*>::iterator elem=listToUpdate.begin();
    unsigned int maxLen=1;

    while (elem != listToUpdate.end()) {
      // errorMessageLenCheck
      if ((*elem)->errorCode()!=0)
	maxLen=(*elem)->errorMessage().length()>maxLen?(*elem)->errorMessage().length():maxLen;
      elem++;
    }
   
    // now I can allocate the memory for the strings

    unsigned int bufferCellSize = maxLen * sizeof(char);

    lensErrorMessage = (ub2*) malloc(nb * sizeof(ub2));
    bufferErrorMessage = (char*) malloc(nb * bufferCellSize);

    if (lensErrorMessage == 0 || bufferErrorMessage == 0  ) {
       if (lensErrorMessage != 0) free(lensErrorMessage);
       if (bufferErrorMessage != 0) free(bufferErrorMessage);  
       if (bufferFileId != 0) free(bufferFileId);
       if (bufferErrorCode != 0) free(bufferErrorCode);
       if (lensFileId != 0) free(lensFileId);
       if (lensErrorCode != 0) free(lensErrorCode);
       castor::exception::OutOfMemory e; 
       throw e;
    }


    elem=listToUpdate.begin();
    unsigned int i=0;
    while (elem != listToUpdate.end()){ 

      // fileid

      oracle::occi::Number n = (double)((*elem)->fileid());
      oracle::occi::Bytes b = n.toBytes();
      b.getBytes(bufferFileId[i],b.length());
      lensFileId[i] = b.length();

      // errorCode

      n = (double)((*elem)->errorCode());
      b = n.toBytes();
      b.getBytes(bufferErrorCode[i],b.length());
      lensErrorCode[i] = b.length();

      // errorMessage
      
      lensErrorMessage[i]=(*elem)->errorMessage().length();
      if (lensErrorMessage[i] != 0){
	strncpy(bufferErrorMessage+(bufferCellSize*i),(*elem)->errorMessage().c_str(),lensErrorMessage[i]);
      
      } else {
	strncpy(bufferErrorMessage+(bufferCellSize*i),"",1);
	lensErrorMessage[i]=1;
      }

      i++;
      elem++;
    }

    ub4 unused = nb;
    m_updateSubRequestSegmentsStatement->setDouble(1,(double)obj->id()); // subrequest id

    m_updateSubRequestSegmentsStatement->setDataBufferArray(2,bufferFileId, oracle::occi::OCCI_SQLT_NUM, nb, &unused, 21, lensFileId); // fileid
    m_updateSubRequestSegmentsStatement->setDataBufferArray(3,bufferErrorCode, oracle::occi::OCCI_SQLT_NUM,nb, &unused, 21, lensErrorCode); // errorCode
    
    ub4 len=nb;
    m_updateSubRequestSegmentsStatement->setDataBufferArray(4,bufferErrorMessage, oracle::occi::OCCI_SQLT_CHR, len, &len, maxLen, lensErrorMessage); // errorMessage

    m_updateSubRequestSegmentsStatement->executeUpdate();

  } catch (oracle::occi::SQLException  e) {
    // clean up

    if (bufferFileId) free(bufferFileId);
    bufferFileId=NULL;
    if (bufferErrorCode) free(bufferErrorCode);
    bufferErrorCode=NULL;
    if (bufferErrorMessage) free(bufferErrorMessage);
    bufferErrorMessage=NULL;
    if (lensFileId) free(lensFileId);
    lensFileId=NULL;
    if (lensErrorCode) free(lensErrorCode);
    lensErrorCode=NULL;
    if (lensErrorMessage) free(lensErrorMessage);
    lensErrorMessage=NULL;
    
    // forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught updateSubRequestSegments"
      << std::endl << e.what();
    throw ex;   
  }  catch (castor::exception::Exception ex) {
    
    // clean up

    if (bufferFileId) free(bufferFileId);
    bufferFileId=NULL;
    if (bufferErrorCode) free(bufferErrorCode);
    bufferErrorCode=NULL;
    if (bufferErrorMessage) free(bufferErrorMessage);
    bufferErrorMessage=NULL;
    if (lensFileId) free(lensFileId);
    lensFileId=NULL;
    if (lensErrorCode) free(lensErrorCode);
    lensErrorCode=NULL;
    if (lensErrorMessage) free(lensErrorMessage);
    lensErrorMessage=NULL;
    
    //forward the exception
    throw ex;

  }
  
  
  // clean up

  if (bufferFileId) free(bufferFileId);
  bufferFileId=NULL;
  if (bufferErrorCode) free(bufferErrorCode);
  bufferErrorCode=NULL;
  if (bufferErrorMessage) free(bufferErrorMessage);
  bufferErrorMessage=NULL;
  if (lensFileId) free(lensFileId);
  lensFileId=NULL;
  if (lensErrorCode) free(lensErrorCode);
  lensErrorCode=NULL;
  if (lensErrorMessage) free(lensErrorMessage);
  lensErrorMessage=NULL;

}


//------------------------------------------------------------------------------
// insertSubRequestSegments
//------------------------------------------------------------------------------

void castor::repack::ora::OraRepackSvc::insertSubRequestSegments(
								 castor::repack::RepackSubRequest* obj) 
  throw (castor::exception::Exception)
{
  
    /// stores it into the database

    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);   
    svcs()->updateRep(&ad, obj, false);
    svcs()->fillRep(&ad,obj,OBJ_RepackSegment,false); // segments too 
}


//------------------------------------------------------------------------------
// private:  getSegmentsForSubRequest
//------------------------------------------------------------------------------
void castor::repack::ora::OraRepackSvc::getSegmentsForSubRequest(RepackSubRequest* rsub) throw (castor::exception::Exception){

  oracle::occi::ResultSet * rset=NULL;
  try{
    if ( m_getSegmentsForSubRequestStatement == NULL ) {
      m_getSegmentsForSubRequestStatement = 
	createStatement(s_getSegmentsForSubRequestStatementString);
      m_getSegmentsForSubRequestStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR); 
    }

    m_getSegmentsForSubRequestStatement->setDouble(1, rsub->id());
    m_getSegmentsForSubRequestStatement->executeUpdate();
    rset = m_getSegmentsForSubRequestStatement->getCursor(2);
    oracle::occi::ResultSet::Status status = rset->next();
      
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
	
      RepackSegment* seg= new RepackSegment();
      seg->setFileid((u_signed64)rset->getDouble(1));
      seg->setSegsize((u_signed64)rset->getDouble(2));
      seg->setCompression(rset->getInt(3));
      seg->setFilesec(rset->getInt(4));
      seg->setCopyno(rset->getInt(5));
      seg->setBlockid((u_signed64)rset->getDouble(6));
      seg->setFileseq((u_signed64)rset->getDouble(7));
      seg->setErrorCode(rset->getInt(8));
      seg->setErrorMessage(rset->getString(9));
      seg->setId((u_signed64)rset->getDouble(10));

      // attach it
      seg->setRepacksubrequest(rsub);
      rsub->addRepacksegment(seg);
      status = rset->next();
    }
    m_getSegmentsForSubRequestStatement->closeResultSet(rset);

  } catch(oracle::occi::SQLException e){
    //forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getSegmentsForSubRequest"
      << std::endl << e.what();
    throw ex;
  }

}


//------------------------------------------------------------------------------
// getSubRequestByVid 
//------------------------------------------------------------------------------
castor::repack::RepackResponse* 
castor::repack::ora::OraRepackSvc::getSubRequestByVid(std::string vid, bool fill) throw (castor::exception::Exception) { 
  RepackResponse* result = new RepackResponse();
  RepackSubRequest* sub = new RepackSubRequest();
  sub->setVid(vid);
  result->setRepacksubrequest(sub);

  oracle::occi::ResultSet *rset =NULL;
  try{
    if ( m_getSubRequestByVidStatement == NULL ) {
      m_getSubRequestByVidStatement = 
	createStatement(s_getSubRequestByVidStatementString);
      m_getSubRequestByVidStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR); 
    }

    m_getSubRequestByVidStatement->setString(1, vid);
    m_getSubRequestByVidStatement->executeUpdate();

    // RepackSubRequest

    rset = m_getSubRequestByVidStatement->getCursor(2);

    // Run through the cursor
    oracle::occi::ResultSet::Status status = rset->next();

    // we took just the first

    if (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      result->setErrorCode(0);
      sub->setVid(rset->getString(1));
      sub->setXsize((u_signed64)rset->getDouble(2));
      sub->setStatus((castor::repack::RepackSubRequestStatusCode)rset->getInt(3));
      sub->setFilesMigrating(rset->getInt(4));
      sub->setFilesStaging(rset->getInt(5));
      sub->setFiles(rset->getInt(6));
      sub->setFilesFailed(rset->getInt(7));
      sub->setCuuid(rset->getString(8));
      sub->setSubmitTime((u_signed64)rset->getDouble(9));
      sub->setFilesStaged(rset->getInt(10));
      sub->setFilesFailedSubmit(rset->getInt(11));
      sub->setRetryNb((u_signed64)rset->getDouble(12));
      sub->setId((u_signed64)rset->getDouble(13));
      sub->setRepackrequest(NULL); // don't used        
    
      // Get the segment and attach it 

      if (fill)  getSegmentsForSubRequest(sub);
 
     
    } else {
      
      //No tape

      result->setErrorCode(-1);
      result->setErrorMessage("Unknown vid");
      
    }
    
    m_getSubRequestByVidStatement->closeResultSet(rset);

  } catch (oracle::occi::SQLException e) {
    //clean up
    if (result) freeRepackObj(result);
    //forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getSubRequestByVid"
      << std::endl << e.what();
    throw ex;

  } catch (castor::exception::Exception  e) {
    //clean up
    if (result) freeRepackObj(result);
    // forward the exception
    throw e;
  }
  return result;

}


//------------------------------------------------------------------------------
// getSubRequestsByStatus
//------------------------------------------------------------------------------
std::vector<castor::repack::RepackSubRequest*> 
castor::repack::ora::OraRepackSvc::getSubRequestsByStatus( castor::repack::RepackSubRequestStatusCode st, bool fill)throw (castor::exception::Exception)
{
  std::vector<RepackSubRequest*> subs;
  
  RepackSubRequest* resp=NULL;
  oracle::occi::ResultSet *rset = NULL;
  
  try{
    /// Check whether the statements are ok
    if ( m_getSubRequestsByStatusStatement == NULL ) {
      m_getSubRequestsByStatusStatement = createStatement(s_getSubRequestsByStatusStatementString);
      m_getSubRequestsByStatusStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR); // to get a valid repack subrequest request 
    }	
    m_getSubRequestsByStatusStatement->setInt(1,st);
    m_getSubRequestsByStatusStatement->executeUpdate();
    rset = m_getSubRequestsByStatusStatement->getCursor(2);
    // Run through the cursor
    oracle::occi::ResultSet::Status status = rset->next();
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      resp = new castor::repack::RepackSubRequest();
      resp->setVid(rset->getString(1));
      resp->setXsize((u_signed64)rset->getDouble(2));
      resp->setStatus((castor::repack::RepackSubRequestStatusCode) rset->getInt(3));
      resp->setFilesMigrating(rset->getInt(4));
      resp->setFilesStaging(rset->getInt(5));
      resp->setFiles(rset->getInt(6));
      resp->setFilesFailed(rset->getInt(7));
      resp->setCuuid(rset->getString(8));
      resp->setSubmitTime((u_signed64)rset->getDouble(9));
      resp->setFilesStaged(rset->getInt(10));
      resp->setFilesFailedSubmit(rset->getInt(11));
      resp->setRetryNb((u_signed64)rset->getDouble(12));
      resp->setId((u_signed64)rset->getDouble(13));
      
      // get request

      castor::BaseAddress ad;
      ad.setCnvSvcName("DbCnvSvc");
      ad.setCnvSvcType(castor::SVC_DBCNV);
      svcs()->fillObj(&ad,resp,OBJ_RepackRequest); //get the request

      // get segments if needed
      
      if (fill) getSegmentsForSubRequest(resp);
      
      subs.push_back(resp);
      status = rset->next();

    }
    m_getSubRequestsByStatusStatement->closeResultSet(rset); 
  }catch (oracle::occi::SQLException e) { 
    
    // clean up
    std::vector<RepackSubRequest*>::iterator toDelete= subs.begin();
    while (toDelete != subs.end()){
      if (*toDelete) freeRepackObj(*toDelete);
      toDelete++;
    }
    subs.clear();
    
    // forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getSubRequests"
      << std::endl << e.what();
    throw ex;

  } catch (castor::exception::Exception ex) {

    //clean up
    std::vector<RepackSubRequest*>::iterator toDelete= subs.begin();
    while (toDelete != subs.end()){
      if (*toDelete) freeRepackObj(*toDelete);
      *toDelete=NULL;
      toDelete++;
    }
    subs.clear();
    // forward the exception
    throw ex;
  } 

  return subs;	
}


//------------------------------------------------------------------------------
// getAllSubRequests
//------------------------------------------------------------------------------
castor::repack::RepackAck* 
castor::repack::ora::OraRepackSvc::getAllSubRequests()
  throw (castor::exception::Exception)
{
  castor::repack::RepackAck* ack=new castor::repack::RepackAck();
  castor::repack::RepackResponse* resp = NULL;
  castor::repack::RepackSubRequest* sub=NULL;
  
  oracle::occi::ResultSet *rset = NULL;
  try{
    /// Check whether the statements are ok
    if ( m_getAllSubRequestsStatement == NULL ) {
      m_getAllSubRequestsStatement = createStatement(s_getAllSubRequestsStatementString);
      m_getAllSubRequestsStatement->registerOutParam
        (1, oracle::occi::OCCICURSOR); // to get a valid repack subrequest request 
    }	
    m_getAllSubRequestsStatement->executeUpdate();
    rset = m_getAllSubRequestsStatement->getCursor(1);
    // Run through the cursor
    oracle::occi::ResultSet::Status status = rset->next();
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      resp = new RepackResponse();
      resp->setErrorCode(0);

      sub = new RepackSubRequest();
      sub->setVid(rset->getString(1));
      sub->setXsize((u_signed64)rset->getDouble(2));
      sub->setStatus((castor::repack::RepackSubRequestStatusCode) rset->getInt(3));
      sub->setFilesMigrating(rset->getInt(4));
      sub->setFilesStaging(rset->getInt(5));
      sub->setFiles(rset->getInt(6));
      sub->setFilesFailed(rset->getInt(7));
      sub->setCuuid(rset->getString(8));
      sub->setSubmitTime((u_signed64)rset->getDouble(9));
      sub->setFilesStaged(rset->getInt(10));
      sub->setFilesFailedSubmit(rset->getInt(11));
      sub->setRetryNb((u_signed64)rset->getDouble(12));
      sub->setId((u_signed64)rset->getDouble(13));
      sub->setRepackrequest(NULL); // not used
      
      resp->setRepacksubrequest(sub);
      ack->addRepackresponse(resp);
      status = rset->next();

    }
    m_getAllSubRequestsStatement->closeResultSet(rset);  
  
  }catch (oracle::occi::SQLException e) {

    // clean up
    if (ack) freeRepackObj(ack);
    
    // forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in getAllSubRequests"
      << std::endl << e.what();
    throw ex;

  } catch (castor::exception::Exception ex) {
    // clean up
    if (ack) freeRepackObj(ack);
    //forward the exception
    throw ex;
  }

  return ack;	
}


//------------------------------------------------------------------------------
// validateRepackSubRequest
//------------------------------------------------------------------------------
bool  castor::repack::ora::OraRepackSvc::validateRepackSubRequest( RepackSubRequest* tape, int numFiles, int numTapes)  throw (castor::exception::Exception){

  try{
    /// Check whether the statements are ok
    if ( m_validateRepackSubRequestStatement == NULL ) {
      m_validateRepackSubRequestStatement = createStatement(s_validateRepackSubRequestStatementString);
      m_validateRepackSubRequestStatement->registerOutParam(4, oracle::occi::OCCIINT); // to get a valid repack subrequest request 
   
    }	
    m_validateRepackSubRequestStatement->setDouble(1, tape->id());
    m_validateRepackSubRequestStatement->setInt(2, numFiles);
    m_validateRepackSubRequestStatement->setInt(3, numTapes);

    m_validateRepackSubRequestStatement->executeUpdate();
    
    /// get the request we found
    bool ret =  (u_signed64)  m_validateRepackSubRequestStatement->getInt(4) == 1?true:false;
    return ret;
  }catch (oracle::occi::SQLException e) {
    // forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in validateRepackSubRequest"
      << std::endl << e.what();
    throw ex;
  }
  return false;
}


//------------------------------------------------------------------------------
// resurrectTapesOnHold
//------------------------------------------------------------------------------

void  castor::repack::ora::OraRepackSvc::resurrectTapesOnHold(int numFiles, int numTapes)  throw (castor::exception::Exception){

  try{
    /// Check whether the statements are ok
    if ( m_resurrectTapesOnHoldStatement == NULL ) {
      m_resurrectTapesOnHoldStatement = createStatement(s_resurrectTapesOnHoldStatementString);
   
    }	
    m_resurrectTapesOnHoldStatement->setInt(1, numFiles);
    m_resurrectTapesOnHoldStatement->setInt(2, numTapes);
    m_resurrectTapesOnHoldStatement->executeUpdate();
    

  }catch (oracle::occi::SQLException e) {
    // forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in resurrectTapesOnHold"
      << std::endl << e.what();
    throw ex;

  }  
}


//------------------------------------------------------------------------------
// restartSubRequest
//------------------------------------------------------------------------------
void  castor::repack::ora::OraRepackSvc::restartSubRequest(u_signed64 srId)
  throw (castor::exception::Exception){

  try{
    /// Check whether the statements are ok
    if ( m_restartSubRequestStatement == NULL ) {
      m_restartSubRequestStatement = createStatement(s_restartSubRequestStatementString);
   
    }	
    m_restartSubRequestStatement->setDouble(1, srId);
    m_restartSubRequestStatement->executeUpdate();
    

  } catch (oracle::occi::SQLException e) {
    //forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in restartSubRequest"
      << std::endl << e.what();
    throw ex;

   
  }  
}


//------------------------------------------------------------------------------
// changeSubRequestsStatus
//------------------------------------------------------------------------------

castor::repack::RepackAck*  castor::repack::ora::OraRepackSvc::changeSubRequestsStatus(std::vector<castor::repack::RepackSubRequest*> srs,  castor::repack::RepackSubRequestStatusCode st)
  throw (castor::exception::Exception){
  RepackAck* ack=new castor::repack::RepackAck();
  oracle::occi::ResultSet *rset =NULL;
  ub2 *lens = NULL;
  char *buffer = NULL;

  try{
    /// Check whether the statements are ok
    if ( m_changeSubRequestsStatusStatement == NULL ) {
      m_changeSubRequestsStatusStatement = createStatement(s_changeSubRequestsStatusStatementString);
      m_changeSubRequestsStatusStatement->registerOutParam
        (3, oracle::occi::OCCICURSOR);
   
    }

    // loop 
    std::map<std::string,int> hitMissMap;

    std::vector<std::string> listOfVids;
    std::vector<RepackSubRequest*>::iterator subReq=srs.begin();
    unsigned int maxLen=0;
    unsigned int numTapes=0;

    while (subReq !=srs.end()) {
      if (*subReq) {
	listOfVids.push_back((*subReq)->vid());
	hitMissMap[(*subReq)->vid()]=0;
	maxLen=maxLen>((*subReq)->vid()).length()?maxLen:((*subReq)->vid()).length();
	numTapes++;
      }
      subReq++;
    }
    
    if (numTapes==0 || maxLen==0){
      ack=new RepackAck();
      RepackResponse* resp=new RepackResponse();
      resp->setErrorCode(-1);
      resp->setErrorMessage("no tape given");
      ack->addRepackresponse(resp);
      return ack;
    }
    
    unsigned int bufferCellSize = maxLen * sizeof(char);

    lens = (ub2*) malloc(maxLen * sizeof(ub2));
    buffer =
      (char*) malloc(numTapes * bufferCellSize);

    if ( buffer == 0 ||  lens == 0 ) {
       if (buffer != 0) free(buffer);
       if (lens != 0) free(lens);
       castor::exception::OutOfMemory e; 
       throw e;
    }


    // Fill in the structure

    std::vector<std::string>::iterator vid= listOfVids.begin();
    int i=0;
    while (vid != listOfVids.end()){
      lens[i]=(*vid).length();
      strncpy(buffer+(i * bufferCellSize),(*vid).c_str(),lens[i]);
      vid++;
      i++;
    }

    ub4 len=numTapes;
    m_changeSubRequestsStatusStatement->setDataBufferArray
      (1, buffer, oracle::occi::OCCI_SQLT_CHR,
       len, &len, maxLen, lens);

    m_changeSubRequestsStatusStatement->setInt(2,(int)st);
    m_changeSubRequestsStatusStatement->executeUpdate();
    
    // RETURN
    rset = m_changeSubRequestsStatusStatement->getCursor(3);
    // Run through the cursor
    oracle::occi::ResultSet::Status status = rset->next();
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      RepackResponse* resp = new RepackResponse();
      RepackSubRequest* sub = new RepackSubRequest();
      sub->setVid(rset->getString(1));
      sub->setXsize((u_signed64)rset->getDouble(2));
      sub->setStatus((castor::repack::RepackSubRequestStatusCode) rset->getInt(3));
      sub->setFilesMigrating(rset->getInt(4));
      sub->setFilesStaging(rset->getInt(5));
      sub->setFiles(rset->getInt(6));
      sub->setFilesFailed(rset->getInt(7));
      sub->setCuuid(rset->getString(8));
      sub->setSubmitTime((u_signed64)rset->getDouble(9));
      sub->setFilesStaged(rset->getInt(10));
      sub->setFilesFailedSubmit(rset->getInt(11));
      sub->setRetryNb((u_signed64)rset->getDouble(12));
      sub->setId((u_signed64)rset->getDouble(13));
      sub->setRepackrequest(NULL);
      
      resp->setRepacksubrequest(sub);
      ack->addRepackresponse(resp);
      hitMissMap[sub->vid()]=1;    
      status = rset->next();
    } 
   

    if (ack->repackresponse().size() !=  listOfVids.size()) {
      // add miss response	
      std::vector<std::string>::iterator elem=listOfVids.begin();
      while (elem !=listOfVids.end()){
	if (hitMissMap[*elem] == 0){
	  // failure the request exists
	  RepackResponse* resp = new RepackResponse();
	  RepackSubRequest* sub= new RepackSubRequest(); 
	  sub->setVid(*elem);
	  sub->setRepackrequest(NULL);
	  resp->setRepacksubrequest(sub);
	  resp->setErrorCode(-1);
	  resp->setErrorMessage("operation not allowed");
	  ack->addRepackresponse(resp);
	}
	elem++;
      }
    }

    endTransation();
    m_changeSubRequestsStatusStatement->closeResultSet(rset);

  }catch (oracle::occi::SQLException e) {
    //clean up
    if (ack) freeRepackObj(ack);
    if (buffer) free(buffer);
    buffer=NULL;
    if (lens)   free(lens);
    lens=NULL;
    
    // forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in changeSubRequestsStatus"
      << std::endl << e.what();
    throw ex;


 
  }catch (castor::exception::Exception ex){
    // clean up
    if (ack) freeRepackObj(ack);
    if (buffer) free(buffer);
    buffer=NULL;
    if (lens)   free(lens);
    lens=NULL;
    //forward the exception
    throw ex;
  }
  
  // clean up

  if (buffer) free(buffer);
  buffer=NULL;
  if (lens)   free(lens);
  lens=NULL;

  return ack;  
}


//------------------------------------------------------------------------------
// changeAllSubRequestsStatus
//------------------------------------------------------------------------------
castor::repack::RepackAck*  castor::repack::ora::OraRepackSvc::changeAllSubRequestsStatus(castor::repack::RepackSubRequestStatusCode st)
  throw (castor::exception::Exception){
  castor::repack::RepackAck* ack=new castor::repack::RepackAck();
  oracle::occi::ResultSet *rset =NULL;

  try{
    /// Check whether the statements are ok
    if ( m_changeAllSubRequestsStatusStatement == NULL ) {
      m_changeAllSubRequestsStatusStatement =createStatement(s_changeAllSubRequestsStatusStatementString);
      m_changeAllSubRequestsStatusStatement->registerOutParam(2, oracle::occi::OCCICURSOR); 
   
    }	
    m_changeAllSubRequestsStatusStatement->setInt(1,(int)st);
    m_changeAllSubRequestsStatusStatement->executeUpdate();

    rset = m_changeAllSubRequestsStatusStatement->getCursor(2);
    // Run through the cursor
    oracle::occi::ResultSet::Status status = rset->next();

    if (status != oracle::occi::ResultSet::DATA_AVAILABLE){
      RepackResponse* resp = new RepackResponse();
      resp->setErrorCode(-1);
      resp->setErrorMessage("No finished tapes to archive");
      ack->addRepackresponse(resp);    
      return ack;
    }

    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      RepackResponse* resp = new RepackResponse();
      resp->setErrorCode(0);
      RepackSubRequest* sub= new RepackSubRequest();
      sub->setVid(rset->getString(1));
      sub->setXsize((u_signed64)rset->getDouble(2));
      sub->setStatus((castor::repack::RepackSubRequestStatusCode) rset->getInt(3));
      sub->setFilesMigrating(rset->getInt(4));
      sub->setFilesStaging(rset->getInt(5));
      sub->setFiles(rset->getInt(6));
      sub->setFilesFailed(rset->getInt(7));
      sub->setCuuid(rset->getString(8));
      sub->setSubmitTime((u_signed64)rset->getDouble(9));
      sub->setFilesStaged(rset->getInt(10));
      sub->setFilesFailedSubmit(rset->getInt(11));
      sub->setRetryNb((u_signed64)rset->getDouble(12));
      sub->setId((u_signed64)rset->getDouble(13));
      sub->setRepackrequest(NULL);
      resp->setRepacksubrequest(sub);
      ack->addRepackresponse(resp);
      status = rset->next();
    } 
    endTransation();
    m_changeAllSubRequestsStatusStatement->closeResultSet(rset);
  }catch (oracle::occi::SQLException e) {
    //clean up
    if (ack) freeRepackObj(ack);
    //forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in changeAllSubRequestsStatus"
      << std::endl << e.what();
    throw ex;
   
  } catch(castor::exception::Exception ex){
    //cleanup
    if (ack) freeRepackObj(ack);
    throw ex;
  }

  return ack;
}



//------------------------------------------------------------------------------
//  getLastTapeInformation 
//------------------------------------------------------------------------------
std::vector<castor::repack::RepackSegment*> 
castor::repack::ora::OraRepackSvc::getLastTapeInformation(std::string vidName, u_signed64* creationTime)
  throw (castor::exception::Exception)
{
 
  std::vector<castor::repack::RepackSegment*> result;
  oracle::occi::ResultSet *rset=NULL;

  try {
    if (vidName.length() ==0) {
      castor::exception::Internal ex;
      ex.getMessage() << "passed Parameter is NULL" << std::endl;
      throw ex;
    }
  
    if ( m_selectLastSegmentsSituationStatement == NULL ) {
      m_selectLastSegmentsSituationStatement = 
	createStatement(s_selectLastSegmentsSituationStatementString);
      
      m_selectLastSegmentsSituationStatement->registerOutParam(2, oracle::occi::OCCICURSOR); 
      m_selectLastSegmentsSituationStatement->registerOutParam(3, oracle::occi::OCCIDOUBLE);
      
    }

    m_selectLastSegmentsSituationStatement->setString(1,vidName);
    
    // execute the procedure

    m_selectLastSegmentsSituationStatement->executeUpdate();

    // get the Cursor
    rset =m_selectLastSegmentsSituationStatement->getCursor(2);

    // get the creationTime

     if (creationTime != NULL) *creationTime=( u_signed64 ) m_selectLastSegmentsSituationStatement->getDouble(3);


    // run the cursor and fill in the array
    
    oracle::occi::ResultSet::Status status = rset->next();
    
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {

      RepackSegment* seg = new RepackSegment();

      seg->setFileid((u_signed64)rset->getDouble(1));
      seg->setSegsize((u_signed64)rset->getDouble(2));
      seg->setCompression((u_signed64)rset->getDouble(3));
      seg->setFilesec(rset->getInt(4));
      seg->setCopyno(rset->getInt(5));
      seg->setBlockid((u_signed64)rset->getDouble(6));
      seg->setFileseq(rset->getInt(7));

      result.push_back(seg);

      status = rset->next();
    } 
    if (rset)  m_selectLastSegmentsSituationStatement->closeResultSet(rset);
  }catch (oracle::occi::SQLException e) {
    //clean up
    
    std::vector<castor::repack::RepackSegment*>::iterator item= result.begin();
    while (item != result.end()){
	 if (*item) delete(*item);
	 item ++;
    }
    result.clear();

    // forward the exception
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in selectLastSegmentsSituationStatement"
      << std::endl << e.what();
    throw ex;
    
    
  } catch (castor::exception::Exception ex){
    //clean up
    std::vector<castor::repack::RepackSegment*>::iterator item= result.begin();
    while (item != result.end()){
	 if (*item) delete(*item);
	 item ++;
    }
    result.clear();

    // forward the exception
    throw ex;
  }
  return result;
}


