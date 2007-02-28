/******************************************************************************
 *                      castor/repack/DatabaseHelper.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files

#include "castor/repack/DatabaseHelper.hpp"

#include "castor/db/cnv/DbRepackRequestCnv.hpp"
#include "castor/db/cnv/DbRepackSubRequestCnv.hpp"
#include "castor/db/cnv/DbRepackSegmentCnv.hpp"




//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL for choosing the next job to do, depending on status
const std::string castor::repack::DatabaseHelper::s_selectCheckSubRequestStatementString =
  "SELECT q.id FROM RepackSubRequest q WHERE q.status= :1 AND ROWNUM < 2";

/// SQL for existing segment check
const std::string castor::repack::DatabaseHelper::s_selectExistingSegmentsStatementString =
  "SELECT fileid,compression,segsize,filesec, RepackSegment.id,copyno, RepackSubRequest.status FROM RepackSegment,RepackSubRequest WHERE RepackSegment.fileid = :1 AND RepackSubRequest.id = RepackSegment.vid AND RepackSubRequest.status != 6";

/// SQL for archiving RepackSubRequests
const std::string castor::repack::DatabaseHelper::s_archiveStatementString =
  "UPDATE RepackSubRequest set status = 6 where vid=:1 and status = 5"; // ARCHIVED, FINISHED

/// SQL for getting a specified RepacksubRequest in a certain status
const std::string castor::repack::DatabaseHelper::s_isStoredStatementString =
  "SELECT id FROM RepackSubRequest where vid=:1 and status != 6";   // SUBREQUEST_ARCHIVED

/// SQL for getting RepackSubRequest in a certain status
const std::string castor::repack::DatabaseHelper::s_selectAllSubRequestsStatusStatementString =
  "SELECT id FROM RepackSubRequest where status = :1 order by vid";

/// SQL for getting RepackSubRequest with a certain vid
const std::string castor::repack::DatabaseHelper::s_selectAllSubRequestsVidStatementString =
  "SELECT id FROM RepackSubRequest where vid = :1";

/// SQL for getting RepackSubRequest which are not SUBREQUEST_ARCHIVED
const std::string castor::repack::DatabaseHelper::s_selectAllSubRequestsStatementString =
  "SELECT id FROM RepackSubRequest where status != 6 order by vid"; // ARCHIVED

/// SQL for locking a table row of a RepackSubRequest
const std::string castor::repack::DatabaseHelper::s_selectLockStatementString = 
  "SELECT * from RepackSubRequest where id = :1 FOR UPDATE";


/// SQL for getting lattest information of a tapy copy to do a repack undo

const std::string castor::repack::DatabaseHelper::s_selectLastSegmentsSituationStatementString=
"select repacksubrequest.id from repacksubrequest,repackrequest where repacksubrequest.requestid=repackrequest.id and repackrequest.creationtime in (select max(creationtime) from (select * from repackrequest where id in (select requestid from repacksubrequest where vid=:1))) and repacksubrequest.vid=:1";


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::repack::DatabaseHelper::DatabaseHelper() : 
	  DbBaseObj(NULL),
    m_selectCheckStatement(NULL),
    m_selectCheckSubRequestStatement(NULL),
    m_selectAllSubRequestsStatement(NULL),
    m_selectExistingSegmentsStatement(NULL),
    m_isStoredStatement(NULL),
    m_archiveStatement(NULL),
    m_selectAllSubRequestsStatusStatement(NULL),
    m_selectAllSubRequestsVidStatement(NULL),
    m_selectLockStatement(NULL),
    m_selectLastSegmentsSituationStatement(NULL){
    try {
	ad.setCnvSvcName("DbCnvSvc");
	ad.setCnvSvcType(castor::SVC_DBCNV);
   
    } catch(castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 2, params);
    }

};




//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::repack::DatabaseHelper::~DatabaseHelper() throw(){
  reset();
  cnvSvc()->dropConnection();
};




//------------------------------------------------------------------------------
// resets the statements
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
   if ( m_selectCheckStatement ) delete m_selectCheckStatement;
   if ( m_selectCheckSubRequestStatement ) delete m_selectCheckSubRequestStatement;
   if ( m_selectAllSubRequestsStatement ) delete m_selectAllSubRequestsStatement;
   if ( m_selectExistingSegmentsStatement ) delete m_selectExistingSegmentsStatement;
   if ( m_isStoredStatement ) delete m_isStoredStatement;
   if ( m_archiveStatement ) delete m_archiveStatement;
   if ( m_selectAllSubRequestsStatusStatement ) delete m_selectAllSubRequestsStatusStatement;
   if ( m_selectAllSubRequestsVidStatement ) delete m_selectAllSubRequestsVidStatement;
   if ( m_selectLockStatement ) delete m_selectLockStatement;
   if (m_selectLastSegmentsSituationStatement) delete m_selectLastSegmentsSituationStatement;
  } catch (castor::exception::SQLError ignored) {};
  // Now reset all pointers to 0
   m_selectCheckStatement = NULL;
   m_selectCheckSubRequestStatement = NULL;
   m_selectAllSubRequestsStatement = NULL;
   m_selectExistingSegmentsStatement = NULL;
   m_isStoredStatement = NULL;
   m_archiveStatement = NULL;
   m_selectAllSubRequestsStatusStatement = NULL;
   m_selectAllSubRequestsVidStatement = NULL;
   m_selectLockStatement = NULL;
   m_selectLastSegmentsSituationStatement=NULL;
}




//------------------------------------------------------------------------------
// store_Request 
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::storeRequest(castor::repack::RepackRequest* rreq)
						throw (castor::exception::Exception)
{
  stage_trace(2,"Storing Request in DB" );

  try {

  // Store files for RepackRequest
  if ( rreq != NULL ) {
    svcs()->createRep(&ad, rreq, false);			// Create Representation in DB
    svcs()->fillRep(&ad, rreq, OBJ_RepackSubRequest, false);	//and fill it

    // we must not forget the segments
    for (int i = 0; i < rreq->subRequest().size(); i++){
      svcs()->fillRep(&ad, rreq->subRequest().at(i), OBJ_RepackSegment, false);
    }
    svcs()->commit(&ad);

    // "Request stored in DB" message
    castor::dlf::Param params[] =
	    {castor::dlf::Param("ID", rreq->id()),
	    castor::dlf::Param("SubRequests", rreq->subRequest().size())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 12, 2, params);
    }
  } catch (castor::exception::SQLError e) {
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to store RepackSubRequest: " 
                    << std::endl << e.getMessage().str();
    cnvSvc()->rollback();
    castor::dlf::Param params[] =
    {castor::dlf::Param("Standard Message", sstrerror(e.code())),
     castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 2, params);
  }
  
  return;
}









//------------------------------------------------------------------------------
// is_stored
//------------------------------------------------------------------------------
bool castor::repack::DatabaseHelper::is_stored(std::string vid) 
						throw (castor::exception::Exception)
{
  bool result = false;

  try {
    // Check whether the statements are ok
    if ( m_isStoredStatement == NULL ) {
      m_isStoredStatement = createStatement(s_isStoredStatementString);
    }
    m_isStoredStatement->setString(1, vid);
    castor::db::IDbResultSet *rset = m_isStoredStatement->executeQuery();
    svcs()->commit(&ad);
    if ( rset->next() )	// Something found
    {
      castor::dlf::Param params[] =
      {castor::dlf::Param("VID", vid)};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 28, 1, params);
      result = true;
    }
    delete rset;

  }catch (castor::exception::SQLError e){
    delete m_selectCheckStatement;
    castor::exception::Internal ex;
    ex.getMessage() << "DatabaseHelper::is_stored(..)" 
                    << std::endl << e.getMessage().str();
    throw ex;
  }	
  
  delete m_isStoredStatement;
  m_isStoredStatement = NULL;
  
  return result;
}










//------------------------------------------------------------------------------
// getSubRequestByVid
//------------------------------------------------------------------------------
castor::repack::RepackSubRequest* 
             castor::repack::DatabaseHelper::getSubRequestByVid(
                                                               std::string vid,
                                                               bool fill)
                                            throw (castor::exception::Exception)
{
  RepackSubRequest* result = NULL;
  try {
  // Check whether the statements are ok
    if ( m_selectCheckStatement == NULL ) {
      m_selectCheckStatement = createStatement(s_isStoredStatementString);
    }
    m_selectCheckStatement->setString(1, vid);
    castor::db::IDbResultSet *rset = m_selectCheckStatement->executeQuery();
    svcs()->commit(&ad);
    /// Something found
    if ( rset->next() )	
    {
      u_signed64 id = (u_signed64)rset->getDouble(1);
      if ( rset->next() ){
       // This case should never happen, however not bad to check it
        castor::exception::Internal ex;
        ex.getMessage() << "More than one tape in active state (not archived) found." 
                        << std::endl;
        delete rset;
        throw ex; 
      }
      result = getSubRequest(id);
      delete rset;
			/// we fill the returned Subrequest with Segments and Request
      if ( fill )
         svcs()->fillObj(&ad,result,OBJ_RepackSegment);
      
      svcs()->fillObj(&ad,result,OBJ_RepackRequest);
    }
    
    /// no such tape in DB
    else
    {
      castor::exception::Internal ex;
      ex.getMessage() << "No such Tape found." 
                      << std::endl;
      delete rset;
      throw ex; 
    }
		
  /// Exception handling
  }catch (castor::exception::SQLError e){
   delete m_selectCheckStatement;
   castor::exception::Internal ex;
   ex.getMessage() << "Unable to get a RepackSubRequest from DB: " 
                   << std::endl << e.getMessage().str();
   throw ex;
  }	

  /// delete Statement and return result
  delete m_selectCheckStatement;
  m_selectCheckStatement = NULL;
  return result;
}









//------------------------------------------------------------------------------
// updateSubRequest
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::updateSubRequest(
        castor::repack::RepackSubRequest* obj,
        bool fill,
        Cuuid_t& cuuid) 
        throw (castor::exception::Exception)
{
  try {
    /// stores it into the database
    svcs()->updateRep(&ad, obj, false);

    /// we add the segments to the repacksubrequest 
    if (fill) {
        svcs()->fillRep(&ad, obj, OBJ_RepackSegment, false);
    }
    else {
        for (int i = 0; i < obj->segment().size(); i++)
          svcs()->updateRep(&ad, obj->segment().at(i), false);
    }
    svcs()->commit(&ad);
  /// Exception handling
  } catch (castor::exception::Exception e) {
    svcs()->rollback(&ad);
    castor::dlf::Param params[] =
    {castor::dlf::Param("Standard Message", sstrerror(e.code())),
     castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 27, 2, params);
    return;
  }
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 30, 0, NULL);
}





//------------------------------------------------------------------------------
// private : getSubRequest
//------------------------------------------------------------------------------
castor::repack::RepackSubRequest* 
			castor::repack::DatabaseHelper::getSubRequest(
						u_signed64 sub_id)
						throw (castor::exception::Exception)   
{
  castor::IObject* obj = cnvSvc()->getObjFromId( sub_id );

  if (obj == NULL) {
    castor::exception::Internal ex;
    ex.getMessage() << "DatabaseHelper::getSubRequest(..) :"
                    << " could not retrieve object for id "
                    << sub_id;
    throw ex;
  }
  /// Now, lets see if the right Object was retrieved
  RepackSubRequest* result = dynamic_cast<RepackSubRequest*>(obj);
	
  if ( result == NULL ){ /// oops 
    castor::exception::Internal ex;
    ex.getMessage()  << "DatabaseHelper::getSubRequest(..) :"
                     << " object retrieved for id : " 
                     << sub_id << "was a "
                     << castor::ObjectsIdStrings[obj->type()];
    delete obj;	/// never forget to delete everything used 
    throw ex;
  }
  return result;
}


//------------------------------------------------------------------------------
// unlock
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::lock(RepackSubRequest* tape) throw (castor::exception::Exception)
{
  if ( tape != NULL ){
  /// Since the RepacksubRequest is the only object, which is changed in the DB
  /// we take a statement which locks only this in the DB   
  try {
      if ( m_selectLockStatement == NULL ) {
        m_selectLockStatement = createStatement(s_selectLockStatementString);
      }
      /** Lock the entry */
      m_selectLockStatement->setInt(1, tape->id());
      castor::db::IDbResultSet *rset = m_selectLockStatement->executeQuery();
      if ( !rset->next() ){
        castor::exception::Internal ex;
        ex.getMessage() << "DatabaseHelper::lock(..):"
                        << "Cannot lock table entry for tape " << tape->id() 
                        << std::endl;
        throw ex;
      }

      delete rset;
    }catch (castor::exception::SQLError e){
      svcs()->rollback(&ad);
      castor::dlf::Param params[] = 
        {castor::dlf::Param("Error", e.getMessage().str() )};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 1, params);
    }
  }
  /** AFTER THIS CALL AN UNLOCK HAS TO BE DONE */
}



//------------------------------------------------------------------------------
// unlock
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::unlock() throw ()
{
   try {
    svcs()->commit(&ad);
  }catch (castor::exception::SQLError e){
    cnvSvc()->rollback();
    castor::dlf::Param params[] = 
      {castor::dlf::Param("Error", e.getMessage().str() )};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 1, params);
  }


}



//------------------------------------------------------------------------------
// private : checkSubRequestStatus
//------------------------------------------------------------------------------
castor::repack::RepackSubRequest* 
		castor::repack::DatabaseHelper::checkSubRequestStatus(int status)
					throw (castor::exception::Exception)
{
  RepackSubRequest* result = NULL;
  castor::db::IDbResultSet *rset = NULL;
  stage_trace(3,"Check Request with status %d",status);
  /// Check whether the statements are ok
  if ( m_selectCheckSubRequestStatement == NULL ) {
    m_selectCheckSubRequestStatement = createStatement(s_selectCheckSubRequestStatementString);
  }

  try {	
    m_selectCheckSubRequestStatement->setInt(1, status);
    m_selectCheckSubRequestStatement->autoCommit();
    rset = m_selectCheckSubRequestStatement->executeQuery();
    
    if ( rset->next() ){
      /// get the request we found
      u_signed64 id = (u_signed64)rset->getDouble(1);
      result = getSubRequest(id);
      svcs()->fillObj(&ad,result,OBJ_RepackRequest);
      svcs()->fillObj(&ad,result,OBJ_RepackSegment);
    }
    svcs()->commit(&ad);
    delete rset;
  }catch (castor::exception::SQLError e) {
    /// in case of an exception, we have to delete the RepackSubRequest.
    /// if it is an excpetion in getsubRequest, we don't care (done by the method)
    /// fillObj for RepackRequest - ok, nothing to do.because the obj was not filled
    /// fillObj for RepackSegment - free from the top of the hierachy (RepackRequest)
    if ( result && result->requestID()) freeRepackObj(result->requestID());
    if ( rset ) delete rset;
    if ( m_selectCheckSubRequestStatement) delete m_selectCheckSubRequestStatement;
    m_selectCheckSubRequestStatement = NULL;
    svcs()->rollback(&ad);
    castor::exception::Internal ex;
	  ex.getMessage() 
	  << "DatabaseHelper::checkSubRequestStatus(..): "
	  << "Unable to get the status from a RepackSubRequest" 
	  << std::endl << e.getMessage().str();
	  throw ex;
  }
            
  delete m_selectCheckSubRequestStatement;
  m_selectCheckSubRequestStatement = NULL;
  stage_trace(3,"OK");
  
  return result;	
}

//------------------------------------------------------------------------------
// archive
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::archive(std::string vid) throw (castor::exception::Exception){
  
  if ( m_archiveStatement == NULL ) {
    m_archiveStatement = createStatement(s_archiveStatementString);
  }
  
  try { 
    m_archiveStatement->setString(1, vid);
    m_archiveStatement->execute();
    svcs()->commit(&ad);
  }catch (castor::exception::SQLError e) {
    svcs()->rollback(&ad);
    castor::exception::Internal ex;
    ex.getMessage() 
    << "DatabaseHelper::archive(..): "
    << std::endl << e.getMessage().str();
    throw ex;
  }
}



//------------------------------------------------------------------------------
// private : remove
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::remove(castor::IObject* obj) 
					throw (castor::exception::Exception)
{
  /* we don't really delete something from the database, since a history is 
   * to be kept, so only the RepackSubRequest is set to ARCHIVED
   */
  try {
    
    if ( obj->type() == OBJ_RepackSubRequest ){
      
      RepackSubRequest* sreq = dynamic_cast<RepackSubRequest*>(obj);
      sreq->setStatus(SUBREQUEST_DONE);
      svcs()->updateRep(&ad, sreq, false);
      svcs()->commit(&ad);
    }
  }catch (castor::exception::SQLError e){
    //svcs()->rollback(&ad);
    cnvSvc()->rollback();
    castor::exception::Internal ex;
    ex.getMessage() << "DatabaseHelper::remove():"
                    << "Unable to remove Object with Type "
                    << obj->type() << "from DB" 
                    << std::endl << e.getMessage().str();
    throw ex;
  }
}



//------------------------------------------------------------------------------
// getAllSubRequests
//------------------------------------------------------------------------------
std::vector<castor::repack::RepackSubRequest*>*
                             castor::repack::DatabaseHelper::getAllSubRequests()
                                             throw (castor::exception::Exception)
{
  if ( m_selectAllSubRequestsStatement == NULL ) {
    m_selectAllSubRequestsStatement = 
                         createStatement(s_selectAllSubRequestsStatementString);
  }
  return internalgetSubRequests( m_selectAllSubRequestsStatement );
}


//------------------------------------------------------------------------------
// getAllSubRequestsStatus 
//------------------------------------------------------------------------------
std::vector<castor::repack::RepackSubRequest*>* 
                       castor::repack::DatabaseHelper::getAllSubRequestsStatus(
                                                                       int status
                                                                      )
                                              throw (castor::exception::Exception)
{
  if ( m_selectAllSubRequestsStatusStatement == NULL ) {
    m_selectAllSubRequestsStatusStatement = 
      createStatement(s_selectAllSubRequestsStatusStatementString);
  }
  m_selectAllSubRequestsStatusStatement->setInt(1, status);
  
  std::vector<castor::repack::RepackSubRequest*>* result = NULL;
  result = internalgetSubRequests(m_selectAllSubRequestsStatusStatement);
  
  /// we want to have to corresponding RepackRequest for each RepackSubRequest
  for (int i=0; i<result->size(); i++ ){
    svcs()->fillObj(&ad,result->at(i),OBJ_RepackRequest);
  }
  svcs()->commit(&ad);
  return result;
}


//------------------------------------------------------------------------------
// getAllSubRequestsVid 
//------------------------------------------------------------------------------
std::vector<castor::repack::RepackSubRequest*>* 
                       castor::repack::DatabaseHelper::getAllSubRequestsVid(
                                                                 std::string vid 
                                                                      )
                                              throw (castor::exception::Exception)
{
  if ( m_selectAllSubRequestsVidStatement == NULL ) {
    m_selectAllSubRequestsVidStatement = 
      createStatement(s_selectAllSubRequestsVidStatementString);
  }
  m_selectAllSubRequestsVidStatement->setString(1, vid);
  
  std::vector<castor::repack::RepackSubRequest*>* result = NULL;
  result = internalgetSubRequests(m_selectAllSubRequestsVidStatement);
  
  /// we want to have to corresponding RepackRequest for each RepackSubRequest
  for (int i=0; i<result->size(); i++ ){
    svcs()->fillObj(&ad,result->at(i),OBJ_RepackRequest);
  }
  svcs()->commit(&ad);
  return result;
}




//------------------------------------------------------------------------------
// private : getSubRequests 
//------------------------------------------------------------------------------
std::vector<castor::repack::RepackSubRequest*>* 
                          castor::repack::DatabaseHelper::internalgetSubRequests
                                           (castor::db::IDbStatement* statement) 
                                             throw (castor::exception::Exception)
{

  std::vector<castor::repack::RepackSubRequest*>* result 
                         = new std::vector<castor::repack::RepackSubRequest*>();
  castor::db::IDbResultSet *rset = NULL;
  /// execute the Statement and get the
  try { 
     rset = statement->executeQuery();
     svcs()->commit(&ad);
    while ( rset->next() ){   /** now add the subrequests to vector */
      u_signed64 id = (u_signed64)rset->getDouble(1);
      RepackSubRequest* tape = getSubRequest(id);
      result->push_back(tape);
    }
    delete rset;

  }catch (castor::exception::SQLError e) {
    delete rset;  
    cnvSvc()->rollback();
    castor::exception::Internal ex;
    ex.getMessage()
    << "DatabaseHelper::getSubRequests():"
    << "Unable to get RepackSubRequests" 
    << std::endl << e.getMessage().str();
    throw ex;
  }
  return result;
}










//------------------------------------------------------------------------------
// private : getTapeCopy 
//------------------------------------------------------------------------------
castor::repack::RepackSegment* 
                    castor::repack::DatabaseHelper::getTapeCopy(
                                          castor::repack::RepackSegment* lookup
					   )
                    throw (castor::exception::Exception)
{
  castor::repack::RepackSegment* result = NULL;

  if ( lookup == NULL ) {
    castor::exception::Internal ex;
    ex.getMessage() << "passed Parameter is NULL" << std::endl;
    throw ex;
  }
  
  if ( m_selectExistingSegmentsStatement == NULL ) {
    m_selectExistingSegmentsStatement = 
                      createStatement(s_selectExistingSegmentsStatementString);
  }
  
  try {
    m_selectExistingSegmentsStatement->setDouble(1, lookup->fileid() );
    castor::db::IDbResultSet *rset = 
                               m_selectExistingSegmentsStatement->executeQuery();
		
    if ( rset->next() ){
      result = new castor::repack::RepackSegment();
      result->setFileid((u_signed64)rset->getDouble(1));
      result->setCompression(rset->getInt(2));
      result->setSegsize((u_signed64)rset->getDouble(3));
      result->setFilesec(rset->getInt(4));
      result->setId((u_signed64)rset->getDouble(5));
      result->setCopyno(rset->getInt(6));
      /* get the SubRequest and Request */
      cnvSvc()->fillObj(&ad,result,OBJ_RepackSubRequest,true);
      cnvSvc()->fillObj(&ad, result->vid(), OBJ_RepackRequest, true);
    }
    delete rset;
  }catch (castor::exception::SQLError e) {
		castor::exception::Internal ex;
		ex.getMessage()
		<< "DatabaseHelper::getTapeCopy():"
		<< "Unable to get all RepackSubRequests" 
		<< std::endl << e.getMessage().str();
		throw ex;
	}
  return result;
}


//------------------------------------------------------------------------------
// private : getLastTapeInformation 
//------------------------------------------------------------------------------
castor::repack::RepackRequest* 
 castor::repack::DatabaseHelper::getLastTapeInformation(std::string vidName)
                    throw (castor::exception::Exception)
{

  castor::repack::RepackSubRequest* result=NULL;

  if (vidName.length() ==0) {
    castor::exception::Internal ex;
    ex.getMessage() << "passed Parameter is NULL" << std::endl;
    throw ex;
  }
  
  if ( m_selectLastSegmentsSituationStatement == NULL ) {
    m_selectLastSegmentsSituationStatement = 
                      createStatement(s_selectLastSegmentsSituationStatementString);
  }
  
  try {
    m_selectLastSegmentsSituationStatement->setString(1,vidName);
    castor::db::IDbResultSet *rset = m_selectLastSegmentsSituationStatement->executeQuery();

    if ( rset->next() ){
      /// get the request we found
      u_signed64 id = (u_signed64)rset->getDouble(1);
      result = getSubRequest(id);
      svcs()->fillObj(&ad,result,OBJ_RepackRequest);
      svcs()->fillObj(&ad,result,OBJ_RepackSegment);
    }
    delete rset;
  }catch (castor::exception::SQLError e) {
		castor::exception::Internal ex;
		ex.getMessage()
		<< "DatabaseHelper::getLastTapeInformation():"
		<< "Unable to get all RepackSubRequests" 
		<< std::endl << e.getMessage().str();
		throw ex;
	}
  return  (result==NULL)? NULL:(result->requestID());
}
