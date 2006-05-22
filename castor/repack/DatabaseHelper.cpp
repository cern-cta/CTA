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
/// SQL statement for ToDo
const std::string castor::repack::DatabaseHelper::m_selectToDoStatementString =
  "SELECT q.id FROM RepackSubRequest q WHERE q.status = 1001 AND ROWNUM < 2 "; // status READYFORSTAGING AND READYFORMIG
const std::string castor::repack::DatabaseHelper::m_selectCheckStatementString =
  "SELECT q.id FROM RepackSubRequest q WHERE q.vid= :1";
const std::string castor::repack::DatabaseHelper::m_selectCheckSubRequestStatementString =
  "SELECT q.id FROM RepackSubRequest q WHERE q.status= :1 AND ROWNUM < 2";
const std::string castor::repack::DatabaseHelper::m_selectAllSubRequestsStatementString =
  "SELECT id FROM RepackSubRequest";
const std::string castor::repack::DatabaseHelper::m_selectExistingSegmentsStatementString =
  "SELECT fileid,compression,segsize,filesec,id,vid FROM RepackSegment,RepackSubrequest where RepackSegment.fileid = :1 and RepackSegment.copyno = :2";
const std::string castor::repack::DatabaseHelper::m_isStoredStatementString =
  "SELECT id FROM RepackSubRequest where vid=:1";








//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::repack::DatabaseHelper::DatabaseHelper() : 
	DbBaseObj(NULL),
	 m_selectToDoStatement(NULL),
    m_selectCheckStatement(NULL),
    m_selectCheckSubRequestStatement(NULL),
    m_selectAllSubRequestsStatement(NULL),
    m_selectExistingSegmentsStatement(NULL),
    m_isStoredStatement(NULL) {
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
};




//------------------------------------------------------------------------------
// resets the statements
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
   if ( m_selectToDoStatement ) delete m_selectToDoStatement;
   if ( m_selectCheckStatement ) delete m_selectCheckStatement;
   if ( m_selectCheckSubRequestStatement ) delete m_selectCheckSubRequestStatement;
   if ( m_selectAllSubRequestsStatement ) delete m_selectAllSubRequestsStatement;
   if ( m_selectExistingSegmentsStatement ) delete m_selectExistingSegmentsStatement;
   if ( m_isStoredStatement ) delete m_isStoredStatement;
  } catch (castor::exception::SQLError ignored) {};
  // Now reset all pointers to 0
   m_selectToDoStatement = NULL;
   m_selectCheckStatement = NULL;
   m_selectCheckSubRequestStatement = NULL;
   m_selectAllSubRequestsStatement = NULL;
   m_selectExistingSegmentsStatement = NULL;
   m_isStoredStatement = NULL;
}





//------------------------------------------------------------------------------
// store_Request 
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::storeRequest(castor::repack::RepackRequest* rreq)
						throw (castor::exception::Internal)
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
	    			<< std::endl << e.getMessage();
	    svcs()->rollback(&ad);
	    castor::dlf::Param params[] =
      		{castor::dlf::Param("Standard Message", sstrerror(e.code())),
		 castor::dlf::Param("Precise Message", e.getMessage().str())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 13, 2, params);
	    return;
	  }
	
	return;
}




//------------------------------------------------------------------------------
// is_stored
//------------------------------------------------------------------------------
bool castor::repack::DatabaseHelper::is_stored(std::string vid) 
						throw (castor::exception::Internal)
{
	bool result = false;
	
	
	try {
		// Check whether the statements are ok
		if ( m_isStoredStatement == NULL ) {
			m_isStoredStatement = 
					createStatement(m_isStoredStatementString);
		}
		m_isStoredStatement->setString(1, vid);
		castor::db::IDbResultSet *rset = 
				m_isStoredStatement->executeQuery();

		if ( rset->next() )	/* Something found*/
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
		ex.getMessage() << "DatabaseHelper::Unable to execute check" 
				<< std::endl << e.getMessage();
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
						std::string vid) 
						throw (castor::exception::Internal)
{
	RepackSubRequest* result = NULL;
	try {
		// Check whether the statements are ok
		if ( m_selectCheckStatement == NULL ) {
			m_selectCheckStatement = 
				createStatement(m_selectCheckStatementString);
		}
		m_selectCheckStatement->setString(1, vid);
		castor::db::IDbResultSet *rset = 
				m_selectCheckStatement->executeQuery();

		if ( rset->next() )	/* Something found*/
		{
			u_signed64 id = (u_signed64)rset->getDouble(1);
			result = getSubRequest(id);
			
			svcs()->fillObj(&ad,result,OBJ_RepackSegment);
		}
		delete rset;
	
	}catch (castor::exception::SQLError e){
		delete m_selectCheckStatement;
		castor::exception::Internal ex;
		ex.getMessage() << "Unable to get a RepackSubRequest from DB: " 
				<< std::endl << e.getMessage();
		throw ex;
	}	
	
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
				throw (castor::exception::Internal)
{
	try {
		// stores it into the database
		svcs()->updateRep(&ad, obj, false);
    
    /** we add the segments to the repacksubrequest */
    if (fill) {
        svcs()->fillRep(&ad, obj, OBJ_RepackSegment, false);
    }
    else {
        for (int i = 0; i < obj->segment().size(); i++)
          svcs()->updateRep(&ad, obj->segment().at(i), false);
		}
		svcs()->commit(&ad);
		
	} catch (castor::exception::Exception e) {
	 	 svcs()->rollback(&ad);
	    	castor::dlf::Param params[] =
      		{castor::dlf::Param("Standard Message", sstrerror(e.code())),
      	 	 castor::dlf::Param("Precise Message", e.getMessage().str())};
	 	castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 27, 2, params);
	 	stage_trace(3,"DatabaseHelper: Error updating DB");
        	return;
	}
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 30, 0, NULL);
}




//------------------------------------------------------------------------------
// requestToDo
//------------------------------------------------------------------------------
castor::repack::RepackSubRequest* castor::repack::DatabaseHelper::requestToDo() 
					throw (castor::exception::Internal)
{
	RepackSubRequest* result = NULL;
	result = checkSubRequestStatus(SUBREQUEST_READYFORSTAGING);
	
	return result;
}


//------------------------------------------------------------------------------
// internal : getSubRequest
//------------------------------------------------------------------------------
castor::repack::RepackSubRequest* 
			castor::repack::DatabaseHelper::getSubRequest(
						u_signed64 sub_id)
						throw (castor::exception::Internal)   
{
	castor::IObject* obj = cnvSvc()->getObjFromId( sub_id );
	if (obj == NULL) {
		castor::exception::Internal ex;
		ex.getMessage()
			<< "DatabaseHelper::getSubRequest(..) :"
			   " could not retrieve object for id "	<< sub_id;
		throw ex;
	}
	/* Now, lets see if the right Object was retrieved */ 
	RepackSubRequest* result = dynamic_cast<RepackSubRequest*>(obj);
	
	if ( result == NULL ){ /* oops */
		castor::exception::Internal ex;
		ex.getMessage()
			<< "DatabaseHelper::getSubRequest(..) :"
			<< " object retrieved for id : " 
			<< sub_id << "was a "
			<< castor::ObjectsIdStrings[obj->type()];
		delete obj;	/* never forget to delete everything used */
		throw ex;
	}
  return result;
}



//------------------------------------------------------------------------------
// private : checkSubRequestStatus
//------------------------------------------------------------------------------
castor::repack::RepackSubRequest* 
		castor::repack::DatabaseHelper::checkSubRequestStatus(int status)
					throw (castor::exception::Internal)
{
	RepackSubRequest* result = NULL;

	// Check whether the statements are ok
	if ( m_selectCheckSubRequestStatement == NULL ) {
		m_selectCheckSubRequestStatement = createStatement(m_selectCheckSubRequestStatementString);
	}
		
	try {	
		m_selectCheckSubRequestStatement->setInt(1, status);
		castor::db::IDbResultSet *rset = 
				m_selectCheckSubRequestStatement->executeQuery();
	
		if ( !rset->next() ){	/* no Job to be done */
			delete rset;
			return NULL;	
		}
		else
		{
			// get the request we found, which the ID we got from DB
				u_signed64 id = (u_signed64)rset->getDouble(1);
				delete rset;
				result = getSubRequest(id);
				svcs()->fillObj(&ad,result,OBJ_RepackSegment);
		}
	}catch (castor::exception::SQLError e) {
		delete result;
		castor::exception::Internal ex;
		ex.getMessage() 
		<< "DatabaseHelper::checkSubRequestStatus(..): "
		<< "Unable to get the status from a RepackSubRequest" 
		<< std::endl << e.getMessage();
		throw ex;
	}
					
	delete m_selectCheckSubRequestStatement;
	m_selectCheckSubRequestStatement = NULL;
	
	return result;	// careful! This Object is not complete ! 
					// The Segments are missing !!!
}



//------------------------------------------------------------------------------
// private : remove
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::remove(castor::IObject* obj) 
						throw (castor::exception::Internal)
{
	try {
		if ( obj->type() == OBJ_RepackSubRequest ){
			RepackSubRequest* sreq = dynamic_cast<RepackSubRequest*>(obj);
			for (int i=0; i<sreq->segment().size(); i++ ){
				svcs()->deleteRep(&ad,sreq->segment().at(i), false);
			}
		}
		svcs()->deleteRep(&ad,obj, false);
		svcs()->commit(&ad);
	
	}catch (castor::exception::SQLError e){
		svcs()->rollback(&ad);
		castor::exception::Internal ex;
		ex.getMessage()
		<< "DatabaseHelper::remove():"
		<< "Unable to remove Object with Type "
		<< obj->type() << "from DB" 
		<< std::endl << e.getMessage();
		throw ex;
	}
}



//------------------------------------------------------------------------------
// private : getAllSubRequests
//------------------------------------------------------------------------------
std::vector<castor::repack::RepackSubRequest*>*
	castor::repack::DatabaseHelper::getAllSubRequests()
						throw (castor::exception::Internal)
{
	std::vector<castor::repack::RepackSubRequest*>* result 
			= new std::vector<castor::repack::RepackSubRequest*>();
   
	if ( m_selectAllSubRequestsStatement == NULL ) {
		m_selectAllSubRequestsStatement = 
			createStatement(m_selectAllSubRequestsStatementString);
	}

	try {	
		castor::db::IDbResultSet *rset = 
			m_selectAllSubRequestsStatement->executeQuery();
		
		while ( rset->next() ){		/** now add the subrequests to vector */
			u_signed64 id = (u_signed64)rset->getDouble(1);
			RepackSubRequest* tape = getSubRequest(id);
			result->push_back(tape);
		}
		delete rset;
	
	}catch (castor::exception::SQLError e) {
		castor::exception::Internal ex;
		ex.getMessage()
		<< "DatabaseHelper::getAllSubRequests():"
		<< "Unable to get all RepackSubRequests" 
		<< std::endl << e.getMessage();
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
                    throw (castor::exception::Internal)
{

  castor::repack::RepackSegment* result = NULL;

  if ( m_selectExistingSegmentsStatement == NULL ) {
    m_selectExistingSegmentsStatement = createStatement(m_selectExistingSegmentsStatementString);
  }
  
  try {
    m_selectExistingSegmentsStatement->setDouble(1, lookup->fileid() );
    m_selectExistingSegmentsStatement->setDouble(1, lookup->copyno() );
    castor::db::IDbResultSet *rset = 
      m_selectAllSubRequestsStatement->executeQuery();
		
    if ( rset->next() ){
      result = new castor::repack::RepackSegment();
      result->setFileid((u_signed64)rset->getDouble(1));
      result->setCompression(rset->getInt(2));
      result->setSegsize((u_signed64)rset->getDouble(3));
      result->setFilesec(rset->getInt(4));
      result->setId((u_signed64)rset->getDouble(5));
      RepackSubRequest* tmp = new RepackSubRequest();
      tmp->setId((u_signed64)rset->getDouble(6));
      result->setVid(tmp);
    }
    delete rset;
  }catch (castor::exception::SQLError e) {
		castor::exception::Internal ex;
		ex.getMessage()
		<< "DatabaseHelper::getTapeCopy():"
		<< "Unable to get all RepackSubRequests" 
		<< std::endl << e.getMessage();
		throw ex;
	}
  return result;
}
