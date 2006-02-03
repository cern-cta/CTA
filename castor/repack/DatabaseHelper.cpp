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
  "SELECT q.id FROM RepackSubRequest q WHERE q.status = 1001 AND ROWNUM < 2 FOR UPDATE";
const std::string castor::repack::DatabaseHelper::m_selectCheckStatementString =
  "SELECT q.vid FROM RepackSubRequest q WHERE q.vid= :1";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::repack::DatabaseHelper::DatabaseHelper() throw() : 
	DbBaseObj(NULL),
	m_selectToDoStatement(NULL) {
  try {
    
  	ad.setCnvSvcName("DbCnvSvc");
	ad.setCnvSvcType(castor::SVC_DBCNV);
	
	
    // create oracle and streaming conversion service
    // so that it is not deleted and recreated all the time
    //castor::IService *svc =
	//    svcs()->service("DbCnvSvc", castor::SVC_DBCNV);
    //if (0 == svc) {
      // "Could not get Conversion Service for Database" message
      //castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2);
    //}
  } catch(castor::exception::Exception e) {
    // "Exception caught : server is stopping" message
    castor::dlf::Param params[] =
      {castor::dlf::Param("Standard Message", sstrerror(e.code())),
       castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 2, params);
  }

};




//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::repack::DatabaseHelper::~DatabaseHelper() throw() {
	reset();
};




//------------------------------------------------------------------------------
// resets the statements
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
   delete m_selectToDoStatement;
  } catch (castor::exception::SQLError ignored) {};
  // Now reset all pointers to 0
  m_selectToDoStatement = NULL;
}





//------------------------------------------------------------------------------
// store_Request 
//------------------------------------------------------------------------------
void castor::repack::DatabaseHelper::storeRequest(castor::repack::RepackRequest* rreq)
						throw (castor::exception::Internal)
{
	stage_trace(2,"Storing Request in DB" );
	
	
		/* Check, if one of the tapes is already in DB */
		for ( int i=0; i<rreq->subRequest().size(); i++ ){
			if ( checkForVid( rreq->subRequest().at(i)->vid() )){
				castor::exception::Internal ex;
				ex.getMessage() << " VID : " << rreq->subRequest().at(i)->vid() 
								<< " already in Database ! Aborting.."
								<< std::endl;
				throw ex;
			}
		}
	
	try {
		
	    // Store files for RepackRequest
		if ( rreq != NULL ) {
		  svcs()->createRep(&ad, rreq, false);
		  svcs()->fillRep(&ad, rreq, OBJ_RepackSubRequest, false);
		  for (int i = 0; i < rreq->subRequest().size(); i++)
			  svcs()->fillRep(&ad, rreq->subRequest().at(i), OBJ_RepackSegment, false);
		}
	
		svcs()->commit(&ad);
		// "Request stored in DB" message
		castor::dlf::Param params[] =
		  {castor::dlf::Param("ID", rreq->id())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 12, 1, params);
	
	  } catch (castor::exception::Exception e) {
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
// requestToDo
//------------------------------------------------------------------------------
bool castor::repack::DatabaseHelper::checkForVid(std::string vid) throw()
{
	bool result = false;
	stage_trace(2,"Checking, if %s is already in DB ",vid.c_str());
	try {
		// Check whether the statements are ok
		if ( m_selectCheckStatement == NULL ) {
			m_selectCheckStatement = createStatement(m_selectCheckStatementString);
		}
		m_selectCheckStatement->setString(1, vid);
		castor::db::IDbResultSet *rset = m_selectCheckStatement->executeQuery();

		if ( rset->next() )	/* Something found : return true*/
			result = true;	

		delete rset;
	}catch (castor::exception::SQLError e){
		delete m_selectCheckStatement;
		castor::exception::Internal ex;
		ex.getMessage() 	<< "Unable to get a RepackSubRequest from DB " 
							<< std::endl << e.getMessage();
		throw ex;
	}
	
	return result;
}





void castor::repack::DatabaseHelper::updateRep(castor::IObject* obj) throw ()
{
	try {
		// Stores it into the data base
		svcs()->updateRep(&ad, obj, true);
		svcs()->commit(&ad);
	 } catch (castor::exception::Exception e) {
       svcs()->rollback(&ad);
	}
}




//------------------------------------------------------------------------------
// requestToDo
//------------------------------------------------------------------------------
castor::repack::RepackSubRequest* castor::repack::DatabaseHelper::requestToDo() 
					throw (castor::exception::Exception)
{
	// Check whether the statements are ok
	if ( m_selectToDoStatement == NULL ) {
		m_selectToDoStatement = createStatement(m_selectToDoStatementString);
	}	
	// Execute statement and get result
	try {
		castor::db::IDbResultSet *rset = m_selectToDoStatement->executeQuery();
		if ( !rset->next() ){	/* no Job to be done */
			delete rset;
			return NULL;	
		}
		else	/* jepp, we found a job, happy about that, 
				we try to return the RepackRequest */
		{
			// get the request we found, which the ID we got from DB
			u_signed64 id = (u_signed64)rset->getDouble(1);
			castor::IObject* obj = cnvSvc()->getObjFromId( id );
			if (obj == NULL) {
				castor::exception::Internal ex;
				ex.getMessage()
						<< "castor::repack::DatabaseHelper::requestToDo(..) :"
						   " could not retrieve object for id "	<< id;
				throw ex;
		    	}
			/* Now, lets see if the right Object was retrieved */ 
		    RepackSubRequest* result = dynamic_cast<RepackSubRequest*>(obj);

		    if ( result == NULL ){ /* oops */
		    	castor::exception::Internal ex;
				ex.getMessage()
						<< "castor::repack::DatabaseHelper::requestToDo(..) :"
						   " object retrieved for id : " << id << "was a "
						   << castor::ObjectsIdStrings[obj->type()];
				throw ex;
				delete obj;	/* never forget to delete everything used */
				delete rset;
				return NULL;
		    }
		    svcs()->fillObj(&ad,result,OBJ_RepackSegment);
		    return result;
		}
	  	delete rset;
	  	
	  	
	} catch (castor::exception::SQLError e) {
		castor::exception::Internal ex;
		ex.getMessage()
		<< "Unable to get a RepackSubRequest from DB " << std::endl << e.getMessage();
		throw ex;
	}
    
}
