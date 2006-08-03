/******************************************************************************
 *                      OraCleanSvc.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: OraCleanSvc.cpp,v $ $Author: gtaur $
 *
 * @author Giulia Taurelli
 *****************************************************************************/


// Include Files

#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/IClient.hpp"
#include "castor/db/ora/OraCleanSvc.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Busy.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/NotSupported.hpp"
#include "castor/BaseAddress.hpp"
#include "occi.h"
#include <Cuuid.h>
#include <string>
#include <sstream>
#include <vector>
#include <Cns_api.h>
#include <serrno.h>

#include "h/stager_client_api_common.h" 



// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraCleanSvc>* s_factoryOraCleanSvc =
  new castor::SvcFactory<castor::db::ora::OraCleanSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------


/// SQL statement for function removeOutOfDateRequests
const std::string castor::db::ora::OraCleanSvc::s_removeOutOfDateRequestsString=
    "BEGIN deleteOutOfDateRequests(:1);END;";

/// SQL statement for removeArchivedRequests function
const std::string castor::db::ora::OraCleanSvc::s_removeArchivedRequestsString=
    "BEGIN deleteArchivedRequests(:1); END;";



// -----------------------------------------------------------------------
// OraCleanSvc
// -----------------------------------------------------------------------

castor::db::ora::OraCleanSvc::OraCleanSvc(const std::string name) :
  OraCommonSvc(name),
  m_removeOutOfDateRequestsStatement(0),
  m_removeArchivedRequestsStatement(0){


}

// -----------------------------------------------------------------------
// ~OraTapeSvc
// -----------------------------------------------------------------------
castor::db::ora::OraCleanSvc::~OraCleanSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCleanSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraCleanSvc::ID() {
  return castor::SVC_ORACLEANSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraCleanSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    deleteStatement(m_removeOutOfDateRequestsStatement);
    deleteStatement(m_removeArchivedRequestsStatement);
  } catch (oracle::occi::SQLException e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("message", e.getMessage())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 7, 1, params);
     };

  // Now reset all pointers to 0

  m_removeOutOfDateRequestsStatement = 0;
  m_removeArchivedRequestsStatement = 0;


}

// -----------------------------------------------------------------------
// removeOutOfDateRequests
// -----------------------------------------------------------------------
 
int castor::db::ora::OraCleanSvc::removeOutOfDateRequests(int numDays)
        throw (castor::exception::Exception){
    try{
       if (0 == m_removeOutOfDateRequestsStatement) {
           m_removeOutOfDateRequestsStatement 
              =createStatement(s_removeOutOfDateRequestsString);
           m_removeOutOfDateRequestsStatement->setAutoCommit(true);
       }

    // execute the statement
   
      m_removeOutOfDateRequestsStatement->setInt(1, numDays*24*60*60);
    
      unsigned int nb = m_removeOutOfDateRequestsStatement->executeUpdate();
       
       if (nb == 0) {
         rollback();
         castor::exception::NoEntry e;
         e.getMessage() << "deleteOutOfDateRequests function not found";
         throw e;
       }
       
       castor::dlf::Param logParams[] =
	 {castor::dlf::Param("message", "Removed out of date request.")};
       castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 2, 1, logParams);
       return 0;

    }catch (oracle::occi::SQLException e) {

            handleException(e);
            castor::exception::Internal ex;

	    castor::dlf::Param params[] =
	      {castor::dlf::Param("message",e.getMessage())};
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 1, params);
	    throw ex;
	    return -1;
    }
}


// -----------------------------------------------------------------------
// removeArchivedRequests
// -----------------------------------------------------------------------


int  castor::db::ora::OraCleanSvc::removeArchivedRequests(int hours)
        throw (castor::exception::Exception){
    try{
       if (0 == m_removeArchivedRequestsStatement) {
           m_removeArchivedRequestsStatement 
              =createStatement(s_removeArchivedRequestsString);
           m_removeArchivedRequestsStatement->setAutoCommit(true);
       }

    // execute the statement
   
       m_removeArchivedRequestsStatement->setInt(1, hours*60*60); 
       unsigned int nb = m_removeArchivedRequestsStatement->executeUpdate();

       if (nb == 0) {
         rollback();
         castor::exception::NoEntry e;
         e.getMessage() << "deleteArchivedRequests function not found";
         throw e;
       }
       castor::dlf::Param logParams[] =
         {castor::dlf::Param("message", "Removed archived request.")};
       castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 3, 1, logParams);
       return 0;
    }catch (oracle::occi::SQLException e) {

      handleException(e);
      castor::exception::Internal ex;

      castor::dlf::Param params[] =
	{castor::dlf::Param(castor::dlf::Param("message",e.getMessage()))};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 6, 1, params);
      throw ex;
      return -1;
    }

}



 

