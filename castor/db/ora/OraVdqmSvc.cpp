/******************************************************************************
 *                      OraVdqmSvc.cpp
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
 * @(#)RCSfile: OraVdqmSvc.cpp  Revision: 1.0  Release Date: May 31, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/Constants.hpp"
#include "castor/IClient.hpp"
#include "castor/db/ora/OraStagerSvc.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Busy.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/NotSupported.hpp"

#include "castor/vdqm/TapeRequest.hpp"
#include "castor/vdqm/TapeServer.hpp"
#include "castor/vdqm/ExtendedDeviceGroup.hpp"

#include "OraVdqmSvc.hpp"

#include "occi.h"
#include <Cuuid.h>
#include <string>
#include <sstream>
#include <vector>
#include <Cns_api.h>
#include <vmgr_api.h>
#include <Ctape_api.h>
#include <serrno.h>

#define NS_SEGMENT_NOTOK (' ')


 
// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraVdqmSvc> s_factoryOraVdqmSvc;
const castor::IFactory<castor::IService>&
OraVdqmSvcFactory = s_factoryOraVdqmSvc;


//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for function checkExtDevGroup
const std::string castor::db::ora::OraVdqmSvc::s_checkExtDevGroupStatementString =
  "SELECT id FROM Tape WHERE status = :1";

/// SQL statement for function getTapeServer
const std::string castor::db::ora::OraVdqmSvc::s_selectTapeServerStatementString =
  "SELECT id FROM Tape WHERE status = :1";


/// SQL statement for function checkTapeRequest
const std::string castor::db::ora::OraVdqmSvc::s_checkTapeRequestStatement1String =
  "SELECT tapeDrive FROM TapeRequest WHERE id = :1";

/// SQL statement for function checkTapeRequest
const std::string castor::db::ora::OraVdqmSvc::s_checkTapeRequestStatement2String =
  "SELECT count(*) FROM TapeRequest WHERE id <= :1";
  

/// SQL statement for function getFreeTapeDrive
const std::string castor::db::ora::OraVdqmSvc::s_selectFreeTapeDriveStatementString =
  "SELECT id FROM Tape WHERE status = :1";


// -----------------------------------------------------------------------
// OraVdqmSvc
// -----------------------------------------------------------------------
castor::db::ora::OraVdqmSvc::OraVdqmSvc(const std::string name) :
  BaseSvc(name), OraBaseObj(0),
  m_checkExtDevGroupStatement(0),
  m_selectTapeServerStatement(0),
  m_checkTapeRequestStatement1(0),
  m_checkTapeRequestStatement2(0),
  m_selectFreeTapeDriveStatement(0) {
}


// -----------------------------------------------------------------------
// ~OraVdqmSvc
// -----------------------------------------------------------------------
castor::db::ora::OraVdqmSvc::~OraVdqmSvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraVdqmSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraVdqmSvc::ID() {
  return castor::SVC_ORAVDQMSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraVdqmSvc::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_checkExtDevGroupStatement);
    deleteStatement(m_selectTapeServerStatement);
    deleteStatement(m_checkTapeRequestStatement1);
    deleteStatement(m_checkTapeRequestStatement2);
    deleteStatement(m_selectFreeTapeDriveStatement);
  } catch (oracle::occi::SQLException e) {};
  
  // Now reset all pointers to 0
  m_checkExtDevGroupStatement = 0;
  m_selectTapeServerStatement = 0;
  m_checkTapeRequestStatement1 = 0;
  m_checkTapeRequestStatement2 = 0;
  m_selectFreeTapeDriveStatement = 0;
}

// -----------------------------------------------------------------------
// checkExtDevGroup
// -----------------------------------------------------------------------
bool castor::db::ora::OraVdqmSvc::checkExtDevGroup(
	const castor::vdqm::ExtendedDeviceGroup *extDevGrp)
  throw (castor::exception::Exception) {
  	return true;
}


// -----------------------------------------------------------------------
// selectTapeServer
// -----------------------------------------------------------------------
castor::vdqm::TapeServer* 
	castor::db::ora::OraVdqmSvc::selectTapeServer(const std::string serverName)
  throw (castor::exception::Exception) {
  	 castor::vdqm::TapeServer* tapeServer = NULL;
  	 return tapeServer;
}


// -----------------------------------------------------------------------
// checkTapeRequest
// -----------------------------------------------------------------------
int castor::db::ora::OraVdqmSvc::checkTapeRequest(
	const castor::vdqm::TapeRequest *tapeRequest) 
	throw (castor::exception::Exception) {
  	
  try {
    // Check whether the statements are ok
    if (0 == m_checkTapeRequestStatement1) {
      m_checkTapeRequestStatement1 =
        createStatement(s_checkTapeRequestStatement1String);
    }
    
    if (0 == m_checkTapeRequestStatement2) {
      m_checkTapeRequestStatement2 =
        createStatement(s_checkTapeRequestStatement2String);
    }
    
    // execute the statement1 and see whether we found something   
    m_checkTapeRequestStatement1->setDouble(1, tapeRequest->id());
    oracle::occi::ResultSet *rset = m_checkTapeRequestStatement1->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return -1
      m_checkTapeRequestStatement1->closeResultSet(rset);
      return -1;
    }
    else if ((int)rset->getDouble(1)) {
    	//The request is already beeing handled from  a tape Drive
    	// In this case we return the queue position 0
    	return 0;
    }
    
    // If we come up to this line, the job is not handled till now, but it exists
    // execute the statement2 and see whether we found something
    m_checkTapeRequestStatement2->setDouble(1, tapeRequest->id());
    rset = m_checkTapeRequestStatement2->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      // Nothing found, return -1
      //Normally, the second statement should always find something!
      m_checkTapeRequestStatement2->closeResultSet(rset);
      return -1;
    }
    
    // Return the TapeRequest queue position
    return (int)rset->getDouble(1);
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in checkTapeRequest."
      << std::endl << e.what();
    throw ex;
  }
  // We should never reach this point.
}

// -----------------------------------------------------------------------
// selectFreeTapeDrive
// -----------------------------------------------------------------------
castor::vdqm::TapeDrive* 
	castor::db::ora::OraVdqmSvc::selectFreeTapeDrive
	(const castor::vdqm::ExtendedDeviceGroup *extDevGrp)
  throw (castor::exception::Exception) {
 return NULL; 	
}
