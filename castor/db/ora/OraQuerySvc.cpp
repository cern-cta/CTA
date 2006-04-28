/******************************************************************************
 *                      OraQuerySvc.cpp
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
 * @(#)$RCSfile: OraQuerySvc.cpp,v $ $Revision: 1.34 $ $Release$ $Date: 2006/04/28 07:20:23 $ $Author: felixehm $
 *
 * Implementation of the IQuerySvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/IFactory.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/db/ora/OraQuerySvc.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
#include "castor/stager/Request.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/TooBig.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/query/DiskPoolQueryResponse.hpp"
#include "castor/query/DiskServerDescription.hpp"
#include "castor/query/FileSystemDescription.hpp"
#include <common.h>

#include <sstream>
#include <string>
#include <stdlib.h>
#include <list>

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraQuerySvc>* s_factoryOraQuerySvc =
  new castor::SvcFactory<castor::db::ora::OraQuerySvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

const std::string castor::db::ora::OraQuerySvc::s_diskCopies4FileStatementString =
  "BEGIN fileIdStageQuery(:1, :2, :3, :4); END;";

const std::string castor::db::ora::OraQuerySvc::s_diskCopies4FileNameStatementString =
  "BEGIN fileNameStageQuery(:1, :2, :3, :4); END;";

const std::string castor::db::ora::OraQuerySvc::s_diskCopies4ReqIdStatementString =
  "BEGIN reqIdStageQuery(:1, :2, :3, :4); END;";

const std::string castor::db::ora::OraQuerySvc::s_diskCopies4UserTagStatementString =
  "BEGIN userTagStageQuery(:1, :2, :3, :4); END;";

const std::string castor::db::ora::OraQuerySvc::s_diskCopies4ReqIdLastRecallsStatementString =
  "BEGIN reqIdLastRecallsStageQuery(:1, :2, :3, :4); END;";

const std::string castor::db::ora::OraQuerySvc::s_diskCopies4UserTagLastRecallsStatementString =
  "BEGIN userTagLastRecallsStageQuery(:1, :2, :3, :4); END;";

const std::string castor::db::ora::OraQuerySvc::s_requestToDoStatementString =
  "BEGIN :1 := 0; DELETE FROM newRequests WHERE type IN (33, 34, 41, 103) AND ROWNUM < 2 RETURNING id INTO :1; END;";

const std::string castor::db::ora::OraQuerySvc::s_describeDiskPoolsStatementString =
  "BEGIN describeDiskPools(:1, :2); END;";

const std::string castor::db::ora::OraQuerySvc::s_describeDiskPoolStatementString =
  "BEGIN describeDiskPool(:1, :2); END;";

// -----------------------------------------------------------------------
// OraQuerySvc
// -----------------------------------------------------------------------
castor::db::ora::OraQuerySvc::OraQuerySvc(const std::string name) :
  OraCommonSvc(name),
  m_diskCopies4FileNameStatement(0),
  m_diskCopies4FileStatement(0),
  m_diskCopies4ReqIdStatement(0),
  m_diskCopies4UserTagStatement(0),
  m_diskCopies4ReqIdLastRecallsStatement(0),
  m_diskCopies4UserTagLastRecallsStatement(0),
  m_requestToDoStatement(0),
  m_describeDiskPoolsStatement(0),
  m_describeDiskPoolStatement(0) {}

// -----------------------------------------------------------------------
// ~OraQuerySvc
// -----------------------------------------------------------------------
castor::db::ora::OraQuerySvc::~OraQuerySvc() throw() {
  reset();
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraQuerySvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::db::ora::OraQuerySvc::ID() {
  return castor::SVC_ORAQUERYSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraQuerySvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    deleteStatement(m_diskCopies4FileNameStatement);
    deleteStatement(m_diskCopies4FileStatement);
    deleteStatement(m_diskCopies4ReqIdStatement);
    deleteStatement(m_diskCopies4UserTagStatement);
    deleteStatement(m_diskCopies4ReqIdLastRecallsStatement);
    deleteStatement(m_diskCopies4UserTagLastRecallsStatement);
    deleteStatement(m_requestToDoStatement);
    deleteStatement(m_describeDiskPoolsStatement);
    deleteStatement(m_describeDiskPoolStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_diskCopies4FileNameStatement = 0;
  m_diskCopies4FileStatement = 0;
  m_diskCopies4ReqIdStatement = 0;
  m_diskCopies4UserTagStatement = 0;
  m_diskCopies4ReqIdLastRecallsStatement = 0;
  m_diskCopies4UserTagLastRecallsStatement = 0;
  m_requestToDoStatement = 0;
  m_describeDiskPoolsStatement = 0;
  m_describeDiskPoolStatement = 0;
}


// -----------------------------------------------------------------------
// gatherResults
// -----------------------------------------------------------------------
std::list<castor::stager::DiskCopyInfo*>*
castor::db::ora::OraQuerySvc::gatherResults(oracle::occi::ResultSet *rset)
  throw (oracle::occi::SQLException) {
    // Gather the results
    std::list<castor::stager::DiskCopyInfo*>* result =
      new std::list<castor::stager::DiskCopyInfo*>();
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      castor::stager::DiskCopyInfo* item =
        new castor::stager::DiskCopyInfo();
      item->setFileId((u_signed64)rset->getDouble(1));
      item->setNsHost(rset->getString(2));
      item->setId((u_signed64)rset->getDouble(3));
      item->setDiskCopyPath(rset->getString(4));
      item->setSize((u_signed64)rset->getDouble(5));
      item->setDiskCopyStatus((castor::stager::DiskCopyStatusCodes)
                              rset->getInt(6));
      item->setTapeCopyStatus((castor::stager::TapeCopyStatusCodes)0);
      item->setSegmentStatus((castor::stager::SegmentStatusCodes)0);
      item->setDiskServer(rset->getString(7));
      item->setMountPoint(rset->getString(8));
      item->setNbAccesses(rset->getInt(9));
      item->setLastKnownFileName(rset->getString(10));
      result->push_back(item);
    }
    return result;
}


// -----------------------------------------------------------------------
// diskCopies4FileName
// -----------------------------------------------------------------------
std::list<castor::stager::DiskCopyInfo*>*
castor::db::ora::OraQuerySvc::diskCopies4FileName
(std::string fileName, u_signed64 svcClassId)
  throw (castor::exception::Exception) {
  // default value for the maximal number of responses to give
  unsigned long maxNbResponses = 1000;
  try {
    // Check whether the statements are ok
    if (0 == m_diskCopies4FileNameStatement) {
      m_diskCopies4FileNameStatement =
        createStatement(s_diskCopies4FileNameStatementString);
      m_diskCopies4FileNameStatement->registerOutParam
        (4, oracle::occi::OCCICURSOR);
    }
    // get max number of responses for a file name query
    char* p;
    if ( (p = getenv ("FILEQUERY_MAXNBRESPONSES")) || (p = getconfent ("FILEQUERY", "MAXNBRESPONSES", 0)) ) {
      char* pend = p;
      unsigned long ip = strtoul(p, &pend, 0);
      if (*pend != 0) {
	clog() << ERROR
	       << "Invalid limit for number of responses to a file query : '"
	       << pend << "'. Should be a number." << std::endl;
      } else if (ip > 30000) {
	clog() << ERROR
	       << "Limit for number of responses to a file query is too high : "
	       << ip << " > 30000. Will use default : 1000." << std::endl;
      } else {
	maxNbResponses = ip;
      }
    }
    // execute the statement and see whether we found something
    m_diskCopies4FileNameStatement->setString(1, fileName);
    m_diskCopies4FileNameStatement->setDouble(2, svcClassId);
    m_diskCopies4FileNameStatement->setInt(3, maxNbResponses);
    unsigned int nb = m_diskCopies4FileNameStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "diskCopies4FileName : Unable to execute query.";
      throw ex;
    }
    oracle::occi::ResultSet *rset =
      m_diskCopies4FileNameStatement->getCursor(4);
    std::list<castor::stager::DiskCopyInfo*>* result = gatherResults(rset);
    m_diskCopies4FileNameStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    if (e.getErrorCode() == 20102) {
      // Too may files would have been returned, give up !
      castor::exception::TooBig ex;
      ex.getMessage() << "Too many matching files : more than "
		      << maxNbResponses;
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage() << "Error caught in diskCopies4FileName."
		      << std::endl << e.what();
      throw ex;
    }
  }
}



// -----------------------------------------------------------------------
// diskCopies4File
// -----------------------------------------------------------------------
std::list<castor::stager::DiskCopyInfo*>*
castor::db::ora::OraQuerySvc::diskCopies4File
(std::string fileId, std::string nsHost, u_signed64 svcClassId)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_diskCopies4FileStatement) {
      m_diskCopies4FileStatement =
        createStatement(s_diskCopies4FileStatementString);
      m_diskCopies4FileStatement->registerOutParam
        (4, oracle::occi::OCCICURSOR);
    }
    // execute the statement and see whether we found something
    m_diskCopies4FileStatement->setString(1, fileId);
    m_diskCopies4FileStatement->setString(2, nsHost);
    m_diskCopies4FileStatement->setDouble(3, svcClassId);
    unsigned int nb = m_diskCopies4FileStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "diskCopies4File : Unable to execute query.";
      throw ex;
    }
    oracle::occi::ResultSet *rset =
      m_diskCopies4FileStatement->getCursor(4);
    std::list<castor::stager::DiskCopyInfo*>* result = gatherResults(rset);
    m_diskCopies4FileStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in diskCopies4File."
                    << std::endl << e.what();
    throw ex;
  }
}



// -----------------------------------------------------------------------
// diskCopies4Request
// -----------------------------------------------------------------------
std::list<castor::stager::DiskCopyInfo*>*
castor::db::ora::OraQuerySvc::diskCopies4Request
(castor::stager::RequestQueryType reqType, std::string param, u_signed64 svcClassId)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if(0 == m_diskCopies4ReqIdStatement) {
      m_diskCopies4ReqIdStatement =
        createStatement(s_diskCopies4ReqIdStatementString);
      m_diskCopies4ReqIdStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_diskCopies4ReqIdStatement->registerOutParam
        (4, oracle::occi::OCCICURSOR);
      m_diskCopies4UserTagStatement =
        createStatement(s_diskCopies4UserTagStatementString);
      m_diskCopies4UserTagStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_diskCopies4UserTagStatement->registerOutParam
        (4, oracle::occi::OCCICURSOR);
      m_diskCopies4ReqIdLastRecallsStatement =
        createStatement(s_diskCopies4ReqIdLastRecallsStatementString);
      m_diskCopies4ReqIdLastRecallsStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_diskCopies4ReqIdLastRecallsStatement->registerOutParam
        (4, oracle::occi::OCCICURSOR);
      m_diskCopies4UserTagLastRecallsStatement =
        createStatement(s_diskCopies4UserTagLastRecallsStatementString);
      m_diskCopies4UserTagLastRecallsStatement->registerOutParam
        (3, oracle::occi::OCCIINT);
      m_diskCopies4UserTagLastRecallsStatement->registerOutParam
        (4, oracle::occi::OCCICURSOR);
    }
    // identify the statement to be used
    oracle::occi::Statement *requestStatement = 0;
    switch(reqType) {
      case castor::stager::REQUESTQUERYTYPE_REQID:
        requestStatement = m_diskCopies4ReqIdStatement;
        break;
      case castor::stager::REQUESTQUERYTYPE_USERTAG:
        requestStatement = m_diskCopies4UserTagStatement;
        break;
      case castor::stager::REQUESTQUERYTYPE_REQID_GETNEXT:
        requestStatement = m_diskCopies4ReqIdLastRecallsStatement;
        break;
      case castor::stager::REQUESTQUERYTYPE_USERTAG_GETNEXT:
        requestStatement = m_diskCopies4UserTagLastRecallsStatement;
        break;
      default:
        castor::exception::Internal ex;
        ex.getMessage()
          << "diskCopies4Request: request type" << reqType << " not allowed.";
        throw ex;
    }

    // execute the statement and see whether we found something
    requestStatement->setString(1, param);
    requestStatement->setDouble(2, svcClassId);
    unsigned int nb = requestStatement->executeUpdate();
    if(0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "diskCopies4Request: unable to execute query.";
      throw ex;
    }
    if(requestStatement->getInt(3) == 1)
      return 0;
    oracle::occi::ResultSet *rset = requestStatement->getCursor(4);
    std::list<castor::stager::DiskCopyInfo*>* result = gatherResults(rset);
    requestStatement->closeResultSet(rset);
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in diskCopies4Request."
                    << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// requestToDo
// -----------------------------------------------------------------------
castor::stager::Request*
castor::db::ora::OraQuerySvc::requestToDo()
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_requestToDoStatement) {
      m_requestToDoStatement =
        createStatement(s_requestToDoStatementString);
      m_requestToDoStatement->registerOutParam
        (1, oracle::occi::OCCIDOUBLE);
      m_requestToDoStatement->setAutoCommit(true);
    }
    // execute the statement
    m_requestToDoStatement->executeUpdate();
    // see whether we've found something
    u_signed64 id = (u_signed64)m_requestToDoStatement->getDouble(1);
    if (0 == id) {
      // Found no Request to handle
      return 0;
    }
    // Create result
    IObject* obj = cnvSvc()->getObjFromId(id);
    if (0 == obj) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "requestToDo : could not retrieve object for id "
        << id;
      throw ex;
    }
    castor::stager::Request* result =
      dynamic_cast<castor::stager::Request*>(obj);
    if (0 == result) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "requestToDo : object retrieved for id "
        << id << " was a "
        << castor::ObjectsIdStrings[obj->type()]
        << " while a Request was expected.";
      delete obj;
      throw ex;
    }
    // return
    return result;
  } catch (oracle::occi::SQLException e) {
    rollback();
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in requestToDo."
      << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// describeDiskPools
// -----------------------------------------------------------------------
std::vector<castor::query::DiskPoolQueryResponse*>*
castor::db::ora::OraQuerySvc::describeDiskPools (std::string svcClass)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if(0 == m_describeDiskPoolsStatement) {
      m_describeDiskPoolsStatement =
        createStatement(s_describeDiskPoolsStatementString);
      m_describeDiskPoolsStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR);
    }
    // execute the statement and gather results
    m_describeDiskPoolsStatement->setString(1, svcClass);
    unsigned int nb = m_describeDiskPoolsStatement->executeUpdate();
    if(0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "describeDiskPools : unable to execute query.";
      throw ex;
    }
    oracle::occi::ResultSet *rset = m_describeDiskPoolsStatement->getCursor(2);
    std::vector<castor::query::DiskPoolQueryResponse*>* result =
      new std::vector<castor::query::DiskPoolQueryResponse*>();
    castor::query::DiskPoolQueryResponse* resp = 0;
    castor::query::DiskServerDescription* dsd = 0;
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      if (rset->getInt(1) == 1) {
	// This line indicates a new DiskPool and gives
	// summary info
	resp = new castor::query::DiskPoolQueryResponse();
	resp->setDiskPoolName(rset->getString(3));
        resp->setFreeSpace((u_signed64)rset->getDouble(7));
        resp->setTotalSpace((u_signed64)rset->getDouble(8));
        resp->setReservedSpace((u_signed64)rset->getDouble(9));
	result->push_back(resp);
      } else {
	// this is not a diskPool summary
	if (rset->getInt(2) == 1) {
	  // This line indicates a new DiskServer and
	  // gives summary info
	  dsd = new castor::query::DiskServerDescription();
	  dsd->setName(rset->getString(4));
	  dsd->setStatus(rset->getInt(5));
	  dsd->setFreeSpace((u_signed64)rset->getDouble(7));
	  dsd->setTotalSpace((u_signed64)rset->getDouble(8));
	  dsd->setReservedSpace((u_signed64)rset->getDouble(9));
	  dsd->setQuery(resp);
	  resp->addDiskServers(dsd);
	} else {
	  // This is a fileSystem description
	  castor::query::FileSystemDescription* fsd =
	    new castor::query::FileSystemDescription();
	  fsd->setMountPoint(rset->getString(6));
	  fsd->setFreeSpace((u_signed64)rset->getDouble(7));
	  fsd->setTotalSpace((u_signed64)rset->getDouble(8));
	  fsd->setReservedSpace((u_signed64)rset->getDouble(9));
	  fsd->setMinFreeSpace(rset->getFloat(10));
	  fsd->setMaxFreeSpace(rset->getFloat(11));
	  fsd->setStatus(rset->getInt(12));
	  fsd->setDiskServer(dsd);
	  dsd->addFileSystems(fsd);
	}
      }
    }
    // If no answer, send an error
    if (0 == result->size()) {
      delete result;
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "No Diskpool found";
      throw ex;      
    }
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in describeDiskPools."
                    << std::endl << e.what();
    throw ex;
  }
}

// -----------------------------------------------------------------------
// describeDiskPool
// -----------------------------------------------------------------------
castor::query::DiskPoolQueryResponse*
castor::db::ora::OraQuerySvc::describeDiskPool (std::string diskPool)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if(0 == m_describeDiskPoolStatement) {
      m_describeDiskPoolStatement =
        createStatement(s_describeDiskPoolStatementString);
      m_describeDiskPoolStatement->registerOutParam
        (2, oracle::occi::OCCICURSOR);
    }
    // execute the statement and gather results
    m_describeDiskPoolStatement->setString(1, diskPool);
    castor::query::DiskPoolQueryResponse* result = 0;
    unsigned int nb = m_describeDiskPoolStatement->executeUpdate();
    if(0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "describeDiskPool : unable to execute query.";
      throw ex;
    }
    oracle::occi::ResultSet *rset = m_describeDiskPoolStatement->getCursor(2);
    castor::query::DiskServerDescription* dsd = 0;
    while (oracle::occi::ResultSet::END_OF_FETCH != rset->next()) {
      if (rset->getInt(1) == 1) {
	result = new castor::query::DiskPoolQueryResponse();
	result->setDiskPoolName(diskPool);
	result->setFreeSpace((u_signed64)rset->getDouble(6));
	result->setTotalSpace((u_signed64)rset->getDouble(7));
	result->setReservedSpace((u_signed64)rset->getDouble(8));
      } else if (rset->getInt(2) == 1) {
	dsd = new castor::query::DiskServerDescription();
	dsd->setName(rset->getString(3));
	dsd->setStatus(rset->getInt(4));
        dsd->setFreeSpace((u_signed64)rset->getDouble(6));
        dsd->setTotalSpace((u_signed64)rset->getDouble(7));
        dsd->setReservedSpace((u_signed64)rset->getDouble(8));
	dsd->setQuery(result);
	result->addDiskServers(dsd);
      } else {
        castor::query::FileSystemDescription* fsd =
          new castor::query::FileSystemDescription();
        fsd->setMountPoint(rset->getString(5));
        fsd->setFreeSpace((u_signed64)rset->getDouble(6));
        fsd->setTotalSpace((u_signed64)rset->getDouble(7));
        fsd->setReservedSpace((u_signed64)rset->getDouble(8));
        fsd->setMinFreeSpace(rset->getFloat(9));
        fsd->setMaxFreeSpace(rset->getFloat(10));
        fsd->setStatus(rset->getInt(11));
        fsd->setDiskServer(dsd);
        dsd->addFileSystems(fsd);
      }
    }
    // if nothing found, send error
    if (0 == result) {
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "No Diskpool found with name '"
		      << diskPool << "'";
      throw ex;
    }
    return result;
  } catch (oracle::occi::SQLException e) {
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in describeDiskPool."
                    << std::endl << e.what();
    throw ex;
  }
}
