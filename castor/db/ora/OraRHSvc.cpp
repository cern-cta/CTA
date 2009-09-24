/******************************************************************************
 *                      OraRHSvc.cpp
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
 * @(#)$RCSfile: OraRHSvc.cpp,v $ $Revision: 1.18 $ $Release$ $Date: 2009/04/14 11:30:00 $ $Author: itglp $
 *
 * Implementation of the IRHSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/db/ora/OraRHSvc.hpp"
#include "castor/Constants.hpp"
#include "castor/SvcFactory.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/PermissionDenied.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/bwlist/BWUser.hpp"
#include "castor/bwlist/RequestType.hpp"
#include "castor/bwlist/Privilege.hpp"
#include <string>
#include <vector>
#include "occi.h"


//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::SvcFactory<castor::db::ora::OraRHSvc>* s_factoryOraRHSvc =
  new castor::SvcFactory<castor::db::ora::OraRHSvc>();

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------

/// SQL statement for checkPermission
const std::string castor::db::ora::OraRHSvc::s_checkPermissionStatementString =
"BEGIN checkPermission(:1, :2, :3, :4, :5); END;";

/// SQL statement for addPrivilege
const std::string castor::db::ora::OraRHSvc::s_addPrivilegeStatementString =
  "BEGIN castorbw.addPrivilege(:1, :2, :3, :4); END;";

/// SQL statement for removePrivilege
const std::string castor::db::ora::OraRHSvc::s_removePrivilegeStatementString =
  "BEGIN castorbw.removePrivilege(:1, :2, :3, :4); END;";

/// SQL statement for listPrivileges
const std::string castor::db::ora::OraRHSvc::s_listPrivilegesStatementString =
  "BEGIN castorbw.listPrivileges(:1, :2, :3, :4, :5); END;";

//------------------------------------------------------------------------------
// OraRHSvc
//------------------------------------------------------------------------------
castor::db::ora::OraRHSvc::OraRHSvc(const std::string name) :
  OraCommonSvc(name),
  m_checkPermissionStatement(0),
  m_addPrivilegeStatement(0),
  m_removePrivilegeStatement(0),
  m_listPrivilegesStatement(0) {
}

//------------------------------------------------------------------------------
// ~OraRHSvc
//------------------------------------------------------------------------------
castor::db::ora::OraRHSvc::~OraRHSvc() throw() {
  reset();
}

//------------------------------------------------------------------------------
// id
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraRHSvc::id() const {
  return ID();
}

//------------------------------------------------------------------------------
// ID
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraRHSvc::ID() {
  return castor::SVC_ORARHSVC;
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::reset() throw() {
  // Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  OraCommonSvc::reset();
  try {
    if (m_checkPermissionStatement) deleteStatement(m_checkPermissionStatement);
    if (m_addPrivilegeStatement) deleteStatement(m_addPrivilegeStatement);
    if (m_removePrivilegeStatement) deleteStatement(m_removePrivilegeStatement);
    if (m_listPrivilegesStatement) deleteStatement(m_listPrivilegesStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_checkPermissionStatement = 0;
  m_addPrivilegeStatement = 0;
  m_removePrivilegeStatement = 0;
  m_listPrivilegesStatement = 0;
}

//------------------------------------------------------------------------------
// checkPermission
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::checkPermission
(castor::stager::Request* req)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statements are ok
    if (0 == m_checkPermissionStatement) {
      m_checkPermissionStatement =
        createStatement(s_checkPermissionStatementString);
      m_checkPermissionStatement->registerOutParam
        (5, oracle::occi::OCCIDOUBLE);
    }
    // execute the statement and see whether we found something
    m_checkPermissionStatement->setString(1, req->svcClassName());
    m_checkPermissionStatement->setInt(2, req->euid());
    m_checkPermissionStatement->setInt(3, req->egid());
    m_checkPermissionStatement->setInt(4, req->type());
    unsigned int nb = m_checkPermissionStatement->executeUpdate();
    if (0 == nb) {
      castor::exception::Internal ex;
      ex.getMessage()
        << "checkPermission did not return any result.";
      throw ex;
    }
    // signed because the return value can be negative for errors
    signed64 ret = (signed64)m_checkPermissionStatement->getDouble(5);
    if (ret > 0) {
      // access granted, ret is the svcClass id. We keep it in an object
      // so to let the RH create the link later on. Note that this has
      // the side effect of allocating an object in memory, which needs
      // to be deallocated by the caller.
      stager::SvcClass* sc = new stager::SvcClass();
      sc->setId(ret);
      req->setSvcClass(sc);
    }
    else if (ret == -1) {
      // no access, throw exception
      castor::exception::PermissionDenied ex;
      ex.getMessage() << "Insufficient user privileges to make a request of type "
                      << castor::ObjectsIdStrings[req->type()]
                      << " in service class '"
                      << (0 == req->svcClassName().size() ? "default" :
                          req->svcClassName())
                      << "'\n";
      throw ex;
    } else if("*" != req->svcClassName()) {
      // ret == -2 : non existent service class; accept the special case of '*'
      // (the stager will check if it is valid or not) and refuse the rest
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "Invalid service class '"
                      << req->svcClassName()
                      << "'\n";
      throw ex;
    }
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in checkPermission."
      << std::endl << e.what();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// handleChangePrivilegeTypeLoop
//------------------------------------------------------------------------------
void handleChangePrivilegeTypeLoop
(std::vector<castor::bwlist::RequestType*> &requestTypes,
 oracle::occi::Statement *stmt)
  throw (castor::exception::Exception, oracle::occi::SQLException) {
  // loop on the request types
  if (requestTypes.size() > 0) {
    for (std::vector<castor::bwlist::RequestType*>::const_iterator typeIt =
           requestTypes.begin();
         typeIt != requestTypes.end();
         typeIt++) {
      if ((*typeIt)->reqType() > 0) {
        stmt->setInt(4, (*typeIt)->reqType());
      } else {
        stmt->setNull(4, oracle::occi::OCCINUMBER);
      }
      stmt->executeUpdate();
    }
  } else {
    // no request type given, that means all of them are targeted
    stmt->setNull(4, oracle::occi::OCCINUMBER);
    stmt->executeUpdate();
  }
}

//------------------------------------------------------------------------------
// changePrivilege
//------------------------------------------------------------------------------
void castor::db::ora::OraRHSvc::changePrivilege
(const std::string svcClassName,
 std::vector<castor::bwlist::BWUser*> users,
 std::vector<castor::bwlist::RequestType*> requestTypes,
 bool isAdd)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (isAdd){
      if (0 == m_addPrivilegeStatement) {
        m_addPrivilegeStatement =
          createStatement(s_addPrivilegeStatementString);
      }
    } else {
      if (0 == m_removePrivilegeStatement) {
        m_removePrivilegeStatement =
          createStatement(s_removePrivilegeStatementString);
      }
    }
    // deal with the type of change
    oracle::occi::Statement *stmt =
      isAdd ? m_addPrivilegeStatement : m_removePrivilegeStatement;
    // deal with the service class if any
    if (svcClassName != "*") {
      stmt->setString(1, svcClassName);
    } else {
      stmt->setNull(1, oracle::occi::OCCISTRING);
    }
    // loop over users if any
    if (users.size() > 0) {
      for (std::vector<castor::bwlist::BWUser*>::const_iterator userIt =
             users.begin();
           userIt != users.end();
           userIt++) {
        if ((*userIt)->euid() >= 0) {
          stmt->setInt(2, (*userIt)->euid());
        } else {
          stmt->setNull(2, oracle::occi::OCCINUMBER);
        }
        if ((*userIt)->egid() >= 0) {
          stmt->setInt(3, (*userIt)->egid());
        } else {
          stmt->setNull(3, oracle::occi::OCCINUMBER);
        }
        // loop on the request types if any and call DB
        handleChangePrivilegeTypeLoop(requestTypes, stmt);
      }
    } else {
      // empty user list, that means all uids and all gids
      stmt->setNull(2, oracle::occi::OCCINUMBER);
      stmt->setNull(3, oracle::occi::OCCINUMBER);
      // loop on the request types if any and call DB
      handleChangePrivilegeTypeLoop(requestTypes, stmt);
    }
    commit();
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    if (e.getErrorCode() == 20108) {
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "The operation you are requesting cannot be expressed "
                      << "in the privilege table.\nThis means "
                      << "that you are trying to grant only part of a privilege "
                      << "that is currently denied.\nConsider granting all of it "
                      << "and denying the complement afterward";
      throw ex;
    } else if (e.getErrorCode() == 20113) {
      std::string msg = e.what();
      castor::exception::InvalidArgument ex;
      // extract the original message sent by the PL/SQL code
      // from the surrounding oracle additions
      ex.getMessage() << msg.substr(11,msg.find('\n')-11) << "\n";
      throw ex;
    } else {
      castor::exception::Internal ex;
      ex.getMessage()
        << "Error caught in changePrivilege."
        << std::endl << e.what();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// listPrivileges
//------------------------------------------------------------------------------
std::vector<castor::bwlist::Privilege*>
castor::db::ora::OraRHSvc::listPrivileges
(const std::string svcClassName, const int user,
 const int group, const int requestType)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_listPrivilegesStatement) {
      m_listPrivilegesStatement =
        createStatement(s_listPrivilegesStatementString);
      m_listPrivilegesStatement->registerOutParam
        (5, oracle::occi::OCCICURSOR);
    }
    // deal with the service class, user, group and type
    if (svcClassName != "*") {
      m_listPrivilegesStatement->setString(1, svcClassName);
    } else {
      m_listPrivilegesStatement->setNull(1, oracle::occi::OCCISTRING);
    }
    m_listPrivilegesStatement->setInt(2, user);
    m_listPrivilegesStatement->setInt(3, group);
    m_listPrivilegesStatement->setInt(4, requestType );
    // Call DB
    m_listPrivilegesStatement->executeUpdate();
    // Extract list of privileges
    std::vector<castor::bwlist::Privilege*> result;
    oracle::occi::ResultSet *prs =
      m_listPrivilegesStatement->getCursor(5);
    oracle::occi::ResultSet::Status status = prs->next();
    while (status == oracle::occi::ResultSet::DATA_AVAILABLE) {
      castor::bwlist::Privilege* p = new castor::bwlist::Privilege();
      p->setServiceClass(prs->getString(1));
      p->setEuid(prs->getInt(2));
      if (prs->isNull(2)) p->setEuid(-1);
      p->setEgid(prs->getInt(3));
      if (prs->isNull(3)) p->setEgid(-1);
      p->setRequestType(prs->getInt(4));
      p->setGranted(prs->getInt(5) != 0);
      result.push_back(p);
      status = prs->next();
    }
    // return result
    return result;
  } catch (oracle::occi::SQLException e) {
    handleException(e);
    castor::exception::Internal ex;
    ex.getMessage()
      << "Error caught in listPrivileges."
      << std::endl << e.what();
    throw ex;
  }
}
