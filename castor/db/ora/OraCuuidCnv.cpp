// Include Files
#include "OraCuuidCnv.hpp"
#include "castor/CnvFactory.hpp"
#include "castor/IAddress.hpp"
#include "castor/IConverter.hpp"
#include "castor/IFactory.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectCatalog.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/db/DbAddress.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/stager/Cuuid.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include <list>

//------------------------------------------------------------------------------
// Instantiation of a static factory class
//------------------------------------------------------------------------------
static castor::CnvFactory<castor::db::ora::OraCuuidCnv> s_factoryOraCuuidCnv;
const castor::IFactory<castor::IConverter>& OraCuuidCnvFactory =
  s_factoryOraCuuidCnv;

//------------------------------------------------------------------------------
// Static constants initialization
//------------------------------------------------------------------------------
/// SQL statement for request insertion
const std::string castor::db::ora::OraCuuidCnv::s_insertStatementString =
  "INSERT INTO rh_Cuuid (id, time_low, time_mid, time_hv, clock_hi, clock_low, node) VALUES (:1,:2,:3,:4,:5,:6,:7)";

/// SQL statement for request deletion
const std::string castor::db::ora::OraCuuidCnv::s_deleteStatementString =
  "DELETE FROM rh_Cuuid WHERE id = :1";

/// SQL statement for request selection
const std::string castor::db::ora::OraCuuidCnv::s_selectStatementString =
  "SELECT time_low, time_mid, time_hv, clock_hi, clock_low, node FROM rh_Cuuid WHERE id = :1";

/// SQL statement for request update
const std::string castor::db::ora::OraCuuidCnv::s_updateStatementString =
  "UPDATE rh_Cuuid SET time_low = :1, time_mid = :2, time_hv = :3, clock_hi = :4, clock_low = :5, node = :6 WHERE id = :7";

/// SQL statement for type storage
const std::string castor::db::ora::OraCuuidCnv::s_storeTypeStatementString =
  "INSERT INTO rh_Id2Type (id, type) VALUES (:1, :2)";

/// SQL statement for type deletion
const std::string castor::db::ora::OraCuuidCnv::s_deleteTypeStatementString =
  "DELETE FROM rh_Id2Type WHERE id = :1";

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
castor::db::ora::OraCuuidCnv::OraCuuidCnv() :
  OraBaseCnv(),
  m_insertStatement(0),
  m_deleteStatement(0),
  m_selectStatement(0),
  m_updateStatement(0),
  m_storeTypeStatement(0),
  m_deleteTypeStatement(0) {}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
castor::db::ora::OraCuuidCnv::~OraCuuidCnv() throw() {
  reset();
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::db::ora::OraCuuidCnv::reset() throw() {
  //Here we attempt to delete the statements correctly
  // If something goes wrong, we just ignore it
  try {
    deleteStatement(m_insertStatement);
    deleteStatement(m_deleteStatement);
    deleteStatement(m_selectStatement);
    deleteStatement(m_updateStatement);
    deleteStatement(m_storeTypeStatement);
    deleteStatement(m_deleteTypeStatement);
  } catch (oracle::occi::SQLException e) {};
  // Now reset all pointers to 0
  m_insertStatement = 0;
  m_deleteStatement = 0;
  m_selectStatement = 0;
  m_updateStatement = 0;
  m_storeTypeStatement = 0;
  m_deleteTypeStatement = 0;
}

//------------------------------------------------------------------------------
// ObjType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraCuuidCnv::ObjType() {
  return castor::stager::Cuuid::TYPE();
}

//------------------------------------------------------------------------------
// objType
//------------------------------------------------------------------------------
const unsigned int castor::db::ora::OraCuuidCnv::objType() const {
  return ObjType();
}

//------------------------------------------------------------------------------
// createRep
//------------------------------------------------------------------------------
void castor::db::ora::OraCuuidCnv::createRep(castor::IAddress* address,
                                             castor::IObject* object,
                                             castor::ObjectSet& alreadyDone,
                                             bool autocommit,
                                             bool /*recursive*/)
  throw (castor::exception::Exception) {
  castor::stager::Cuuid* obj =
    dynamic_cast<castor::stager::Cuuid*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_insertStatement) {
      m_insertStatement = createStatement(s_insertStatementString);
    }
    if (0 == m_storeTypeStatement) {
      m_storeTypeStatement = createStatement(s_storeTypeStatementString);
    }
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Set ids of all objects
    int nids = obj->id() == 0 ? 1 : 0;
    u_signed64 id = cnvSvc()->getIds(nids);
    if (0 == obj->id()) obj->setId(id++);
    // Now Save the current object
    m_storeTypeStatement->setInt(1, obj->id());
    m_storeTypeStatement->setInt(2, obj->type());
    m_storeTypeStatement->executeUpdate();
    Cuuid_t content = obj->content();
    m_insertStatement->setInt(1, obj->id());
    m_insertStatement->setInt(2, content.time_low);
    m_insertStatement->setInt(3, content.time_mid);
    m_insertStatement->setInt(4, content.time_hi_and_version);
    m_insertStatement->setInt(5, content.clock_seq_hi_and_reserved);
    m_insertStatement->setInt(6, content.clock_seq_low);
    std::string nodeS((char*)content.node, 6);
    m_insertStatement->setString(7, nodeS);
    m_insertStatement->executeUpdate();
    if (autocommit) {
      cnvSvc()->getConnection()->commit();
    }
  } catch (oracle::occi::SQLException e) {
    try {
      // Always try to rollback
      cnvSvc()->getConnection()->rollback();
      if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
        // We've obviously lost the ORACLE connection here
        cnvSvc()->dropConnection();
      }
    } catch (oracle::occi::SQLException e) {
      // rollback failed, let's drop the connection for security
      cnvSvc()->dropConnection();
    }
    castor::exception::InvalidArgument ex; // XXX fix it depending on ORACLE Error
    ex.getMessage() << "Error in insert request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_insertStatementString;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateRep
//------------------------------------------------------------------------------
void castor::db::ora::OraCuuidCnv::updateRep(castor::IAddress* address,
                                             castor::IObject* object,
                                             castor::ObjectSet& alreadyDone,
                                             bool autocommit,
                                             bool /*recursive*/)
  throw (castor::exception::Exception) {
  castor::stager::Cuuid* obj =
    dynamic_cast<castor::stager::Cuuid*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_updateStatement) {
      m_updateStatement = createStatement(s_updateStatementString);
    }
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Now Update the current object
    Cuuid_t content = obj->content();
    m_updateStatement->setInt(1, content.time_low);
    m_updateStatement->setInt(2, content.time_mid);
    m_updateStatement->setInt(3, content.time_hi_and_version);
    m_updateStatement->setInt(4, content.clock_seq_hi_and_reserved);
    m_updateStatement->setInt(5, content.clock_seq_low);
    std::string nodeS((char*)content.node, 6);
    m_updateStatement->setString(6, nodeS);
    m_updateStatement->setInt(7, obj->id());
    m_updateStatement->executeUpdate();
    if (autocommit) {
      cnvSvc()->getConnection()->commit();
    }
  } catch (oracle::occi::SQLException e) {
    try {
      // Always try to rollback
      cnvSvc()->getConnection()->rollback();
      if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
        // We've obviously lost the ORACLE connection here
        cnvSvc()->dropConnection();
      }
    } catch (oracle::occi::SQLException e) {
      // rollback failed, let's drop the connection for security
      cnvSvc()->dropConnection();
    }
    castor::exception::InvalidArgument ex; // XXX fix it depending on ORACLE Error
    ex.getMessage() << "Error in update request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_updateStatementString;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// deleteRep
//------------------------------------------------------------------------------
void castor::db::ora::OraCuuidCnv::deleteRep(castor::IAddress* address,
                                             castor::IObject* object,
                                             castor::ObjectSet& alreadyDone,
                                             bool autocommit)
  throw (castor::exception::Exception) {
  castor::stager::Cuuid* obj =
    dynamic_cast<castor::stager::Cuuid*>(object);
  // check whether something needs to be done
  if (0 == obj) return;
  try {
    // Check whether the statements are ok
    if (0 == m_deleteStatement) {
      m_deleteStatement = createStatement(s_deleteStatementString);
    }
    if (0 == m_deleteTypeStatement) {
      m_deleteTypeStatement = createStatement(s_deleteTypeStatementString);
    }
    // Mark the current object as done
    alreadyDone.insert(obj);
    // Now Delete the object
    m_deleteTypeStatement->setInt(1, obj->id());
    m_deleteTypeStatement->executeUpdate();
    m_deleteStatement->setInt(1, obj->id());
    m_deleteStatement->executeUpdate();
    if (autocommit) {
      cnvSvc()->getConnection()->commit();
    }
  } catch (oracle::occi::SQLException e) {
    try {
      // Always try to rollback
      cnvSvc()->getConnection()->rollback();
      if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
        // We've obviously lost the ORACLE connection here
        cnvSvc()->dropConnection();
      }
    } catch (oracle::occi::SQLException e) {
      // rollback failed, let's drop the connection for security
      cnvSvc()->dropConnection();
    }
    castor::exception::InvalidArgument ex; // XXX fix it depending on ORACLE Error
    ex.getMessage() << "Error in delete request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_deleteStatementString << std::endl
                    << "and id was " << obj->id();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// createObj
//------------------------------------------------------------------------------
castor::IObject* castor::db::ora::OraCuuidCnv::createObj(castor::IAddress* address,
                                                         castor::ObjectCatalog& newlyCreated)
  throw (castor::exception::Exception) {
  DbAddress* ad =
    dynamic_cast<DbAddress*>(address);
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    if (0 == m_selectStatement) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to create statement :" << std::endl
                      << s_selectStatementString;
      throw ex;
    }
    // retrieve the object from the database
    m_selectStatement->setInt(1, ad->id());
    oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::exception::NoEntry ex;
      ex.getMessage() << "No object found for id :" << ad->id();
      throw ex;
    }
    // create the new Object
    castor::stager::Cuuid* object = new castor::stager::Cuuid();
    // Now retrieve and set members
    Cuuid_t content;
    content.time_low = rset->getInt(1);
    content.time_mid = rset->getInt(2);
    content.time_hi_and_version = rset->getInt(3);
    content.clock_seq_hi_and_reserved = rset->getInt(4);
    content.clock_seq_low = rset->getInt(5);
    memcpy(content.node, rset->getString(6).data(), 6*sizeof(BYTE));
    object->setId(ad->id());
    object->setContent(content);
    newlyCreated[object->id()] = object;
    m_selectStatement->closeResultSet(rset);
    return object;
  } catch (oracle::occi::SQLException e) {
    try {
      // Always try to rollback
      cnvSvc()->getConnection()->rollback();
      if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
        // We've obviously lost the ORACLE connection here
        cnvSvc()->dropConnection();
      }
    } catch (oracle::occi::SQLException e) {
      // rollback failed, let's drop the connection for security
      cnvSvc()->dropConnection();
    }
    castor::exception::InvalidArgument ex; // XXX fix it depending on ORACLE Error
    ex.getMessage() << "Error in select request :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_selectStatementString << std::endl
                    << "and id was " << ad->id();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// updateObj
//------------------------------------------------------------------------------
void castor::db::ora::OraCuuidCnv::updateObj(castor::IObject* obj,
                                             castor::ObjectCatalog& alreadyDone)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement is ok
    if (0 == m_selectStatement) {
      m_selectStatement = createStatement(s_selectStatementString);
    }
    if (0 == m_selectStatement) {
      castor::exception::Internal ex;
      ex.getMessage() << "Unable to create statement :" << std::endl
                      << s_selectStatementString;
      throw ex;
    }
    // retrieve the object from the database
    m_selectStatement->setInt(1, obj->id());
    oracle::occi::ResultSet *rset = m_selectStatement->executeQuery();
    if (oracle::occi::ResultSet::END_OF_FETCH == rset->next()) {
      castor::exception::NoEntry ex;
      ex.getMessage() << "No object found for id :" << obj->id();
      throw ex;
    }
    // Now retrieve and set members
    castor::stager::Cuuid* object = 
      dynamic_cast<castor::stager::Cuuid*>(obj);
    Cuuid_t content;
    content.time_low = rset->getInt(1);
    content.time_mid = rset->getInt(2);
    content.time_hi_and_version = rset->getInt(3);
    content.clock_seq_hi_and_reserved = rset->getInt(4);
    content.clock_seq_low = rset->getInt(5);
    memcpy(content.node, rset->getString(6).data(), 6*sizeof(BYTE));
    object->setContent(content);
    alreadyDone[object->id()] = object;
    m_selectStatement->closeResultSet(rset);
  } catch (oracle::occi::SQLException e) {
    try {
      // Always try to rollback
      cnvSvc()->getConnection()->rollback();
      if (3114 == e.getErrorCode() || 28 == e.getErrorCode()) {
        // We've obviously lost the ORACLE connection here
        cnvSvc()->dropConnection();
      }
    } catch (oracle::occi::SQLException e) {
      // rollback failed, let's drop the connection for security
      cnvSvc()->dropConnection();
    }
    castor::exception::InvalidArgument ex; // XXX Fix it, depending on ORACLE error
    ex.getMessage() << "Error in update object :"
                    << std::endl << e.what() << std::endl
                    << "Statement was :" << std::endl
                    << s_selectStatementString << std::endl
                    << "and id was " << obj->id() << std::endl;;
    throw ex;
  }
}
