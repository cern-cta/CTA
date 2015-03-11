#pragma once

#include <list>
#include <map>

#include <sqlite3.h>

#include "AdminHost.hpp"
#include "AdminUser.hpp"
#include "ArchivalJob.hpp"
#include "ArchiveRoute.hpp"
#include "LogicalLibrary.hpp"
#include "RetrievalJob.hpp"
#include "SecurityIdentity.hpp"
#include "StorageClass.hpp"
#include "Tape.hpp"
#include "TapePool.hpp"

namespace cta {

/**
 * Mock database.
 */
class SqliteDatabase {

public:

  /**
   * Constructor.
   */
  SqliteDatabase();

  /**
   * Destructor.
   */
  ~SqliteDatabase() throw();
  
  void insertTapePool(const SecurityIdentity &requester, const std::string &name, const uint16_t nbDrives, const uint32_t nbPartialTapes, const std::string &comment);
  
  void insertStorageClass(const SecurityIdentity &requester, const std::string &name, const uint16_t nbCopies, const std::string &comment);
  
  void insertArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb, const std::string &tapePoolName, const std::string &comment);
  
  void insertTape(const SecurityIdentity &requester, const std::string &vid, const std::string &logicalLibraryName, const std::string &tapePoolName, const uint64_t capacityInBytes, const std::string &comment);
  
  void insertAdminUser(const SecurityIdentity &requester, const UserIdentity &user, const std::string &comment);
  
  void insertAdminHost(const SecurityIdentity &requester, const std::string &hostName, const std::string &comment);
  
  void insertArchivalJob(const SecurityIdentity &requester, const std::string &tapepool, const std::string &srcUrl, const std::string &dstPath);
  
  void insertRetrievalJob(const SecurityIdentity &requester, const std::string &vid, const std::string &srcPath, const std::string &dstUrl);
  
  void insertLogicalLibrary(const SecurityIdentity &requester, const std::string &name, const std::string &comment);  
  
  void deleteTapePool(const SecurityIdentity &requester, const std::string &name);
  
  void deleteStorageClass(const SecurityIdentity &requester, const std::string &name);

  void deleteArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint16_t copyNb);
  
  void deleteTape(const SecurityIdentity &requester, const std::string &vid);
  
  void deleteAdminUser(const SecurityIdentity &requester, const UserIdentity &user);
  
  void deleteAdminHost(const SecurityIdentity &requester, const std::string &hostName);
  
  void deleteArchivalJob(const SecurityIdentity &requester, const std::string &dstPath);
  
  void deleteRetrievalJob(const SecurityIdentity &requester, const std::string &dstUrl);
  
  void deleteLogicalLibrary(const SecurityIdentity &requester, const std::string &name);  
  
  std::list<cta::TapePool> selectAllTapePools(const SecurityIdentity &requester);

  std::list<cta::StorageClass> selectAllStorageClasses(const SecurityIdentity &requester);

  std::list<cta::ArchiveRoute> selectAllArchiveRoutes(const SecurityIdentity &requester);  
  
  std::list<cta::Tape> selectAllTapes(const SecurityIdentity &requester);

  std::list<cta::AdminUser> selectAllAdminUsers(const SecurityIdentity &requester);

  std::list<cta::AdminHost> selectAllAdminHosts(const SecurityIdentity &requester);
  
  std::map<cta::TapePool, std::list<cta::ArchivalJob> > selectAllArchivalJobs(const SecurityIdentity &requester);

  std::map<cta::Tape, std::list<cta::RetrievalJob> > selectAllRetrievalJobs(const SecurityIdentity &requester);

  std::list<cta::LogicalLibrary> selectAllLogicalLibraries(const SecurityIdentity &requester);
  
  cta::ArchiveRoute getArchiveRouteOfStorageClass(const SecurityIdentity &requester, const std::string &storageClassName, const  uint16_t copyNb);
  
  cta::TapePool getTapePoolByName(const SecurityIdentity &requester, const std::string &name);
  
  cta::Tape getTapeByVid(const SecurityIdentity &requester, const std::string &vid);  

private:
  
  /**
   * SQLite DB handle  
   */
  sqlite3 *m_dbHandle;
  
  void createArchiveRouteTable();

  void createStorageClassTable();

  void createTapePoolTable();
  
  void createTapeTable();
  
  void createAdminUserTable();
    
  void createAdminHostTable();
  
  void createArchivalJobTable();
  
  void createRetrievalJobTable();
  
  void createLogicalLibraryTable();

  void createSchema();
  
  void checkTapePoolExists(const std::string &name);
  
  void checkStorageClassExists(const std::string &name);
  
  void checkArchiveRouteExists(const std::string &name, const uint16_t copyNb);
  
  void checkTapeExists(const std::string &vid);
  
  void checkAdminUserExists(const cta::UserIdentity &user);
  
  void checkAdminHostExists(const std::string &hostName);
  
  void checkArchivalJobExists(const std::string &dstPath);
  
  void checkRetrievalJobExists(const std::string &dstUrl);
  
  void checkLogicalLibraryExists(const std::string &name);
  
}; // struct SqliteDatabase

} // namespace cta
