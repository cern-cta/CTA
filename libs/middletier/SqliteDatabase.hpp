#pragma once

#include <sqlite3.h>

#include "FileSystemNode.hpp"
#include "FileSystemStorageClasses.hpp"
#include "MockAdminHostTable.hpp"
#include "MockAdminUserTable.hpp"
#include "MockArchiveRouteTable.hpp"
#include "MockLogicalLibraryTable.hpp"
#include "MockTapeTable.hpp"
#include "MockTapePoolTable.hpp"

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
  
  void insertStorageClass(const SecurityIdentity &requester, const std::string &name, const uint8_t nbCopies, const std::string &comment);
  
  void insertArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint8_t copyNb, const std::string &tapePoolName, const std::string &comment);
  
  void insertFile(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode);
  
  void insertDirectory(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode);
  
  void deleteTapePool(const SecurityIdentity &requester, const std::string &name);
  
  void deleteStorageClass(const SecurityIdentity &requester, const std::string &name);

  void deleteArchiveRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint8_t copyNb);
  
  void deleteFile(const SecurityIdentity &requester, const std::string &pathname);
  
  void deleteDirectory(const SecurityIdentity &requester, const std::string &pathname);
  
  std::list<cta::TapePool> selectAllTapePools(const SecurityIdentity &requester);

  std::list<cta::StorageClass> selectAllStorageClasses(const SecurityIdentity &requester);

  std::list<cta::ArchiveRoute> selectAllArchiveRoutes(const SecurityIdentity &requester);
  
  void setDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName);
  
  void clearDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  std::string getDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path);

private:
  
  /**
   * SQLite DB handle  
   */
  sqlite3 *m_dbHandle;
  
  void createArchiveRouteTable();

  void createStorageClassTable();

  void createTapePoolTable();
  
  void createDirectoryTable();
  
  void createFileTable();
  
  void createTapeTable();
  
  void createAdminUserTable();
    
  void createAdminHostTable();
  
  void createArchivalJobTable();
  
  void createRetrievalJobTable();
  
  void createLogicalLibraryTable();

  void createSchema();
  
  void checkTapePoolExists(const std::string &name);
  
  void checkStorageClassExists(const std::string &name);
  
  void checkArchiveRouteExists(const std::string &name, const uint8_t copyNb);
  
  void checkFileExists(const std::string &path, const std::string &name);
  
  void checkDirectoryExists(const std::string &path);
  
  void checkDirectoryContainsNoDirectories(const std::string &path);
  
  void checkDirectoryContainsNoFiles(const std::string &path);
  
}; // struct SqliteDatabase

} // namespace cta
