#pragma once

#include <sqlite3.h>

#include "FileSystemNode.hpp"
#include "FileSystemStorageClasses.hpp"
#include "MockAdminHostTable.hpp"
#include "MockAdminUserTable.hpp"
#include "MockLogicalLibraryTable.hpp"
#include "MockMigrationRouteTable.hpp"
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

private:
  
  /**
   * SQLite DB handle  
   */
  sqlite3 *m_dbHandle;
  
  void createMigrationRouteTable();

  void createStorageClassTable();

  void createTapePoolTable();

  void createSchema();
  
  void insertTapePool(const SecurityIdentity &requester, const std::string &name, const uint16_t nbDrives, const uint32_t nbPartialTapes, const std::string &comment);
  
  void insertStorageClass(const SecurityIdentity &requester, const std::string &name, const uint8_t nbCopies, const std::string &comment);
  
  void insertMigrationRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint8_t copyNb, const std::string &tapePoolName, const std::string &comment);
  
  void deleteTapePool(const SecurityIdentity &requester, const std::string &name);
  
  void deleteStorageClass(const SecurityIdentity &requester, const std::string &name);

  void deleteMigrationRoute(const SecurityIdentity &requester, const std::string &storageClassName, const uint8_t copyNb);
  
  std::list<cta::TapePool> selectAllTapePools(const SecurityIdentity &requester);

  std::list<cta::StorageClass> selectAllStorageClasses(const SecurityIdentity &requester);

  std::list<cta::MigrationRoute> selectAllMigrationRoutes(const SecurityIdentity &requester);
  
}; // struct SqliteDatabase

} // namespace cta
