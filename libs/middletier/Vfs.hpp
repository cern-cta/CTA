#pragma once

#include "DirectoryIterator.hpp"
#include "SecurityIdentity.hpp"

namespace cta {

/**
 * Mock database.
 */
class Vfs {

public:

  /**
   * Constructor.
   */
  Vfs();

  /**
   * Destructor.
   */
  ~Vfs() throw();  
  
  void setDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName);
  
  void clearDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  std::string getDirectoryStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  void createFile(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode);
  
  void createDirectory(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode);
  
  void deleteFile(const SecurityIdentity &requester, const std::string &pathname);
  
  void deleteDirectory(const SecurityIdentity &requester, const std::string &pathname);
  
  cta::DirectoryEntry statDirectoryEntry(const SecurityIdentity &requester, const std::string &pathname);
  
  cta::DirectoryIterator getDirectoryContents(const SecurityIdentity &requester, const std::string &dirPath); 

private:
  
  std::string m_baseDir;
  
  std::string m_fsDir;
  
  void checkDirectoryExists(const std::string &dirPath);
  
  void checkPathnameDoesNotExist(const std::string &dirPath);
  
  std::list<cta::DirectoryEntry> getDirectoryEntries(const SecurityIdentity &requester, const std::string &dirPath);
  
}; // struct Vfs

} // namespace cta
