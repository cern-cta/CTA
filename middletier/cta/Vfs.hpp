#pragma once

#include "cta/DirIterator.hpp"
#include "cta/SecurityIdentity.hpp"

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
  
  void setDirStorageClass(const SecurityIdentity &requester, const std::string &path, const std::string &storageClassName);
  
  void clearDirStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  std::string getDirStorageClass(const SecurityIdentity &requester, const std::string &path);
  
  void createFile(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode);
  
  void createDir(const SecurityIdentity &requester, const std::string &pathname, const uint16_t mode);
  
  void deleteFile(const SecurityIdentity &requester, const std::string &pathname);
  
  void deleteDir(const SecurityIdentity &requester, const std::string &pathname);
  
  cta::DirEntry statDirEntry(const SecurityIdentity &requester, const std::string &pathname);
  
  cta::DirIterator getDirContents(const SecurityIdentity &requester, const std::string &dirPath);
  
  bool isExistingDir(const SecurityIdentity &requester, const std::string &dirPath);
  
  std::string getVidOfFile(const SecurityIdentity &requester, const std::string &pathname, uint16_t copyNb);
  
  void checkStorageClassIsNotInUse(const SecurityIdentity &requester, const std::string &storageClass, const std::string &dirPath);

private:
  
  std::string m_baseDir;
  
  std::string m_fsDir;
  
  void checkDirectoryExists(const std::string &dirPath);
  
  void checkPathnameDoesNotExist(const std::string &dirPath);
  
  std::list<cta::DirEntry> getDirEntries(const SecurityIdentity &requester, const std::string &dirPath);
  
}; // struct Vfs

} // namespace cta
