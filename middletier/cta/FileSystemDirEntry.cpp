#include "cta/FileSystemDirEntry.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemDirEntry::FileSystemDirEntry():
  m_storageClasses(NULL) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemDirEntry::FileSystemDirEntry(
  FileSystemStorageClasses &storageClasses, const DirEntry &entry):
  m_storageClasses(&storageClasses),
  m_entry(entry) {
  setStorageClassName(entry.getStorageClassName());
}

//------------------------------------------------------------------------------
// getEntry
//------------------------------------------------------------------------------
const cta::DirEntry &cta::FileSystemDirEntry::getEntry()
  const throw() {
  return m_entry;
}

//------------------------------------------------------------------------------
// setStorageClassName
//------------------------------------------------------------------------------
void cta::FileSystemDirEntry::setStorageClassName(
  const std::string &storageClassName) {
  const std::string previousStorageClassName = m_entry.getStorageClassName();

  // If there is a change in storage class
  if(previousStorageClassName != storageClassName) {
    if(m_storageClasses) {
      m_storageClasses->decStorageClassUsageCount(previousStorageClassName);
      m_storageClasses->incStorageClassUsageCount(storageClassName);
    }

    m_entry.setStorageClassName(storageClassName);
  }
}
