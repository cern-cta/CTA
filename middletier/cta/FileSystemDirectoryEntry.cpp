#include "cta/FileSystemDirectoryEntry.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemDirectoryEntry::FileSystemDirectoryEntry():
  m_storageClasses(NULL) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemDirectoryEntry::FileSystemDirectoryEntry(
  FileSystemStorageClasses &storageClasses, const DirectoryEntry &entry):
  m_storageClasses(&storageClasses),
  m_entry(entry) {
  setStorageClassName(entry.getStorageClassName());
}

//------------------------------------------------------------------------------
// getEntry
//------------------------------------------------------------------------------
const cta::DirectoryEntry &cta::FileSystemDirectoryEntry::getEntry()
  const throw() {
  return m_entry;
}

//------------------------------------------------------------------------------
// setStorageClassName
//------------------------------------------------------------------------------
void cta::FileSystemDirectoryEntry::setStorageClassName(
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
