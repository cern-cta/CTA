#include "DirectoryEntry.hpp"

//------------------------------------------------------------------------------
// entryTypeToStr
//------------------------------------------------------------------------------
const char *cta::DirectoryEntry::entryTypeToStr(const EntryType enumValue)
  throw() {
  switch(enumValue) {
  case ENTRYTYPE_NONE     : return "NONE";
  case ENTRYTYPE_FILE     : return "FILE";
  case ENTRYTYPE_DIRECTORY: return "DIRECTORY";
  default                 : return "UNKNOWN";
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirectoryEntry::DirectoryEntry():
  m_ownerId(0),
  m_groupId(0),
  m_mode(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirectoryEntry::DirectoryEntry(const EntryType entryType,
  const std::string &name, const std::string &storageClassName):
  m_entryType(entryType),
  m_name(name),
  m_ownerId(0),
  m_groupId(0),
  m_storageClassName(storageClassName) {
}

//------------------------------------------------------------------------------
// getEntryType
//------------------------------------------------------------------------------
cta::DirectoryEntry::EntryType cta::DirectoryEntry::getEntryType()
  const throw() {
  return m_entryType;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::DirectoryEntry::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getOwnerId
//------------------------------------------------------------------------------
uint32_t cta::DirectoryEntry::getOwnerId() const throw() {
  return m_ownerId;
}

//------------------------------------------------------------------------------
// getGroupId
//------------------------------------------------------------------------------
uint32_t cta::DirectoryEntry::getGroupId() const throw() {
  return m_groupId;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
uint16_t cta::DirectoryEntry::getMode() const throw() {
  return m_mode;
}

//------------------------------------------------------------------------------
// setStorageClassName
//------------------------------------------------------------------------------
void cta::DirectoryEntry::setStorageClassName(
  const std::string &storageClassName) {
  m_storageClassName = storageClassName;
}
  
//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::DirectoryEntry::getStorageClassName() const throw() {
  return m_storageClassName;
}
