#include "cta/DirEntry.hpp"

//------------------------------------------------------------------------------
// entryTypeToStr
//------------------------------------------------------------------------------
const char *cta::DirEntry::entryTypeToStr(const EntryType enumValue)
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
cta::DirEntry::DirEntry():
  m_ownerId(0),
  m_groupId(0),
  m_mode(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirEntry::DirEntry(const EntryType entryType,
  const std::string &name, const std::string &storageClassName):
  m_entryType(entryType),
  m_name(name),
  m_ownerId(0),
  m_groupId(0),
  m_storageClassName(storageClassName) {
}

//------------------------------------------------------------------------------
// getType
//------------------------------------------------------------------------------
cta::DirEntry::EntryType cta::DirEntry::getType()
  const throw() {
  return m_entryType;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::DirEntry::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getOwnerId
//------------------------------------------------------------------------------
uint32_t cta::DirEntry::getOwnerId() const throw() {
  return m_ownerId;
}

//------------------------------------------------------------------------------
// getGroupId
//------------------------------------------------------------------------------
uint32_t cta::DirEntry::getGroupId() const throw() {
  return m_groupId;
}

//------------------------------------------------------------------------------
// getMode
//------------------------------------------------------------------------------
uint16_t cta::DirEntry::getMode() const throw() {
  return m_mode;
}

//------------------------------------------------------------------------------
// setStorageClassName
//------------------------------------------------------------------------------
void cta::DirEntry::setStorageClassName(
  const std::string &storageClassName) {
  m_storageClassName = storageClassName;
}
  
//------------------------------------------------------------------------------
// getStorageClassName
//------------------------------------------------------------------------------
const std::string &cta::DirEntry::getStorageClassName() const throw() {
  return m_storageClassName;
}
