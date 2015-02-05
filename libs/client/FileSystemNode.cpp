#include "Exception.hpp"
#include "FileSystemNode.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemNode::FileSystemNode():
  m_storageClasses(NULL),
  m_parent(NULL) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemNode::FileSystemNode(const DirectoryEntry &entry):
  m_storageClasses(NULL),
  m_parent(NULL),
  m_entry(entry) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemNode::FileSystemNode(FileSystemStorageClasses &storageclasses, 
  const DirectoryEntry &entry):
  m_storageClasses(&storageclasses),
  m_entry(entry) {
  storageclasses.incStorageClassUsageCount(entry.storageClassName);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::FileSystemNode::~FileSystemNode() throw() {
  try {
    decStorageClassUsageCount();
  } catch(...) {
  }

  try {
    deleteAndClearChildren();
  } catch(...) {
  }
}

//------------------------------------------------------------------------------
// decStorageClassUsageCount
//------------------------------------------------------------------------------
void cta::FileSystemNode::decStorageClassUsageCount() {
  if(m_storageClasses) {
    m_storageClasses->decStorageClassUsageCount(m_entry.storageClassName);
  }
}

//------------------------------------------------------------------------------
// deleteAndClearChildren
//------------------------------------------------------------------------------
void cta::FileSystemNode::deleteAndClearChildren() {
  for(std::map<std::string, FileSystemNode*>::const_iterator itor =
    m_children.begin(); itor != m_children.end(); itor++) {
    delete(itor->second);
  }
  m_children.clear();
}

//------------------------------------------------------------------------------
// getParent
//------------------------------------------------------------------------------
cta::FileSystemNode &cta::FileSystemNode::getParent() {
  if(NULL == m_parent) {
    throw Exception("Internal error");
  }

  return *m_parent;
}

//------------------------------------------------------------------------------
// getParent
//------------------------------------------------------------------------------
const cta::FileSystemNode &cta::FileSystemNode::getParent() const {
  if(NULL == m_parent) {
    throw Exception("Internal error");
  }

  return *m_parent;
}

//------------------------------------------------------------------------------
// getEntry
//------------------------------------------------------------------------------
cta::DirectoryEntry &cta::FileSystemNode::getEntry() throw() {
  return m_entry;
}

//------------------------------------------------------------------------------
// getEntry
//------------------------------------------------------------------------------
const cta::DirectoryEntry &cta::FileSystemNode::getEntry() const throw() {
  return m_entry;
}

//------------------------------------------------------------------------------
// getDirectoryEntries
//------------------------------------------------------------------------------
std::list<cta::DirectoryEntry> cta::FileSystemNode::getDirectoryEntries()
  const {
  std::list<DirectoryEntry> entries;
  for(std::map<std::string, FileSystemNode*>::const_iterator itor =
    m_children.begin(); itor != m_children.end(); itor++) {
    entries.push_back(itor->second->getEntry());
  }
  return entries;
}

//------------------------------------------------------------------------------
// addChild
//------------------------------------------------------------------------------
void cta::FileSystemNode::addChild(FileSystemNode *const child) {
  if(childExists(child->getEntry().name)) {
    delete child;
    throw Exception("FileSystemNode already exists");
  }

  child->m_parent = this;
  m_children[child->getEntry().name] = child;
}

//------------------------------------------------------------------------------
// hasAtLeastOneChild
//------------------------------------------------------------------------------
bool cta::FileSystemNode::hasAtLeastOneChild() const {
  return !m_children.empty();
}

//------------------------------------------------------------------------------
// childExists
//------------------------------------------------------------------------------
bool cta::FileSystemNode::childExists(const std::string &name) const {
  std::map<std::string, FileSystemNode*>::const_iterator itor =
    m_children.find(name);
  return m_children.end() != itor;
}

//------------------------------------------------------------------------------
// getChild
//------------------------------------------------------------------------------
cta::FileSystemNode &cta::FileSystemNode::getChild(const std::string &name) {
  std::map<std::string, FileSystemNode*>::iterator itor = m_children.find(name);
  if(m_children.end() == itor) {
    throw Exception("No such file or directory");
  }
  if(NULL == itor->second) {
    throw Exception("Unexpected NULL pointer");
  }
  return *(itor->second);
}

//------------------------------------------------------------------------------
// getChild
//------------------------------------------------------------------------------
const cta::FileSystemNode &cta::FileSystemNode::getChild(
  const std::string &name) const {
  std::map<std::string, FileSystemNode*>::const_iterator itor = 
    m_children.find(name);
  if(m_children.end() == itor) {
    throw Exception("No such file or directory");
  }
  if(NULL == itor->second) {
    throw Exception("Unexpected NULL pointer");
  }
  return *(itor->second);
}

//------------------------------------------------------------------------------
// deleteChild
//------------------------------------------------------------------------------
void cta::FileSystemNode::deleteChild(const std::string &name) {
  std::map<std::string, FileSystemNode*>::iterator itor = m_children.find(name);
  if(m_children.end() == itor) {
    throw Exception("No such file or directory");
  }

  delete itor->second;
  m_children.erase(itor);
}
