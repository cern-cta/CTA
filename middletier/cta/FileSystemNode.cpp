#include "cta/Exception.hpp"
#include "cta/FileSystemNode.hpp"

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
cta::FileSystemNode::FileSystemNode(FileSystemStorageClasses &storageclasses, 
  const DirEntry &entry):
  m_storageClasses(&storageclasses),
  m_entry(FileSystemDirEntry(storageclasses, entry)) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::FileSystemNode::~FileSystemNode() throw() {
  try {
    deleteAndClearChildren();
  } catch(...) {
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
// getFileSystemEntry
//------------------------------------------------------------------------------
cta::FileSystemDirEntry &cta::FileSystemNode::getFileSystemEntry()
  throw() {
  return m_entry;
}

//------------------------------------------------------------------------------
// getFileSystemEntry
//------------------------------------------------------------------------------
const cta::FileSystemDirEntry &cta::FileSystemNode::getFileSystemEntry()
  const throw() {
  return m_entry;
}

//------------------------------------------------------------------------------
// getDirEntries
//------------------------------------------------------------------------------
std::list<cta::DirEntry> cta::FileSystemNode::getDirEntries()
  const {
  std::list<DirEntry> entries;
  for(std::map<std::string, FileSystemNode*>::const_iterator itor =
    m_children.begin(); itor != m_children.end(); itor++) {
    const FileSystemNode *const childNode = itor->second;
    if(NULL == childNode) {
      throw(Exception("getDirEntries encountered a NULL child pointer"));
    }
    const cta::FileSystemDirEntry &childEntry =
      childNode->getFileSystemEntry();
    entries.push_back(childEntry.getEntry());
  }
  return entries;
}

//------------------------------------------------------------------------------
// addChild
//------------------------------------------------------------------------------
void cta::FileSystemNode::addChild(FileSystemNode *const child) {
  if(childExists(child->getFileSystemEntry().getEntry().getName())) {
    delete child;
    throw Exception("FileSystemNode already exists");
  }

  child->m_parent = this;
  m_children[child->getFileSystemEntry().getEntry().getName()] = child;
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
