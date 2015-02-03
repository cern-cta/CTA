#include "Exception.hpp"
#include "FileSystemNode.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemNode::FileSystemNode() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::FileSystemNode::FileSystemNode(const DirectoryEntry& entry):
  m_entry(entry) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::FileSystemNode::~FileSystemNode() throw() {
  for(std::map<std::string, FileSystemNode*>::const_iterator itor =
    m_children.begin(); itor != m_children.end(); itor++) {
    delete(itor->second);
  }
  m_children.clear();
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

  m_children[child->getEntry().name] = child;
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
