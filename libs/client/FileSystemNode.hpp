#pragma once

#include "DirectoryEntry.hpp"

#include <map>

namespace cta {

/**
 * A node in the file system.
 */
class FileSystemNode {
public:

  /**
   * Constructor.
   */
  FileSystemNode();

  /**
   * Constructor.
   *
   * @param entry The description of the node in the form of a DirectoryEntry.
   */
  FileSystemNode(const DirectoryEntry& entry);

  /**
   * Destructor.
   */
  ~FileSystemNode() throw();

  /**
   * Returns the description of this node in the form of a DirectoryEntry.
   *
   * @return The description of this node in the form of a DirectoryEntry.
   */
  const DirectoryEntry &getEntry() const throw();

  /**
   * Gets the contents of this file system node in the form of a list of
   * DirectoryEntries.
   *
   * @return The contents of this file system node in the form of a list of
   * DirectoryEntries.
   */
  std::list<DirectoryEntry> getDirectoryEntries() const;

  /**
   * Gets the child file-system node corresponding to the specified name.
   */
  FileSystemNode &getChild(const std::string &name);

  /**
   * Gets the child file-system node corresponding to the specified name.
   */
  const FileSystemNode &getChild(const std::string &name) const;

protected:

  /**
   * The description of the node in the form of a DirectoryEntry.
   */
  DirectoryEntry m_entry;

  /**
   * The child nodes as a map from name to node.
   */
  std::map<std::string, FileSystemNode*> m_children;

};

} // namespace cta
