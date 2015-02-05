#pragma once

#include "DirectoryEntry.hpp"
#include "FileSystemStorageClasses.hpp"

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
  FileSystemNode(const DirectoryEntry &entry);

  /**
   * Constructor.
   *
   * @param The storage classes used in the file system.
   * @param entry The description of the node in the form of a DirectoryEntry.
   */
  FileSystemNode(FileSystemStorageClasses &storageclasses,
    const DirectoryEntry &entry);

  /**
   * Destructor.
   */
  ~FileSystemNode() throw();

  /**
   * Gets the parent of this node or throiws an exception if this node does not
   * have a parent.
   */
  FileSystemNode &getParent();

  /**
   * Gets the parent of this node or throws an exception if this node does not
   * have a parent.
   */
  const FileSystemNode &getParent() const;

  /**
   * Returns the description of this node in the form of a DirectoryEntry.
   *
   * @return The description of this node in the form of a DirectoryEntry.
   */
  DirectoryEntry &getEntry() throw();

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
   * Adds the specified node to the children of this node.
   *
   * This method takes ownership of the specified node even in the event of the
   * method throwing an exception.
   *
   * @param node The node to be added.
   */
  void addChild(FileSystemNode *const child);

  /**
   * Returns true if this node has at least one child.
   *
   * @return True if this node has at least one child.
   */
  bool hasAtLeastOneChild() const;

  /**
   * Returns true if the specified child node exists.
   *
   * @param name The name of the child node.
   * @return True if the child node exists.
   */
  bool childExists(const std::string &name) const;

  /**
   * Gets the child file-system node corresponding to the specified name.
   *
   * @param name The name of the child node.
   */
  FileSystemNode &getChild(const std::string &name);

  /**
   * Gets the child file-system node corresponding to the specified name.
   *
   * @param name The name of the child node.
   */
  const FileSystemNode &getChild(const std::string &name) const;

  /**
   * Deletes the child with the specified name.
   *
   * @param name The name of the child node.
   */
  void deleteChild(const std::string &name);

protected:

  /**
   * The storage classes used in the file system.
   */
  FileSystemStorageClasses *m_storageClasses;

  /**
   * Pointer to the parent of this node.
   */
  FileSystemNode *m_parent;

  /**
   * The description of the node in the form of a DirectoryEntry.
   */
  DirectoryEntry m_entry;

  /**
   * The child nodes as a map from name to node.
   */
  std::map<std::string, FileSystemNode*> m_children;

  /**
   * If the file system storage classes are known then this method decremenets
   * the usage count of the storage classs associated with this node.
   */
  void decStorageClassUsageCount();

  /**
   * Deletes and clears the child nodes.
   */
  void deleteAndClearChildren();

}; // class FileSystemNode

} // namespace cta
