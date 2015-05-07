/**
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "cta/FileSystemDirEntry.hpp"
#include "cta/FileSystemStorageClasses.hpp"

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
   * @param storageclasses The storage classes used in the file system.
   * @param entry The description of the node in the form of a DirEntry.
   */
  FileSystemNode(FileSystemStorageClasses &storageclasses,
    const DirEntry &entry);

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
   * Returns the file-system directory-entry.
   *
   * @return The file-system directory-entry.
   */
  FileSystemDirEntry &getFileSystemEntry() throw();

  /**
   * Returns the file-system directory-entry.
   *
   * @return The file-system directory-entry.
   */
  const FileSystemDirEntry &getFileSystemEntry() const throw();

  /**
   * Gets the contents of this file system node in the form of a list of
   * DirectoryEntries.
   *
   * @return The contents of this file system node in the form of a list of
   * DirectoryEntries.
   */
  std::list<DirEntry> getDirEntries() const;

  /**
   * Adds the specified node to the children of this node.
   *
   * This method takes ownership of the specified node even in the event of the
   * method throwing an exception.
   *
   * @param child The child node to be added.
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

private:

  /**
   * The storage classes used in the file system.
   */
  FileSystemStorageClasses *m_storageClasses;

  /**
   * Pointer to the parent of this node.
   */
  FileSystemNode *m_parent;

  /**
   * The file-system diretcory-entry.
   */
  FileSystemDirEntry m_entry;

  /**
   * The child nodes as a map from name to node.
   */
  std::map<std::string, FileSystemNode*> m_children;

  /**
   * Deletes and clears the child nodes.
   */
  void deleteAndClearChildren();

}; // class FileSystemNode

} // namespace cta
