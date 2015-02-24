#pragma once

#include "FileSystemNode.hpp"
#include "FileSystemStorageClasses.hpp"
#include "MiddleTierUser.hpp"
#include "MockDatabase.hpp"
#include "StorageClass.hpp"

namespace cta {

/**
 * A user API of the mock middle-tier.
 */
class MockMiddleTierUser: public MiddleTierUser {
public:

  /**
   * Constructor.
   *
   * @param db The database of the mock middle-tier.
   */
  MockMiddleTierUser(MockDatabase &db);

  /**
   * Destructor.
   */
  ~MockMiddleTierUser() throw();

  /**
   * Creates the specified directory.
   *
   * @param requester The identity of the user requesting the creation of the
   * directory.
   * @param dirPath The full path of the directory.
   */
  void createDirectory(
    const SecurityIdentity &requester,
    const std::string &dirPath);

  /**
   * Deletes the specified directory.
   *
   * @param requester The identity of the user requesting the deletion of the
   * directory.
   * @param dirPath The full path of the directory.
   */
  void deleteDirectory(
    const SecurityIdentity &requester,
    const std::string &dirPath);

  /**
   * Gets the contents of the specified directory.
   *
   * @param requester The identity of the user requesting the contents of the
   * directory.
   * @param dirPath The full path of the directory.
   * @return An iterator over the contents of the directory.
   */
  DirectoryIterator getDirectoryContents(
    const SecurityIdentity &requester,
    const std::string &dirPath) const;

  /**
   * Sets the storage class of the specified directory to the specified value.
   *
   * @param requester The identity of the user requesting the setting of the
   * directory's storage class.
   * @param dirPath The absolute path of the directory.
   * @param storageClassName The name of the storage class.
   */
  void setDirectoryStorageClass(
    const SecurityIdentity &requester,
    const std::string &dirPath,
    const std::string &storageClassName);

  /**
   * Clears the storage class of the specified directory.
   *
   * @param requester The identity of the user requesting the storage class of
   * the directory to be cleared.
   * @param dirPath The absolute path of the directory.
   */
  void clearDirectoryStorageClass(
    const SecurityIdentity &requester,
    const std::string &dirPath);

  /**
   * Gets the storage class if of the specified directory if the directory has
   * one.
   *
   * @param requester The identity of the user requesting the storage class of
   * the directory.
   * @param dirPath The absolute path of the directory.
   * @return The name of the storage class if the directory has one, else an
   * empty string.
   */
  std::string getDirectoryStorageClass(
    const SecurityIdentity &requester,
    const std::string &dirPath) const;

  /**
   * Creates an archival job to asynchronously archive the specified source
   * files to the specified destination within the archive.
   *
   * If there is more than one source file then the destination must be a
   * directory.
   *
   * If there is only one source file then the destination can be either a file
   * or a directory.
   *
   * The storage class of the archived file will be inherited from its
   * destination directory.
   *
   * @param requester The identity of the user requesting the archival.
   * @param srcUrls List of one or more source files.
   * @param dst Destination file or directory within the archive namespace.
   */
  void archive(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    const std::string &dst);

  /**
   * Returns all of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   */
  std::map<TapePool, std::list<ArchivalJob> > getArchivalJobs(
    const SecurityIdentity &requester);

  /**
   * Returns the list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param tapePoolName The name of the tape pool.
   * @return The list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   */
  std::list<ArchivalJob> getArchivalJobs(
    const SecurityIdentity &requester,
    const std::string &tapePoolName);

  /**
   * Creates a retrieval job to asynchronously retrieve the specified archived
   * files and copy them to the specified destination URL.
   *
   * If there is more than one archived file then the destination must be a
   * directory.
   *
   * If there is only one archived file then the destination can be either a
   * file or a directory.
   *
   * @param requester The identity of the user requesting the retrieval.
   * @param srcPaths The list of one of more archived files.
   * @param dstUrl The URL of the destination file or directory.
   */
  void retrieve(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcPaths,
    const std::string &dstUrl);

  /**
   * Returns all of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  std::map<Tape, std::list<RetrievalJob> > getRetrievalJobs(
    const SecurityIdentity &requester);

  /**
   * Returns the list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param vid The volume identifier of the tape.
   * @return The list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  std::list<RetrievalJob> getRetrievalJobs(
    const SecurityIdentity &requester,
    const std::string &vid);

private:

  /**
   * The database of the mock middle-tier.
   */
  MockDatabase &m_db;

  /**
   * Gets the file system node corresponding to the specified path.
   *
   * @path The path.
   * @return The corresponding file system node.
   */
  FileSystemNode &getFileSystemNode(const std::string &path);

  /**
   * Gets the file system node corresponding to the specified path.
   *
   * @path The path.
   * @return The corresponding file system node.
   */
  const FileSystemNode &getFileSystemNode(const std::string &path) const;

  /**
   * Returns true if the specified absolute path is that of an existing
   * directory within the archive namepsace.
   *
   * @param path The specified absolute path.
   * @return True if the specified absolute path is that of an existing
   * directory within the archive namepsace.
   */
  bool isAnExistingDirectory(const std::string &path) const throw();

  /**
   * Archives the specified list of source files to the specified destination
   * directory within the archive namespace.
   *
   * The list of source files should contain at least one file.
   *
   * The storage class of the archived file will be inherited from its
   * destination directory.
   *
   * @param requester The identity of the user requesting the archival.
   * @param srcUrls List of one or more source files.
   * @param dstDir Destination directory within the archive namespace.
   */
  void archiveToDirectory(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    const std::string &dstDir);

  /**
   * Archives the specified list of source files to the specified destination
   * file within the archive namespace.
   *
   * The list of source files should contain one and only one file.
   *
   * The storage class of the archived file will be inherited from its
   * destination directory.
   *
   * @param requester The identity of the user requesting the archival.
   * @param srcUrls List of one or more source files.
   * @param dstFile Destination file within the archive namespace.
   */
  void archiveToFile(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    const std::string &dstFile);

  /**
   * Throws an exception if the specified requester is not authorised to archive
   * a file to the specified destination directory within the archive namespace.
   *
   * @param requester The identity of the user requesting the archival.
   * @param dstDir Destination directory within the archive namespace.
   */
  void checkUserIsAuthorisedToArchive(
    const SecurityIdentity &user,
    const FileSystemNode &dstDir);

}; // class MockMiddleTierUser

} // namespace cta
