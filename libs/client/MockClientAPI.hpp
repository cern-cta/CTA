#pragma once

#include "ClientAPI.hpp"
#include "FileSystemNode.hpp"

#include <map>
#include <vector>

namespace cta {

/**
 * A mock entry point to the client ClientAPI of the CERN Tape Archive project.
 */
class MockClientAPI: public ClientAPI {
public:

  /**
   * Constructor.
   *
   * Creates the root directory "/" owned by user root and with no file
   * attributes or permissions.
   */
  MockClientAPI();

  /**
   * Destructor.
   */
  ~MockClientAPI() throw();

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the user requesting the creation of the
   * administrator.
   * @param adminUser The identity of the administrator.
   */
  void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &adminUser);

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administrator.
   * @param adminUser The identity of the administrator.
   */
  void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &adminUser);

  /**
   * Returns the current list of administrators.
   *
   * @param requester The identity of the user requesting the list.
   */
  std::list<UserIdentity> getAdminUsers(const SecurityIdentity &requester) const;

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the user requesting the creation of the
   * administration host.
   * @param adminHost The network name of the administration host.
   */
  void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &adminHost);

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administration host.
   * @param adminHost The network name of the administration host.
   */
  void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &adminHost);

  /**
   * Returns the current list of administration hosts.
   *
   * @param requester The identity of the user requesting the list.
   */
  std::list<std::string> getAdminHosts(const SecurityIdentity &requester) const;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   */
  void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint8_t nbCopies);

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   */
  void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name);

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of storage classes in lexicographical order.
   */
  std::list<StorageClass> getStorageClasses(
    const SecurityIdentity &requester) const;

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
    const std::string &dirPath);

  /**
   * Archives the specified list of source files to the specified destination
   * within the archive namespace.
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
   * @return The identifier of the archive job.
   */
  std::string archiveToTape(
    const SecurityIdentity &requester,
    const std::list<std::string> &srcUrls,
    std::string dst);

protected:

  /**
   * The current list of administrators.
   */
  std::list<UserIdentity> m_adminUsers;

  /**
   * The current list of administration hosts.
   */
  std::list<std::string> m_adminHosts;

  /**
   * The current mapping from storage class name to storage classes.
   */
  std::map<std::string, StorageClass> m_storageClasses;

  /**
   * The root node of the file-system.
   */
  FileSystemNode m_fileSystemRoot;

  /**
   * Throws an exception if the specified administrator already exists.
   *
   * @param adminUser The identity of the administrator.
   */
  void checkAdminUserDoesNotAlreadyExist(const UserIdentity &adminUser);

  /**
   * Throws an exception if the specified administration host already exists.
   *
   * @param adminHost The network name of the administration host.
   */
  void checkAdminHostDoesNotAlreadyExist(const std::string &adminHost);

  /**
   * Throws an exception if the specified storage class already exists.
   *
   * @param name The name of teh storage class.
   */
  void checkStorageClassDoesNotAlreadyExist(const std::string &name) const;

  /**
   * Throws an exception if the specified storage class does not exist.
   *
   * @param name The name of the storage class.
   */
  void checkStorageClassExists(const std::string &name) const;

  /**
   * Throws an exception if the specified absolute path constains a
   * syntax error.
   *
   * @param path The Absolute path.
   */
  void checkAbsolutePathSyntax(const std::string &path) const;

  /**
   * Throws an exception if the specified path does not start with a slash.
   *
   * @param path The path.
   */
  void checkPathStartsWithASlash(const std::string &path) const;

  /**
   * Throws an exception if the specified path does not contain valid
   * characters.
   *
   * @param path The path.
   */
  void checkPathContainsValidChars(const std::string &path) const;

  /**
   * Throws an exception if the specified character cannot be used within a
   * path.
   *
   * @param c The character to be tested.
   */
  void checkValidPathChar(const char c) const;

  /**
   * Returns true of the specified character can be used within a path.
   */
  bool isValidPathChar(const char c) const;

  /**
   * Throws an exception if the specified path contains consective slashes.  For
   * example the path "/just_before_consectuive_slashes//file" would cause this
   * method to throw an exception.
   *
   * @param path The path.
   */
  void checkPathDoesContainConsecutiveSlashes(const std::string &path) const;

  /**
   * Returns the path of the enclosing directory of the specified path.
   *
   * For example:
   *
   * * path="/grandparent/parent/child" would return "/grandparent/parent"
   * * path="/grandparent" would return "/grandparent"
   * * path="/" would return "" where empty string means no enclosing directoyr
   *
   * @param path The path.
   * @return The path of the enclosing directory.
   */
  std::string getEnclosingDirPath(const std::string &path) const;

  /**
   * Returns the name of the enclosed file or directory of the specified path.
   *
   * @param path The path.
   * @return The name of the enclosed file or directory.
   */
  std::string getEnclosedName(const std::string &path) const;

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
   * Returns the result of trimming both left and right slashes from the
   * specified string.
   *
   * @param s The string to be trimmed.
   * @return The result of trimming the string.
   */
  std::string trimSlashes(const std::string &s) const throw();

  /**
   * Splits the specified string into a vector of strings using the specified
   * separator.
   *
   * Please note that the string to be split is NOT modified.
   *
   * @param str The string to be split.
   * @param separator The separator to be used to split the specified string.
   * @param result The vector when the result of spliting the string will be
   * stored.
   */
  void splitString(const std::string &str, const char separator,
    std::vector<std::string> &result) const throw();

}; // class MockClientAPI

} // namespace cta
