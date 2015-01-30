#pragma once

#include "ClientAPI.hpp"

#include <map>

namespace cta {

/**
 * A mock entry point to the client ClientAPI of the CERN Tape Archive project.
 */
class MockClientAPI: public ClientAPI {
public:

  /**
   * Constructor.
   */
  MockClientAPI();

  /**
   * Destructor.
   */
  ~MockClientAPI() throw();

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
    const UserIdentity &requester,
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
    const UserIdentity &requester,
    const std::string &name);

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting list of storage
   * classes.
   * @return The current list of storage classes in lexicographical order.
   */
  std::list<StorageClass> getStorageClasses(
    const UserIdentity &requester) const;

  /**
   * Creates the specified directory.
   *
   * @param requester The identity of the user requesting the creation of the
   * directory.
   * @param dirPath The full path of the directory.
   */
  void createDirectory(
    const UserIdentity &requester,
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
    const UserIdentity &requester,
    const std::string &dirPath) const;

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
    const UserIdentity &requester,
    const std::list<std::string> &srcUrls,
    std::string dst);

private:

  /**
   * The current list of storage classes.
   */
  std::map<std::string, StorageClass> m_storageClasses;

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
  void checkAbsolutePathSyntax(const std::string &path);

  /**
   * Throws an exception if the specified path does not start with a slash.
   *
   * @param path The path.
   */
  void checkPathStartsWithASlash(const std::string &path);

  /**
   * Throws an exception if the specified path does not contain valid
   * characters.
   *
   * @param path The path.
   */
  void checkPathContainsValidChars(const std::string &path);

  /**
   * Throws an exception if the specified character cannot be used within a
   * path.
   *
   * @param c The character to be tested.
   */
  void checkValidPathChar(const char c);

  /**
   * Returns true of the specified character can be used within a path.
   */
  bool isValidPathChar(const char c);

  /**
   * Throws an exception if the specified path contains consective slashes.  For
   * example the path "/just_before_consectuive_slashes//file" would cause this
   * method to throw an exception.
   *
   * @param path The path.
   */
  void checkPathDoesContainConsecutiveSlashes(const std::string &path);

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
  std::string getEnclosingDirPath(const std::string &path);

}; // class MockClientAPI

} // namespace cta
