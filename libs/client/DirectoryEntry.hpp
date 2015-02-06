#pragma once

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * A directory entry.
 */
class DirectoryEntry {
public:

  /**
   * Enumeration of the different possible type so directory entry.
   */
  enum EntryType {
    ENTRYTYPE_NONE,
    ENTRYTYPE_FILE,
    ENTRYTYPE_DIRECTORY};

  /**
   * Thread safe method that returns the string reprsentation of the specified
   * enumeration value.
   */
  static const char *entryTypeToStr(const EntryType enumValue) throw();

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   */
  DirectoryEntry();

  /**
   * Constructor.
   *
   * Initialises all integer member-variables to 0.
   *
   * @param entryType The type of the entry.
   * @param name The name of the directory entry.
   * @param storageClassName The name of the directory's storage class or an
   * empty string if the directory does not have a storage class.
   */
  DirectoryEntry(const EntryType entryType, const std::string &name,
    const std::string &storageClassName);

  /**
   * Returns the type of the directory entry.
   *
   * @return The type of the directory entry.
   */
  EntryType getEntryType() const throw();

  /**
   * Returns the name of the directory entry.
   *
   * @return The name of the directory entry.
   */
  const std::string &getName() const throw();

  /**
   * Returns the user ID of the directory entry's owner.
   *
   * @return The user ID of the directory entry's owner.
   */
  uint32_t getOwnerId() const throw();

  /**
   * Returns the group ID of the directory entry.
   *
   * @return The group ID of the directory entry.
   */
  uint32_t getGroupId() const throw();

  /**
   * Returns the mode bits of the directory entry.
   *
   * @return The mode bits of the directory entry.
   */
  uint16_t getMode() const throw();

  /**
   * Sets the name of the storage class.
   *
   * @param storageClassName The name of the storage class.
   */
  void setStorageClassName(const std::string &storageClassName);

  /**
   * Returns the name of the directory's storage class or an empty string if the
   * directory does not have a storage class.
   *
   * @return The name of the directory's storage class or an empty string if the
   * directory does not have a storage class.
   */
  const std::string &getStorageClassName() const throw();

private:

  /**
   * The type of the directory entry.
   */
  EntryType m_entryType;

  /**
   * The name of the directory entry.
   */
  std::string m_name;

  /**
   * The user ID of the directory entry's owner.
   */
  uint32_t m_ownerId;

  /**
   * The group ID of the directory entry.
   */
  uint32_t m_groupId;

  /**
   * The mode bits of the directory entry.
   */
  uint16_t m_mode;

  /**
   * The name of the directory's storage class or an empty string if the
   * directory does not have a storage class.
   */
  std::string m_storageClassName;

}; // class DirectoryEntry

} // namespace cta
