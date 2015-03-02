#pragma once

#include "AdminHost.hpp"
#include "AdminUser.hpp"
#include "ArchivalJob.hpp"
#include "ArchiveRoute.hpp"
#include "DirectoryIterator.hpp"
#include "LogicalLibrary.hpp"
#include "SecurityIdentity.hpp"
#include "StorageClass.hpp"
#include "Tape.hpp"
#include "TapePool.hpp"
#include "UserIdentity.hpp"

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * The administration API of the the middle-tier.
 */
class MiddleTierAdmin {
public:

  /**
   * Destructor.
   */
  virtual ~MiddleTierAdmin() throw() = 0;

  /**
   * Creates the specified administrator.
   *
   * @param requester The identity of the user requesting the creation of the
   * administrator.
   * @param user The identity of the administrator.
   * @param comment The comment describing the sministrator.
   */
  virtual void createAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user,
    const std::string &comment) = 0;

  /**
   * Deletes the specified administrator.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administrator.
   * @param user The identity of the administrator.
   */
  virtual void deleteAdminUser(
    const SecurityIdentity &requester,
    const UserIdentity &user) = 0;

  /**
   * Returns the current list of administrators in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administrators in lexicographical order.
   */
  virtual std::list<AdminUser> getAdminUsers(const SecurityIdentity &requester)
   const = 0;

  /**
   * Creates the specified administration host.
   *
   * @param requester The identity of the user requesting the creation of the
   * administration host.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  virtual void createAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified administration host.
   *
   * @param requester The identity of the user requesting the deletion of the
   * administration host.
   * @param hostName The network name of the administration host.
   * @param comment The comment describing the administration host.
   */
  virtual void deleteAdminHost(
    const SecurityIdentity &requester,
    const std::string &hostName) = 0;

  /**
   * Returns the current list of administration hosts in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of administration hosts in lexicographical order.
   */
  virtual std::list<AdminHost> getAdminHosts(const SecurityIdentity &requester)
   const = 0;

  /**
   * Creates the specified storage class.
   *
   * @param requester The identity of the user requesting the creation of the
   * storage class.
   * @param name The name of the storage class.
   * @param nbCopies The number of copies a file associated with this storage
   * class should have on tape.
   * @param comment The comment describing the storage class.
   */
  virtual void createStorageClass(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint8_t nbCopies,
    const std::string &comment) = 0;

  /**
   * Deletes the specified storage class.
   *
   * @param requester The identity of the user requesting the deletion of the
   * storage class.
   * @param name The name of the storage class.
   */
  virtual void deleteStorageClass(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of storage classes in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of storage classes in lexicographical order.
   */
  virtual std::list<StorageClass> getStorageClasses(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the creation of the
   * tape pool.
   * @param name The name of the tape pool.
   * @param nbDrives The maximum number of drives that can be concurrently
   * assigned to this pool independent of whether they are archiving or
   * retrieving files.
   * @param nbPartialTapes The maximum number of tapes that can be partially
   * full at any moment in time.
   * @param comment The comment describing the tape pool.
   */
  virtual void createTapePool(
    const SecurityIdentity &requester,
    const std::string &name,
    const uint16_t nbDrives,
    const uint32_t nbPartialTapes,
    const std::string &comment) = 0;

  /**
   * Delete the tape pool with the specifed name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape pool.
   * @param name The name of the tape pool.
   */
  virtual void deleteTapePool(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Gets the current list of tape pools in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tape pools in lexicographical order.
   */
  virtual std::list<TapePool> getTapePools(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates the specified archive route.
   *
   * @param requester The identity of the user requesting the creation of the
   * archive route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   * @param tapePoolName The name of the destination tape pool.
   * @param comment The comment describing the archive route.
   */
  virtual void createArchiveRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint8_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment) = 0;

  /**
   * Deletes the specified archive route.
   *
   * @param requester The identity of the user requesting the deletion of the
   * archive route.
   * @param storageClassName The name of the storage class that identifies the
   * source disk files.
   * @param copyNb The tape copy number.
   */
  virtual void deleteArchiveRoute(
    const SecurityIdentity &requester,
    const std::string &storageClassName,
    const uint8_t copyNb) = 0;

  /**
   * Gets the current list of archive routes.
   *
   * @param requester The identity of the user requesting the list.
   */
  virtual std::list<ArchiveRoute> getArchiveRoutes(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a logical library with the specified name.
   *
   * @param requester The identity of the user requesting the creation of the
   * logical library.
   * @param name The name of the logical library.
   * @param comment The comment describing the logical library.
   */
  virtual void createLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name,
    const std::string &comment) = 0;

  /**
   * Deletes the logical library with the specified name.
   *
   * @param requester The identity of the user requesting the deletion of the
   * logical library.
   * @param name The name of the logical library.
   */
  virtual void deleteLogicalLibrary(
    const SecurityIdentity &requester,
    const std::string &name) = 0;

  /**
   * Returns the current list of libraries in lexicographical order.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of libraries in lexicographical order.
   */
  virtual std::list<LogicalLibrary> getLogicalLibraries(
    const SecurityIdentity &requester) const = 0;

  /**
   * Creates a tape.
   *
   * @param requester The identity of the user requesting the creation of the
   * tape.
   * @param vid The volume identifier of the tape.
   * @param logicalLibrary The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param comment The comment describing the logical library.
   */
  virtual void createTape(
    const SecurityIdentity &requester,
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const std::string &comment) = 0;

  /**
   * Deletes the tape with the specified volume identifier.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape.
   * @param vid The volume identifier of the tape.
   */
  virtual void deleteTape(
    const SecurityIdentity &requester,
    const std::string &vid) = 0;

  /**
   * Returns the current list of tapes in the lexicographical order of their
   * volume identifiers.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tapes in the lexicographical order of their
   * volume identifiers.
   */
  virtual std::list<Tape> getTapes(
    const SecurityIdentity &requester) const = 0;

}; // class MiddleTierAdmin

} // namespace cta
