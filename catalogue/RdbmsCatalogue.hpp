/*
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

#include "catalogue/Catalogue.hpp"
#include "catalogue/RequesterAndGroupMountPolicies.hpp"
#include "rdbms/Conn.hpp"
#include "rdbms/ConnPool.hpp"

#include <memory>
#include <mutex>

namespace cta {
namespace common {
namespace dataStructures {

/**
 * Forward declaration.
 */
class TapeFile;

} // namespace dataStructures
} // namespace catalogue
} // namespace cta

namespace cta {
namespace catalogue {

/**
 * Forward declaration.
 */
class ArchiveFileRow;

/**
 * CTA catalogue implemented using a relational database backend.
 */
class RdbmsCatalogue: public Catalogue {
protected:

  /**
   * Protected constructor only to be called by sub-classes.
   *
   * @param connFactory The factory for creating new database connections.
   * @param nbConns The maximum number of concurrent connections to be
   * created by the catalogue.
   */
  RdbmsCatalogue(std::unique_ptr<rdbms::ConnFactory> connFactory, const uint64_t nbConns);

public:

  /**
   * Destructor.
   */
  ~RdbmsCatalogue() override;

  /////////////////////////////////////////////////////////////////////
  // START OF METHODS DIRECTLY INVOLVED IN DATA TRANSFER AND SCHEDULING
  /////////////////////////////////////////////////////////////////////

  /**
   * Notifies the catalogue that the specified tape was labelled.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of tape drive that was used to label the tape.
   * @param lbpIsOn Set to true if Logical Block Protection (LBP) was enabled.
   */
  void tapeLabelled(const std::string &vid, const std::string &drive, const bool lbpIsOn) override;

  /**
   * Prepares the catalogue for a new archive file and returns the information
   * required to queue the associated archive request.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * storage class belongs.
   * @param storageClassName The name of the storage class of the file to be
   * archived.  The storage class name is only guaranteed to be unique within
   * its disk instance.  The storage class name will be used by the Catalogue
   * to determine the destination tape pool for each tape copy.
   * @param user The user for whom the file is to be archived.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * archiving the file.
   * @return The information required to queue the associated archive request.
   */
  common::dataStructures::ArchiveFileQueueCriteria prepareForNewFile(
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const common::dataStructures::UserIdentity &user) override;

  /**
   * Returns the list of tapes that can be written to by a tape drive in the
   * specified logical library, in other words tapes that are labelled, not
   * disabled, not full and are in the specified logical library.
   *
   * @param logicalLibraryName The name of the logical library.
   */
  std::list<TapeForWriting> getTapesForWriting(const std::string &logicalLibraryName) const override;

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param event The tape file written event.
   */
  void fileWrittenToTape(const TapeFileWritten &event) override;

  /**
   * Notifies the CTA catalogue that the specified tape has been mounted in
   * order to archive files.
   *
   * The purpose of this method is to keep track of which drive mounted a given
   * tape for archiving files last.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of the drive where the tape was mounted.
   */
  void tapeMountedForArchive(const std::string &vid, const std::string &drive) override;

  /**
   * Prepares for a file retrieval by returning the information required to
   * queue the associated retrieve request(s).
   *
   * @param instanceName The name of the instance from where the retrieval request originated
   * @param archiveFileId The unique identifier of the archived file that is
   * to be retrieved.
   * @param user The user for whom the file is to be retrieved.  This will be
   * used by the Catalogue to determine the mount policy to be used when
   * retrieving the file.
   *
   * @return The information required to queue the associated retrieve request(s).
   */
  common::dataStructures::RetrieveFileQueueCriteria prepareToRetrieveFile(
    const std::string &instanceName,
    const uint64_t archiveFileId,
    const common::dataStructures::UserIdentity &user) override;

  /**
   * Notifies the CTA catalogue that the specified tape has been mounted in
   * order to retrieve files.
   *
   * The purpose of this method is to keep track of which drive mounted a given
   * tape for retrieving files last.
   *
   * @param vid The volume identifier of the tape.
   * @param drive The name of the drive where the tape was mounted.
   */
  void tapeMountedForRetrieve(const std::string &vid, const std::string &drive) override;

  /**
   * This method notifies the CTA catalogue that there is no more free space on
   * the specified tape.
   *
   * @param vid The volume identifier of the tape.
   */
  void noSpaceLeftOnTape(const std::string &vid) override;

  ///////////////////////////////////////////////////////////////////
  // END OF METHODS DIRECTLY INVOLVED IN DATA TRANSFER AND SCHEDULING
  ///////////////////////////////////////////////////////////////////

  void createAdminUser(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) override;
  void deleteAdminUser(const std::string &username) override;
  std::list<common::dataStructures::AdminUser> getAdminUsers() const override;
  void modifyAdminUserComment(const common::dataStructures::SecurityIdentity &admin, const std::string &username, const std::string &comment) override;

  void createAdminHost(const common::dataStructures::SecurityIdentity &admin, const std::string &hostName, const std::string &comment) override;
  void deleteAdminHost(const std::string &hostName) override;
  std::list<common::dataStructures::AdminHost> getAdminHosts() const override;
  void modifyAdminHostComment(const common::dataStructures::SecurityIdentity &admin, const std::string &hostName, const std::string &comment) override;

  /**
   * Creates the specified storage class.
   *
   * @param admin The administrator.
   * @param storageClass The storage class.
   */
  void createStorageClass(
    const common::dataStructures::SecurityIdentity &admin,
    const common::dataStructures::StorageClass &storageClass) override;

  /**
   * Deletes the specified storage class.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * storage class belongs.
   * @param stoargeClassName The name of the storage class which is only
   * guaranteed to be unique within its disk isntance.
   */
  void deleteStorageClass(const std::string &diskInstanceName, const std::string &storageClassName) override;

  std::list<common::dataStructures::StorageClass> getStorageClasses() const override;
  void modifyStorageClassNbCopies(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &name, const uint64_t nbCopies) override;
  void modifyStorageClassComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &name, const std::string &comment) override;

  void createTapePool(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbPartialTapes, const bool encryptionValue, const std::string &comment) override;
  void deleteTapePool(const std::string &name) override;
  std::list<common::dataStructures::TapePool> getTapePools() const override;
  void modifyTapePoolNbPartialTapes(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t nbPartialTapes) override;
  void modifyTapePoolComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;
  void setTapePoolEncryption(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const bool encryptionValue) override;

  void createArchiveRoute(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &diskInstanceName,
    const std::string &storageClassName,
    const uint64_t copyNb,
    const std::string &tapePoolName,
    const std::string &comment) override;


  /**
   * Deletes the specified archive route.
   *
   * @param diskInstanceName The name of the disk instance to which the storage
   * class belongs.
   * @param storageClassName The name of the storage class which is only
   * guaranteed to be unique within its disk instance.
   * @param copyNb The copy number of the tape file.
   */
  void deleteArchiveRoute(
    const std::string &diskInstanceName,
    const std::string &storageClassName, 
    const uint64_t copyNb) override;

  std::list<common::dataStructures::ArchiveRoute> getArchiveRoutes() const override;
  void modifyArchiveRouteTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &storageClassName, const uint64_t copyNb, const std::string &tapePoolName) override;
  void modifyArchiveRouteComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &storageClassName, const uint64_t copyNb, const std::string &comment) override;

  void createLogicalLibrary(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;
  void deleteLogicalLibrary(const std::string &name) override;
  std::list<common::dataStructures::LogicalLibrary> getLogicalLibraries() const override;
  void modifyLogicalLibraryComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  /**
   * Creates a tape.
   *
   * @param encryptionKey The optional identifier of the encrption key.  This
   * optional parameter should either have a non-empty string value or no value
   * at all.  Empty strings are prohibited.
   */
  void createTape(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const optional<std::string> &encryptionKey,
    const uint64_t capacityInBytes,
    const bool disabled,
    const bool full,
    const std::string &comment) override;

  void deleteTape(const std::string &vid) override;

  /**
   * Returns the list of tapes that meet the specified search criteria.
   *
   * @param searchCriteria The search criteria.
   * @return The list of tapes.
   */
  std::list<common::dataStructures::Tape> getTapes(const TapeSearchCriteria &searchCriteria) const override;

  /**
   * Returns the tapes with the specified volume identifiers.
   *
   * This method will throw an exception if it cannot find ALL of the specified
   * tapes.
   *
   * @param vids The tape volume identifiers (VIDs).
   * @return Map from tape volume identifier to tape.
   */
  common::dataStructures::VidToTapeMap getTapesByVid(const std::set<std::string> &vids) const override;

  /**
   * Reclaims the specified tape.
   *
   * This method will throw an exception if the specified tape does not exist.
   *
   * This method will throw an exception if the specified tape is not FULL.
   *
   * This method will throw an exception if there is still at least one tape
   * file recorded in the cataligue as being on the specified tape.
   *
   * @param admin The administrator.
   * @param vid The volume identifier of the tape to be reclaimed.
   */
  void reclaimTape(const common::dataStructures::SecurityIdentity &admin, const std::string &vid) override;

  void modifyTapeLogicalLibraryName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &logicalLibraryName) override;
  void modifyTapeTapePoolName(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &tapePoolName) override;
  void modifyTapeCapacityInBytes(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const uint64_t capacityInBytes) override;
  void modifyTapeEncryptionKey(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &encryptionKey) override;

  /**
   * Sets the full status of the specified tape.
   *
   * Please note that this method is to be called by the CTA front-end in
   * response to a command from the CTA command-line interface (CLI).
   *
   * @param admin The administrator.
   * @param vid The volume identifier of the tape to be marked as full.
   * @param fullValue Set to true if the tape is full.
   */
  void setTapeFull(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool fullValue) override;

  void setTapeDisabled(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const bool disabledValue) override;
  void modifyTapeComment(const common::dataStructures::SecurityIdentity &admin, const std::string &vid, const std::string &comment) override;

  void modifyRequesterMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &mountPolicy) override;
  void modifyRequesteMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterName, const std::string &comment) override;
  void modifyRequesterGroupMountRulePolicy(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &mountPolicy) override;
  void modifyRequesterGroupMountRuleComment(const common::dataStructures::SecurityIdentity &admin, const std::string &instanceName, const std::string &requesterGroupName, const std::string &comment) override;

  void createMountPolicy(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &name,
    const uint64_t archivePriority,
    const uint64_t minArchiveRequestAge,
    const uint64_t retrievePriority,
    const uint64_t minRetrieveRequestAge,
    const uint64_t maxDrivesAllowed,
    const std::string &comment) override;

  /**
   * Returns the list of all existing mount policies.
   *
   * @return the list of all existing mount policies.
   */
  std::list<common::dataStructures::MountPolicy> getMountPolicies() const override;

  /**
   * Deletes the specified mount policy.
   *
   * @param name The name of the mount policy.
   */
  void deleteMountPolicy(const std::string &name) override;

  /**
   * Creates the rule that the specified mount policy will be used for the
   * specified requester.
   *
   * Please note that requester mount-rules overrule requester-group
   * mount-rules.
   *
   * @param admin The administrator.
   * @param mountPolicyName The name of the mount policy.
   * @param diskInstance The name of the disk instance to which the requester
   * belongs.
   * @param requesterName The name of the requester which is only guarantted to
   * be unique within its disk instance.
   * @param comment Comment.
   */
  void createRequesterMountRule(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &mountPolicyName,
    const std::string &diskInstance,
    const std::string &requesterName,
    const std::string &comment) override;

  /**
   * Returns the rules that specify which mount policy is be used for which
   * requester.
   *
   * @return the rules that specify which mount policy is be used for which
   * requester.
   */
  std::list<common::dataStructures::RequesterMountRule> getRequesterMountRules() const override;

  /**
   * Deletes the specified mount rule.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * requester belongs.
   * @param requesterName The name of the requester which is only guaranteed to
   * be unique within its disk instance.
   */
  void deleteRequesterMountRule(const std::string &diskInstanceName, const std::string &requesterName) override;

  /**
   * Creates the rule that the specified mount policy will be used for the
   * specified requester group.
   *
   * Please note that requester mount-rules overrule requester-group
   * mount-rules.
   *
   * @param admin The administrator.
   * @param mountPolicyName The name of the mount policy.
   * @param diskInstanceName The name of the disk instance to which the
   * requester group belongs.
   * @param requesterGroupName The name of the requester group which is only
   * guarantted to be unique within its disk instance.
   * @param comment Comment.
   */
  void createRequesterGroupMountRule(
    const common::dataStructures::SecurityIdentity &admin,
    const std::string &mountPolicyName,
    const std::string &diskInstanceName,
    const std::string &requesterGroupName,
    const std::string &comment) override;

  /**
   * Returns the rules that specify which mount policy is be used for which
   * requester group.
   *
   * @return the rules that specify which mount policy is be used for which
   * requester group.
   */
  std::list<common::dataStructures::RequesterGroupMountRule> getRequesterGroupMountRules() const override;

  /**
   * Deletes the specified mount rule.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * requester group belongs.
   * @param requesterGroupName The name of the requester group which is only
   * guaranteed to be unique within its disk instance.
   */
  void deleteRequesterGroupMountRule(
    const std::string &diskInstanceName,
    const std::string &requesterGroupName) override;

  void modifyMountPolicyArchivePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t archivePriority) override;
  void modifyMountPolicyArchiveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minArchiveRequestAge) override;
  void modifyMountPolicyRetrievePriority(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t retrievePriority) override;
  void modifyMountPolicyRetrieveMinRequestAge(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t minRetrieveRequestAge) override;
  void modifyMountPolicyMaxDrivesAllowed(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const uint64_t maxDrivesAllowed) override;
  void modifyMountPolicyComment(const common::dataStructures::SecurityIdentity &admin, const std::string &name, const std::string &comment) override;

  /**
   * Returns an iterator over the list of archive files that meet the specified
   * search criteria.
   *
   * Please note that the list is ordered by archive file ID.
   *
   * Please note that this method will throw an exception if the
   * nbArchiveFilesToPrefetch parameter is set to 0.  The parameter must be set
   * to a value greater than or equal to 1.
   *
   * @param searchCriteria The search criteria.
   * @param nbArchiveFilesToPrefetch The number of archive files to prefetch.
   * This parameter must be set to a value equal to or greater than 1.
   * @return An iterator over the list of archive files.
   */
  std::unique_ptr<ArchiveFileItor> getArchiveFileItor(const TapeFileSearchCriteria &searchCriteria,
    const uint64_t nbArchiveFilesToPrefetch) const override;

  /**
   * Throws a UserError exception if the specified searchCriteria is not valid
   * due to a user error.
   *
   * @param searchCriteria The search criteria.
   */
  void checkTapeFileSearchCriteria(const TapeFileSearchCriteria &searchCriteria) const;

  /**
   * Returns a summary of the tape files that meet the specified search
   * criteria.
   *
   * @param searchCriteria The search criteria.
   * @return The summary.
   */
  common::dataStructures::ArchiveFileSummary getTapeFileSummary(
    const TapeFileSearchCriteria &searchCriteria) const override;

  /**
   * Returns the archive file with the specified unique identifier.
   *
   * This method assumes that the archive file being requested exists and will
   * therefore throw an exception if it does not.
   *
   * @param id The unique identifier of the archive file.
   * @return The archive file.
   */
  common::dataStructures::ArchiveFile getArchiveFileById(const uint64_t id) override;

  /**
   * Returns true if the specified user running the CTA command-line tool on
   * the specified host has administrator privileges.
   *
   * @param admin The administrator.
   * @return True if the specified user running the CTA command-line tool on
   * the specified host has administrator privileges.
   */
  bool isAdmin(const common::dataStructures::SecurityIdentity &admin) const override;

  /**
   * Returns true if SCHEMA_STATUS column of the CTA_TABLE contains the value LOCKED.
   *
   * @return True if SCHEMA_STATUS column of the CTA_TABLE contains the value LOCKED.
   */
  bool schemaIsLocked() const override;

  /**
   * Sets the value of the SCHEMA_STATUS column of the CTA_TABLE to LOCKED.
   *
   * Please note that this method is idempotent.
   */
  void lockSchema() override;

  /**
   * Sets the value of the SCHEMA_STATUS column of the CTA_TABLE to UNLOCKED.
   *
   * Please note that this method is idempotent.
   */
  void unlockSchema() override;

  /**
   * Checks that the most trivial query goes through. Returns true on success,
   * false on failure.
   * 
   * @return True if the query went through.
   */
  void ping() override;

protected:

  /**
   * Mutex to be used to a take a global lock on the database.
   */
  std::mutex m_mutex;

  /**
   * The database connection factory.
   */
  std::unique_ptr<rdbms::ConnFactory> m_connFactory;

  /**
   * The pool of connections to the underlying relational database.
   */
  mutable rdbms::ConnPool m_connPool;

  /**
   * Returns true if the specified admin user exists.
   *
   * @param conn The database connection.
   * @param adminUsername The name of the admin user.
   * @return True if the admin user exists.
   */
  bool adminUserExists(rdbms::Conn &conn, const std::string adminUsername) const;

  /**
   * Returns true if the specified admin host exists.
   *
   * @param conn The database connection.
   * @param adminHost The name of the admin host.
   * @return True if the admin host exists.
   */
  bool adminHostExists(rdbms::Conn &conn, const std::string adminHost) const;

  /**
   * Returns true if the specified storage class exists.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the storage
   * class belongs.
   * @param storageClassName The name of the storage class.
   * @return True if the storage class exists.
   */
  bool storageClassExists(rdbms::Conn &conn, const std::string &diskInstanceName, const std::string &storageClassName)
    const;

  /**
   * Returns true if the specified tape pool exists.
   *
   * @param conn The database connection.
   * @param tapePoolName The name of the tape pool.
   * @return True if the tape pool exists.
   */
  bool tapePoolExists(rdbms::Conn &conn, const std::string &tapePoolName) const;

  /**
   * Returns true if the specified archive file identifier exists.
   *
   * @param conn The database connection.
   * @param archiveFileId The archive file identifier.
   * @return True if the archive file identifier exists.
   */
  bool archiveFileIdExists(rdbms::Conn &conn, const uint64_t archiveFileId) const;

  /**
   * Returns true if the specified disk file group exists.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the disk
   * file group belongs.
   * @param diskFileGroup The name of the disk file group
   * @return True if the disk file group exists.
   */
  bool diskFileGroupExists(rdbms::Conn &conn, const std::string &diskInstanceName, const std::string &diskFileGroup)
    const;

  /**
   * Returns true if the specified archive route exists.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the storage
   * class belongs.
   * @param storageClassName The name of the storage class which is only
   * guaranteed to be unique within its disk instance.
   * @param copyNb The copy number of the tape file.
   * @return True if the archive route exists.
   */
  bool archiveRouteExists(rdbms::Conn &conn, const std::string &diskInstanceName, const std::string &storageClassName,
    const uint64_t copyNb) const;

  /**
   * Returns true if the specified tape exists.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   * @return True if the tape exists.
   */
  bool tapeExists(rdbms::Conn &conn, const std::string &vid) const;

  /**
   * Returns the list of tapes that meet the specified search criteria.
   *
   * @param conn The database connection.
   * @param searchCriteria The search criteria.
   * @return The list of tapes.
   */
  std::list<common::dataStructures::Tape> getTapes(rdbms::Conn &conn, const TapeSearchCriteria &searchCriteria) const;

  /**
   * Returns true if the specified logical library exists.
   *
   * @param conn The database connection.
   * @param logicalLibraryName The name of the logical library.
   * @return True if the logical library exists.
   */
  bool logicalLibraryExists(rdbms::Conn &conn, const std::string &logicalLibraryName) const;

  /**
   * Returns true if the specified mount policy exists.
   *
   * @param conn The database connection.
   * @param mountPolicyName The name of the mount policy
   * @return True if the mount policy exists.
   */
  bool mountPolicyExists(rdbms::Conn &conn, const std::string &mountPolicyName) const;

  /**
   * Returns true if the specified requester mount-rule exists.
   *
   * @param diskInstanceName The name of the disk instance to which the
   * requester belongs.
   * @param requesterName The username of the requester which is only guaranteed
   * to be unique within its disk instance.
   * @return True if the requester mount-rule exists.
   */
  bool requesterMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
    const std::string &requesterName) const;

  /**
   * Returns the specified requester mount-policy or nullptr if one does not exist.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the
   * requester belongs.
   * @param requesterName The name of the requester which is only guaranteed to
   * be unique within its disk instance.
   * @return The mount policy or nullptr if one does not exists.
   */
  common::dataStructures::MountPolicy *getRequesterMountPolicy(
    rdbms::Conn &conn,
    const std::string &diskInstanceName,
    const std::string &requesterName) const;

  /**
   * Returns true if the specified requester-group mount-rule exists.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the
   * requester group belongs.
   * @param requesterGroupName The name of the requester group which is only
   * guaranteed to be unique within its disk instance.
   * @return True if the requester-group mount-rule exists.
   */
  bool requesterGroupMountRuleExists(rdbms::Conn &conn, const std::string &diskInstanceName,
    const std::string &requesterGroupName) const;

  /**
   * Returns the specified requester-group mount-policy or nullptr if one does not
   * exist.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the
   * requester group belongs.
   * @param requesterGroupName The name of the requester group which is only
   * guaranteed to be unique within its disk instance.
   * @return The mount policy or nullptr if one does not exists.
   */
  common::dataStructures::MountPolicy *getRequesterGroupMountPolicy(rdbms::Conn &conn,
    const std::string &diskInstanceName, const std::string &requesterGroupName) const;

  /**
   * Returns the specified tape log information from the specified database
   * result set.
   *
   * @param rset The result set.
   * @param driveColName The name of the database column that contains the name
   * of the tape drive.
   * @param timeColNAme The name of the database column that contains the time
   * stamp.
   */
  optional<common::dataStructures::TapeLog> getTapeLogFromRset(const rdbms::Rset &rset,
    const std::string &driveColName, const std::string &timeColName) const;

  /**
   * An RdbmsCatalogue specific method that inserts the specified row into the
   * ArchiveFile table.
   *
   * @param conn The database connection.
   * @param row The row to be inserted.
   */
  void insertArchiveFile(rdbms::Conn &conn, const ArchiveFileRow &row);

  /**
   * Creates the database schema.
   */
  void createDbSchema();

  /**
   * Returns true if the specified user name is listed in the ADMIN_USER table.
   *
   * @param conn The database connection.
   * @param userName The name of the user to be search for in the ADMIN_USER
   * table.
   * @return true if the specified user name is listed in the ADMIN_USER table.
   */
  bool userIsAdmin(rdbms::Conn &conn, const std::string &userName) const;

  /**
   * Returns true if the specified host name is listed in the ADMIN_HOST table.
   *
   * @param conn The database connection.
   * @param userName The name of the host to be search for in the ADMIN_HOST
   * table.
   * @return true if the specified host name is listed in the ADMIN_HOST table.
   */
  bool hostIsAdmin(rdbms::Conn &conn, const std::string &userName) const;

  /**
   * Returns the expected number of archive routes for the specified storage
   * class as specified by the call to the createStorageClass() method as
   * opposed to the actual number entered so far using the createArchiveRoute()
   * method.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the storage
   * class belongs.
   * @param storagleClassName The name of the storage class which is only
   * guaranteed to be unique within its disk instance.
   * @return The expected number of archive routes.
   */
  uint64_t getExpectedNbArchiveRoutes(rdbms::Conn &conn, const std::string &diskInstanceName,
    const std::string &storageClassNAme) const;

  /**
   * Inserts the specified tape file into the Tape table.
   *
   * @param conn The database connection.
   * @param tapeFile The tape file.
   * @param archiveFileId The identifier of the archive file of which the tape
   * file is a copy.
   */
  void insertTapeFile(
    rdbms::Conn &conn,
    const common::dataStructures::TapeFile &tapeFile,
    const uint64_t archiveFileId);

  /**
   * Sets the last FSeq of the specified tape to the specified value.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   * @param lastFseq The new value of the last FSeq.
   */
  void setTapeLastFSeq(rdbms::Conn &conn, const std::string &vid, const uint64_t lastFSeq);

  /**
   * Returns the last FSeq of the specified tape.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   * @return The last FSeq.
   */
  uint64_t getTapeLastFSeq(rdbms::Conn &conn, const std::string &vid) const;

  /**
   * Updates the appropriate tape based on the occurrence of the specified
   * event.
   *
   * @param conn The database connection.
   * @param event
   */
  void updateTape(rdbms::Conn &conn, const TapeFileWritten &event);

  /**
   * Returns the specified archive file or a nullptr pointer if it does not
   * exist.
   *
   * @param conn The database connection.
   * @param archiveFileId The identifier of the archive file.
   * @return The archive file or nullptr.
   * an empty list.
   */
  std::unique_ptr<common::dataStructures::ArchiveFile> getArchiveFile(rdbms::Conn &conn, const uint64_t archiveFileId) const;

  /**
   * Throws an exception if the there is a mismatch between the expected and
   * actual common event data.
   *
   * @param expected The expected event data.
   * @param actual The actual event data.
   */
  void throwIfCommonEventDataMismatch(const common::dataStructures::ArchiveFile &expected,
    const TapeFileWritten &actual) const;

  /**
   * Returns the mount policies for the specified requester and requester group.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the
   * requester and requester group belong.
   * @param requesterName The name of the requester which is only guaranteed to
   * be unique within its disk instance.
   * @param requesterGroupName The name of the requester group which is only
   * guaranteed to be unique within its disk instance.
   * @return The mount policies.
   */
  RequesterAndGroupMountPolicies getMountPolicies(
    rdbms::Conn &conn,
    const std::string &diskInstanceName,
    const std::string &requesterName,
    const std::string &requesterGroupName) const;

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   *
   * @param conn The database connection.
   * @return A unique archive ID that can be used by a new archive file within
   * the catalogue.
   */
  virtual uint64_t getNextArchiveFileId(rdbms::Conn &conn) = 0;

  /**
   * Selects the specified tape within the Tape table for update.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because some database technologies directly support SELECT FOR UPDATE
   * whilst others do not.
   *
   * @param conn The database connection.
   * @param vid The volume identifier of the tape.
   */
  virtual common::dataStructures::Tape selectTapeForUpdate(rdbms::Conn &conn, const std::string &vid) = 0;

  /**
   * Nested class used to implement the getArchiveFileItor() method.
   */
  class ArchiveFileItorImpl: public ArchiveFileItor {
  public:

    /**
     * Constructor.
     *
     * @param catalogue The RdbmsCatalogue.
     * @param nbArchiveFilesToPrefetch The number of archive files to prefetch.
     * @param searchCriteria The search criteria.
     */
    ArchiveFileItorImpl(
      const RdbmsCatalogue &catalogue,
      const uint64_t nbArchiveFilesToPrefetch,
      const TapeFileSearchCriteria &searchCriteria);

    /**
     * Destructor.
     */
    ~ArchiveFileItorImpl() override;

    /**
     * Returns true if a call to next would return another archive file.
     */
    bool hasMore() const override;

    /**
     * Returns the next archive or throws an exception if there isn't one.
     */
    common::dataStructures::ArchiveFile next() override;

  private:

    /**
     * The RdbmsCatalogue.
     */
    const RdbmsCatalogue &m_catalogue;

    /**
     * The number of archive files to prefetch.
     */
    const uint64_t m_nbArchiveFilesToPrefetch;

    /**
     * The search criteria.
     */
    TapeFileSearchCriteria m_searchCriteria;

    /**
     * The current offset into the list of archive files in the form of an
     * archive file ID.
     */
    uint64_t m_nextArchiveFileId;

    /**
     * The current list of prefetched archive files.
     */
    std::list<common::dataStructures::ArchiveFile> m_prefechedArchiveFiles;
  }; // class ArchiveFileItorImpl

  /**
   * Returns the specified archive files.  This method is called by the nested
   * class ArchiveFileItorImpl.  Please note that the list of files is ordered
   * by archive file IDs.
   *
   * @param startingArchiveFileId The unique identifier of the first archive
   * file to be returned.
   * @param nbArchiveFiles The maximum number of archive files to be returned.
   * @param searchCriteria The search criteria.
   * @return The archive files.
   */
  std::list<common::dataStructures::ArchiveFile> getArchiveFilesForItor(
    const uint64_t startingArchiveFileId,
    const uint64_t maxNbArchiveFiles,
    const TapeFileSearchCriteria &searchCriteria) const;

  /**
   * Returns the mapping from tape copy to tape pool for the specified storage
   * class.
   *
   * @param conn The database connection.
   * @param diskInstanceName The name of the disk instance to which the storage
   * class belongs.
   * @param storageClassName The name of the storage class which is only
   * guaranteed to be unique within its disk instance.
   * @return The mapping from tape copy to tape pool for the specified storage
   * class.
   */
  common::dataStructures::TapeCopyToPoolMap getTapeCopyToPoolMap(
    rdbms::Conn &conn,
    const std::string &diskInstanceName,
    const std::string &storageClassName) const;

}; // class RdbmsCatalogue

} // namespace catalogue
} // namespace cta
