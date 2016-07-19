/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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

#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/ReadTestResult.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RepackType.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/TestSourceType.hpp"
#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/UpdateFileStorageClassRequest.hpp"
#include "common/dataStructures/VerifyInfo.hpp"
#include "common/dataStructures/WriteTestResult.hpp"

#include "common/exception/Exception.hpp"
#include "scheduler/TapeMount.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include <list>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class implementing a tape resource scheduler.
 */
class Scheduler {
  
public:
  
  /**
   * Constructor.
   */
  Scheduler(
    cta::catalogue::Catalogue &catalogue,
    SchedulerDatabase &db, const uint64_t minFilesToWarrantAMount, const uint64_t minBytesToWarrantAMount); //TODO: we have out the mount policy parameters here temporarily we will remove them once we know where to put them

  /**
   * Destructor.
   */
  virtual ~Scheduler() throw();

  /** 
   * Queue an archive request and return the CTA file ID. 
   * Throws a UserError exception in case of wrong request parameters (ex. no route to tape)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  virtual uint64_t queueArchive(const std::string &instanceName, const cta::common::dataStructures::ArchiveRequest &request);
  
  /**
   * Queue a retrieve request. 
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  virtual void queueRetrieve(const std::string &instanceName, const cta::common::dataStructures::RetrieveRequest &request);
  
  /** 
   * Delete an archived file or a file which is in the process of being archived.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  virtual void deleteArchive(const std::string &instanceName, const cta::common::dataStructures::DeleteArchiveRequest &request);
  
  /** 
   * Cancel an ongoing retrieval.
   * Throws a UserError exception in case of wrong request parameters (ex. file not being retrieved)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  virtual void cancelRetrieve(const std::string &instanceName, const cta::common::dataStructures::CancelRetrieveRequest &request);
  
  /** 
   * Update the file information of an archived file.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown file id)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  virtual void updateFileInfo(const std::string &instanceName, const cta::common::dataStructures::UpdateFileInfoRequest &request);
  
  /** 
   * Update the storage class of an archived file.
   * Throws a UserError exception in case of wrong request parameters (ex. unknown storage class)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  virtual void updateFileStorageClass(const std::string &instanceName, const cta::common::dataStructures::UpdateFileStorageClassRequest &request);
  
  /** 
   * List the storage classes that a specific user is allowed to use (the ones belonging to the instance from where
   * the command was issued)
   * Throws a (Non)RetryableError exception in case something else goes wrong with the request
   */
  virtual std::list<cta::common::dataStructures::StorageClass> listStorageClass(const std::string &instanceName, const cta::common::dataStructures::ListStorageClassRequest &request);

  virtual void labelTape(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool force, const bool lbp, const std::string &tag);
  virtual void setTapeBusy(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool busyValue); // internal function not exposed to the Admin CLI
  virtual void setTapeFull(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool fullValue);
  virtual void setTapeDisabled(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool disabledValue);
  virtual void setTapeLbp(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const bool lbpValue); // internal function (noCLI)

  virtual void repack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tag, const cta::common::dataStructures::RepackType);
  virtual void cancelRepack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid);
  virtual std::list<cta::common::dataStructures::RepackInfo> getRepacks(const cta::common::dataStructures::SecurityIdentity &cliIdentity);
  virtual cta::common::dataStructures::RepackInfo getRepack(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid);

  virtual void shrink(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &tapepool); // removes extra tape copies from a specific pool(usually an "_2" pool)

  virtual void verify(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid, const std::string &tag, const uint64_t numberOfFiles); //if last argument is 0, all files are verified
  virtual void cancelVerify(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid);
  virtual std::list<cta::common::dataStructures::VerifyInfo> getVerifys(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const;
  virtual cta::common::dataStructures::VerifyInfo getVerify(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) const;

  virtual cta::common::dataStructures::ReadTestResult readTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid, const uint64_t firstFSeq, const uint64_t lastFSeq, 
   const bool checkChecksum, const std::string &output, const std::string &tag) const; //when output=="null" discard the data read
  virtual cta::common::dataStructures::WriteTestResult writeTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid, const std::string &inputFile, const std::string &tag) const;
  virtual cta::common::dataStructures::WriteTestResult write_autoTest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const std::string &vid, const uint64_t numberOfFiles, const uint64_t fileSize, 
   const cta::common::dataStructures::TestSourceType testSourceType, const std::string &tag) const;

  virtual void setDriveStatus(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &driveName, const bool up, const bool force);

  virtual std::list<cta::common::dataStructures::ArchiveFile> reconcile(const cta::common::dataStructures::SecurityIdentity &cliIdentity); // returns the list of files unknown to EOS, to be deleted manually by the admin after proper checks

  virtual std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > getPendingArchiveJobs() const;
  virtual std::list<cta::common::dataStructures::ArchiveJob> getPendingArchiveJobs(const std::string &tapePoolName) const;
  virtual std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > getPendingRetrieveJobs() const;
  virtual std::list<cta::common::dataStructures::RetrieveJob> getPendingRetrieveJobs(const std::string &vid) const;

  virtual std::list<cta::common::dataStructures::DriveState> getDriveStates(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const;

  virtual std::unique_ptr<TapeMount> getNextMount(const std::string &logicalLibraryName, const std::string &driveName);
  
  virtual void authorizeAdmin(const cta::common::dataStructures::SecurityIdentity &cliIdentity);

private:

  /**
   * The catalogue.
   */
  cta::catalogue::Catalogue &m_catalogue;

  /**
   * The scheduler database.
   */
  SchedulerDatabase &m_db;
  
  const uint64_t m_minFilesToWarrantAMount;
  const uint64_t m_minBytesToWarrantAMount;
}; // class Scheduler

} // namespace cta
