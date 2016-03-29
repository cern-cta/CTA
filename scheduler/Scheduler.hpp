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

#include "common/dataStructures/AdminHost.hpp"
#include "common/dataStructures/AdminUser.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/dataStructures/ArchiveFileSummary.hpp"
#include "common/dataStructures/ArchiveJob.hpp"
#include "common/dataStructures/ArchiveRequest.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/dataStructures/CancelRetrieveRequest.hpp"
#include "common/dataStructures/Dedication.hpp"
#include "common/dataStructures/DedicationType.hpp"
#include "common/dataStructures/DeleteArchiveRequest.hpp"
#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/EntryLog.hpp"
#include "common/dataStructures/ListStorageClassRequest.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/MountType.hpp"
#include "common/dataStructures/ReadTestResult.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "common/dataStructures/RepackType.hpp"
#include "common/dataStructures/RetrieveJob.hpp"
#include "common/dataStructures/RetrieveRequest.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/dataStructures/StorageClass.hpp"
#include "common/dataStructures/TapeFileLocation.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/dataStructures/TapePool.hpp"
#include "common/dataStructures/TestSourceType.hpp"
#include "common/dataStructures/UpdateFileInfoRequest.hpp"
#include "common/dataStructures/MountGroup.hpp"
#include "common/dataStructures/User.hpp"
#include "common/dataStructures/UserIdentity.hpp"
#include "common/dataStructures/VerifyInfo.hpp"
#include "common/dataStructures/WriteTestResult.hpp"

#include "common/exception/Exception.hpp"
#include "scheduler/TapeMount.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include "common/forwardDeclarations.hpp"

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
    SchedulerDatabase &db);

  /**
   * Destructor.
   */
  virtual ~Scheduler() throw();

  virtual uint64_t queueArchiveRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::ArchiveRequest &request);
  virtual void queueRetrieveRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::RetrieveRequest &request);
  virtual void deleteArchiveRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::DeleteArchiveRequest &request);
  virtual void cancelRetrieveRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::CancelRetrieveRequest &request);
  virtual void updateFileInfoRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::UpdateFileInfoRequest &request);
  virtual std::list<cta::common::dataStructures::StorageClass> listStorageClassRequest(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const cta::common::dataStructures::ListStorageClassRequest &request);

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

  virtual std::map<std::string, std::list<cta::common::dataStructures::ArchiveJob> > getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const;
  virtual std::list<cta::common::dataStructures::ArchiveJob> getPendingArchiveJobs(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &tapePoolName) const;
  virtual std::map<std::string, std::list<cta::common::dataStructures::RetrieveJob> > getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const;
  virtual std::list<cta::common::dataStructures::RetrieveJob> getPendingRetrieveJobs(const cta::common::dataStructures::SecurityIdentity &cliIdentity, const std::string &vid) const;

  virtual std::list<cta::common::dataStructures::DriveState> getDriveStates(const cta::common::dataStructures::SecurityIdentity &cliIdentity) const;

  virtual std::unique_ptr<TapeMount> getNextMount(const std::string &logicalLibraryName, const std::string &driveName);
  
  virtual std::unique_ptr<TapeMount> _old_getNextMount(const std::string &logicalLibraryName, const std::string & driveName);
  
  virtual void authorizeCliIdentity(const cta::common::dataStructures::SecurityIdentity &cliIdentity);

private:

  /**
   * The catalogue.
   */
  cta::catalogue::Catalogue &m_catalogue;

  /**
   * The scheduler database.
   */
  SchedulerDatabase &m_db;
}; // class Scheduler

} // namespace cta
