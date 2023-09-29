/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "scheduler/PostgresSchedDB/ArchiveMount.hpp"
#include "scheduler/PostgresSchedDB/RetrieveMount.hpp"
#include "scheduler/PostgresSchedDB/TapeMountDecisionInfo.hpp"
#include "common/exception/Exception.hpp"
#include "scheduler/PostgresSchedDB/sql/Enums.hpp"
#include "scheduler/PostgresSchedDB/sql/Mounts.hpp"

namespace cta {

namespace postgresscheddb {

TapeMountDecisionInfo::TapeMountDecisionInfo(PostgresSchedDB &pdb, rdbms::ConnPool &cp, const std::string &ownerId, TapeDrivesCatalogueState *drivesState, log::Logger &logger) :
  m_PostgresSchedDB(pdb),
  m_txn(cp),
  m_ownerId(ownerId),
  m_lockTaken(false),
  m_logger(logger),
  m_tapeDrivesState(drivesState)
{
}

std::unique_ptr<SchedulerDatabase::ArchiveMount> TapeMountDecisionInfo::createArchiveMount(
    const cta::SchedulerDatabase::PotentialMount &mount,
    const catalogue::TapeForWriting              &tape,
    const std::string                            &driveName,
    const std::string                            &logicalLibrary,
    const std::string                            &hostName
  )
{

  // To create the mount, we need to:
  //   Check we actually hold the scheduling lock
  //   Indicate which tape to use

  common::dataStructures::JobQueueType queueType;

  switch(mount.type) {
    case common::dataStructures::MountType::ArchiveForUser:
      queueType = common::dataStructures::JobQueueType::JobsToTransferForUser;
      break;
    case common::dataStructures::MountType::ArchiveForRepack:
      queueType = common::dataStructures::JobQueueType::JobsToTransferForRepack;
      break;
    default:
      throw exception::Exception("In TapeMountDecisionInfo::"
        "createArchiveMount(): unexpected mount type.");
  }

  std::unique_ptr<postgresscheddb::ArchiveMount> privateRet(
    new postgresscheddb::ArchiveMount(m_ownerId, m_txn, queueType)
  );

  auto &am = *privateRet;
  // Check we hold the scheduling lock
  if(!m_lockTaken) {
    throw SchedulerDatabase::SchedulingLockNotHeld("In TapeMountDecisionInfo::"
      "createArchiveMount: cannot create mount without holding scheduling lock");
  }

  // Get the next Mount Id
  auto newmount = cta::postgresscheddb::sql::MountsRow::insertMountAndSelect(m_txn, hostName);

  if (!newmount.next()) {
    throw exception::Exception("In TapeMountDecisionInfo::"
      "createArchiveMount(): count not insert mount");
  }

  cta::postgresscheddb::sql::MountsRow newMount(newmount);

  am.nbFilesCurrentlyOnTape    = tape.lastFSeq;
  // Fill up the mount info
  am.mountInfo.mountType       = mount.type;
  am.mountInfo.drive           = driveName;
  am.mountInfo.host            = hostName;
  am.mountInfo.mountId         = newMount.mountId;
  am.mountInfo.tapePool        = tape.tapePool;
  am.mountInfo.logicalLibrary  = logicalLibrary;
  am.mountInfo.vid             = tape.vid;
  am.mountInfo.vo              = tape.vo;
  am.mountInfo.mediaType       = tape.mediaType;
  am.mountInfo.labelFormat     = tape.labelFormat;
  am.mountInfo.vendor          = tape.vendor;
  am.mountInfo.capacityInBytes = tape.capacityInBytes;
  am.mountInfo.encryptionKeyName = tape.encryptionKeyName;

  // todo : check
  commit();

  // Return the mount session object to the user
  std::unique_ptr<SchedulerDatabase::ArchiveMount> ret(privateRet.release());
  return ret;
}

std::unique_ptr<SchedulerDatabase::RetrieveMount> TapeMountDecisionInfo::createRetrieveMount(
    const cta::SchedulerDatabase::PotentialMount& mount,
    const std::string& driveName,
    const std::string& logicalLibrary,
    const std::string& hostName
  )
{
  std::unique_ptr<postgresscheddb::RetrieveMount> privateRet(
    new postgresscheddb::RetrieveMount(m_ownerId, m_txn, mount.vid)
  );

  auto &rm = *privateRet;
  // Check we hold the scheduling lock
  if(!m_lockTaken) {
    throw SchedulerDatabase::SchedulingLockNotHeld("In TapeMountDecisionInfo::"
      "createRetrieveMount: cannot create mount without holding scheduling lock");
  }

  // Get the next Mount Id
  auto newmount = cta::postgresscheddb::sql::MountsRow::insertMountAndSelect(m_txn, hostName);

  if (!newmount.next()) {
    throw exception::Exception("In TapeMountDecisionInfo::"
      "createArchiveMount(): count not insert mount");
  }

  cta::postgresscheddb::sql::MountsRow newMount(newmount);

  // Fill up the mount info
  rm.mountInfo.vid               = mount.vid;
  rm.mountInfo.vo                = mount.vo;
  rm.mountInfo.mediaType         = mount.mediaType;
  rm.mountInfo.labelFormat       = mount.labelFormat.value_or(cta::common::dataStructures::Label::Format::CTA);;
  rm.mountInfo.vendor            = mount.vendor;
  rm.mountInfo.capacityInBytes   = mount.capacityInBytes;
  rm.mountInfo.activity          = mount.activity;
  rm.mountInfo.tapePool          = mount.tapePool;
  rm.mountInfo.encryptionKeyName = mount.encryptionKeyName;
  rm.mountInfo.mountId           = newMount.mountId;
  rm.mountInfo.drive             = driveName;
  rm.mountInfo.logicalLibrary    = logicalLibrary;
  rm.mountInfo.host              = hostName;

  // todo: check
  commit();

  // Return the mount session object to the user
  std::unique_ptr<SchedulerDatabase::RetrieveMount> ret(privateRet.release());
  return ret;
}

void TapeMountDecisionInfo::lock() {
  m_txn.lockGlobal(0);
  m_lockTaken = true;
}

void TapeMountDecisionInfo::commit() {
  m_txn.commit();
  m_lockTaken = false;
}

} //namespace postgresscheddb
} //namespace cta
