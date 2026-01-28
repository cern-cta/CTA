/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/RetrieveMount.hpp"

#include "catalogue/Catalogue.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/log/TimingList.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/SchedulerInstruments.hpp"
#include "common/utils/Timer.hpp"
#include "disk/DiskSystem.hpp"

#include <algorithm>
#include <iterator>
#include <numeric>
#include <opentelemetry/context/runtime_context.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveMount::RetrieveMount(cta::catalogue::Catalogue& catalogue) : m_catalogue(catalogue) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveMount::RetrieveMount(cta::catalogue::Catalogue& catalogue,
                                  std::unique_ptr<SchedulerDatabase::RetrieveMount> dbMount)
    : m_catalogue(catalogue) {
  m_dbMount.reset(dbMount.release());
}

//------------------------------------------------------------------------------
// getMountType()
//------------------------------------------------------------------------------
cta::common::dataStructures::MountType cta::RetrieveMount::getMountType() const {
  return cta::common::dataStructures::MountType::Retrieve;
}

//------------------------------------------------------------------------------
// getNbFiles()
//------------------------------------------------------------------------------
uint32_t cta::RetrieveMount::getNbFiles() const {
  return m_dbMount->nbFilesCurrentlyOnTape;
}

//------------------------------------------------------------------------------
// getVid()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getVid() const {
  return m_dbMount->mountInfo.vid;
}

//------------------------------------------------------------------------------
// getActivity()
//------------------------------------------------------------------------------
std::optional<std::string> cta::RetrieveMount::getActivity() const {
  return m_dbMount->mountInfo.activity;
}

//------------------------------------------------------------------------------
// getMountTransactionId()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getMountTransactionId() const {
  std::stringstream id;
  if (!m_dbMount.get()) {
    throw exception::Exception("In cta::RetrieveMount::getMountTransactionId(): got nullptr dbMount");
  }
  id << m_dbMount->mountInfo.mountId;
  return id.str();
}

//------------------------------------------------------------------------------
// getMountTapePool()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getPoolName() const {
  std::stringstream sTapePool;
  if (!m_dbMount.get()) {
    throw exception::Exception("In cta::RetrieveMount::getPoolName(): got nullptr dbMount");
  }
  sTapePool << m_dbMount->mountInfo.tapePool;
  return sTapePool.str();
}

//------------------------------------------------------------------------------
// getVo()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getVo() const {
  std::stringstream sVo;
  if (!m_dbMount.get()) {
    throw exception::Exception("In cta::RetrieveMount::getVo(): got nullptr dbMount");
  }
  sVo << m_dbMount->mountInfo.vo;
  return sVo.str();
}

//------------------------------------------------------------------------------
// getMediaType()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getMediaType() const {
  std::stringstream sMediaType;
  if (!m_dbMount.get()) {
    throw exception::Exception("In cta::RetrieveMount::getMediaType(): got nullptr dbMount");
  }
  sMediaType << m_dbMount->mountInfo.mediaType;
  return sMediaType.str();
}

//------------------------------------------------------------------------------
// getVendor()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getVendor() const {
  std::stringstream sVendor;
  if (!m_dbMount.get()) {
    throw exception::Exception("In cta::RetrieveMount::getVendor(): got nullptr dbMount");
  }
  sVendor << m_dbMount->mountInfo.vendor;
  return sVendor.str();
}

//------------------------------------------------------------------------------
// getDrive()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getDrive() const {
  std::stringstream sDrive;
  if (!m_dbMount.get()) {
    throw exception::Exception("In cta::RetrieveMount::getDrive(): got nullptr dbMount");
  }
  sDrive << m_dbMount->mountInfo.drive;
  return sDrive.str();
}

//------------------------------------------------------------------------------
// getCapacityInBytes()
//------------------------------------------------------------------------------
uint64_t cta::RetrieveMount::getCapacityInBytes() const {
  if (!m_dbMount.get()) {
    throw exception::Exception("In cta::RetrieveMount::getCapacityInBytes(): got nullptr dbMount");
  }
  return m_dbMount->mountInfo.capacityInBytes;
}

//------------------------------------------------------------------------------
// getEncryptionKeyName()
//------------------------------------------------------------------------------
std::optional<std::string> cta::RetrieveMount::getEncryptionKeyName() const {
  if (!m_dbMount) {
    throw exception::Exception("In cta::RetrieveMount::getEncryptionKeyName(): got nullptr dbMount");
  }
  return m_dbMount->mountInfo.encryptionKeyName;
}

//------------------------------------------------------------------------------
// getLabelFormat()
//------------------------------------------------------------------------------
cta::common::dataStructures::Label::Format cta::RetrieveMount::getLabelFormat() const {
  if (!m_dbMount.get()) {
    throw exception::Exception("In cta::RetrieveMount::getLabelFormat(): got nullptr dbMount");
  }
  return m_dbMount->mountInfo.labelFormat;
}

//------------------------------------------------------------------------------
// getNextJobBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<cta::RetrieveJob>>
cta::RetrieveMount::getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) {
  if (!m_sessionRunning) {
    throw SessionNotRunning("In RetrieveMount::getNextJobBatch(): trying to get job from complete/not started session");
  }
  utils::Timer t;
  // Try and get a new job from the DB
  auto dbJobBatch = m_dbMount->getNextJobBatch(filesRequested, bytesRequested, logContext);
  std::list<std::unique_ptr<RetrieveJob>> ret;
  // We prepare the response
  uint64_t count = 0;
  for (auto& sdrj : dbJobBatch) {
    ret.emplace_back(std::make_unique<RetrieveJob>(this, std::move(sdrj)));
    // ret.back()->m_dbJob.reset(sdrj.release());
    // ret.back()->m_dbJob = std::move(sdrj);
    count++;
  }
  cta::telemetry::metrics::ctaSchedulerOperationDuration->Record(
    t.msecs(),
    {
      {cta::semconv::attr::kSchedulerOperationName,
       cta::semconv::attr::SchedulerOperationNameValues::kInsertForProcessing},
      {cta::semconv::attr::kSchedulerOperationWorkflow,
       cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve       }
  },
    opentelemetry::context::RuntimeContext::GetCurrent());
  cta::telemetry::metrics::ctaSchedulerOperationJobCount->Add(
    count,
    {
      {cta::semconv::attr::kSchedulerOperationName,
       cta::semconv::attr::SchedulerOperationNameValues::kInsertForProcessing},
      {cta::semconv::attr::kSchedulerOperationWorkflow,
       cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve       }
  },
    opentelemetry::context::RuntimeContext::GetCurrent());
  log::ScopedParamContainer(logContext)
    .add("filesRequested", filesRequested)
    .add("filesFetched", dbJobBatch.size())
    .add("bytesRequested", bytesRequested)
    .add("getNextJobBatchTime", t.secs())
    .log(log::INFO, "In SchedulerDB::RetrieveMount::getNextJobBatch(): Finished getting next job batch.");
  return ret;
}

//------------------------------------------------------------------------------
// requeueJobBatch()
//------------------------------------------------------------------------------
void cta::RetrieveMount::requeueJobBatch(std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs,
                                         log::LogContext& logContext) {
  std::list<std::unique_ptr<SchedulerDatabase::RetrieveJob>> jobBatch;
  for (auto& job : jobs) {
    if (job) {
      jobBatch.emplace_back(job->m_dbJob.release());
    }
  }
  jobs.clear();
  m_dbMount->requeueJobBatch(jobBatch, logContext);
}

//------------------------------------------------------------------------------
// requeueJobBatch
//------------------------------------------------------------------------------
uint64_t cta::RetrieveMount::requeueJobBatch(const std::list<std::string>& jobIDsList,
                                             cta::log::LogContext& logContext) const {
  if (jobIDsList.empty()) {
    logContext.log(cta::log::INFO, "In cta::RetrieveMount::requeueJobBatch(): no job IDs provided to fail.");
    return 0;
  }
  // Forward the job IDs to the database handler's requeueJobBatch method.
  uint64_t njobs = m_dbMount->requeueJobBatch(jobIDsList, logContext);
  return njobs;
}

//------------------------------------------------------------------------------
// checkOrReserveFreeDiskSpaceForRequest() - Helper method to do the common disk space reservation check
//------------------------------------------------------------------------------
bool cta::RetrieveMount::checkOrReserveFreeDiskSpaceForRequest(
  const cta::DiskSpaceReservationRequest& diskSpaceReservationRequest,
  log::LogContext& logContext,
  bool doReserve) {
  if (m_externalFreeDiskSpaceScript.empty()) {
    logContext.log(cta::log::WARNING,
                   "In cta::RetrieveMount::checkOrReserveFreeDiskSpaceForRequest(): missing external script path to "
                   "check EOS free space, backpressure will not be applied");
    return true;
  }
  // Get the current file systems list from the catalogue
  cta::disk::DiskSystemList diskSystemList;
  diskSystemList = m_catalogue.DiskSystem()->getAllDiskSystems();
  diskSystemList.setExternalFreeDiskSpaceScript(m_externalFreeDiskSpaceScript);
  cta::disk::DiskSystemFreeSpaceList diskSystemFreeSpace(diskSystemList);

  // Get the existing reservation map from drives.
  auto previousDrivesReservations = m_catalogue.DriveState()->getDiskSpaceReservations();
  // Get the free space from disk systems involved.
  std::set<std::string> diskSystemNames;
  for (const auto& dsrr : diskSpaceReservationRequest) {
    diskSystemNames.insert(dsrr.first);
  }

  try {
    diskSystemFreeSpace.fetchDiskSystemFreeSpace(diskSystemNames, m_catalogue, logContext);
  } catch (const cta::disk::DiskSystemFreeSpaceListException& ex) {
    // Could not get free space for one of the disk systems due to a script error.
    // The queue will not be put to sleep (backpressure will not be applied), and we return
    // true, because we want to allow staging files for retrieve in case of script errors.
    for (const auto& failedDiskSystem : ex.m_failedDiskSystems) {
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", failedDiskSystem.first)
        .add("failureReason", failedDiskSystem.second.getMessageValue())
        .log(cta::log::WARNING,
             "In cta::RetrieveMount::checkOrReserveFreeDiskSpaceForRequest(): unable to request EOS free space for "
             "disk system using external script, backpressure will not be applied");
    }
    return true;
  } catch (std::exception& ex) {
    // Leave a log message before letting the possible exception go up the stack.
    cta::log::ScopedParamContainer(logContext)
      .add("exceptionWhat", ex.what())
      .log(cta::log::ERR,
           "In cta::RetrieveMount::checkOrReserveFreeDiskSpaceForRequest(): got an exception from "
           "diskSystemFreeSpace.fetchDiskSystemFreeSpace().");
    throw;
  }

  // If a file system does not have enough space fail the disk space reservation,  put the queue to sleep and
  // the retrieve mount will immediately stop
  size_t dscount = 0;
  for (const auto& ds : diskSystemNames) {
    uint64_t previousDrivesReservationTotal = 0;
    auto diskSystem = diskSystemFreeSpace.getDiskSystemList().at(ds);
    // Compute previous drives reservation for the physical space of the current disk system.
    for (const auto& previousDriveReservation : previousDrivesReservations) {
      //avoid empty string when no disk space reservation exists for drive
      if (previousDriveReservation.second != 0) {
        auto previousDiskSystem = diskSystemFreeSpace.getDiskSystemList().at(previousDriveReservation.first);
        if (diskSystem.diskInstanceSpace.freeSpaceQueryURL == previousDiskSystem.diskInstanceSpace.freeSpaceQueryURL) {
          previousDrivesReservationTotal += previousDriveReservation.second;
        }
      }
    }
    if (diskSystemFreeSpace.at(ds).freeSpace < diskSpaceReservationRequest.at(ds)
                                                 + diskSystemFreeSpace.at(ds).targetedFreeSpace
                                                 + previousDrivesReservationTotal) {
      cta::log::ScopedParamContainer(logContext)
        .add("diskSystemName", ds)
        .add("diskSystemCount", diskSystemNames.size())
        .add("diskSystemsPutToSleep", dscount)
        .add("freeSpace", diskSystemFreeSpace.at(ds).freeSpace)
        .add("existingReservations", previousDrivesReservationTotal)
        .add("spaceToReserve", diskSpaceReservationRequest.at(ds))
        .add("targetedFreeSpace", diskSystemFreeSpace.at(ds).targetedFreeSpace)
        .log(cta::log::WARNING,
             "In cta::RetrieveMount::checkOrReserveFreeDiskSpaceForRequest(): could not allocate disk space for job, "
             "applying backpressure");

      auto sleepTime = diskSystem.sleepTime;
      putQueueToSleep(ds, sleepTime, logContext);
      dscount++;
    }
  }
  if (dscount > 0) {
    return false;
  }

  if (doReserve) {
    m_catalogue.DriveState()->reserveDiskSpace(m_dbMount->mountInfo.drive,
                                               m_dbMount->mountInfo.mountId,
                                               diskSpaceReservationRequest,
                                               logContext);
  }
  return true;
}

//------------------------------------------------------------------------------
// reserveDiskSpace()
//------------------------------------------------------------------------------
bool cta::RetrieveMount::reserveDiskSpace(const cta::DiskSpaceReservationRequest& diskSpaceReservationRequest,
                                          log::LogContext& logContext) {
  return checkOrReserveFreeDiskSpaceForRequest(diskSpaceReservationRequest, logContext, true);
}

//------------------------------------------------------------------------------
// testReserveDiskSpace()
//------------------------------------------------------------------------------
bool cta::RetrieveMount::testReserveDiskSpace(const cta::DiskSpaceReservationRequest& diskSpaceReservationRequest,
                                              log::LogContext& logContext) {
  return checkOrReserveFreeDiskSpaceForRequest(diskSpaceReservationRequest, logContext, false);
}

//------------------------------------------------------------------------------
// putQueueToSleep()
//------------------------------------------------------------------------------
void cta::RetrieveMount::putQueueToSleep(const std::string& diskSystemName,
                                         const uint64_t sleepTime,
                                         log::LogContext& logContext) {
  m_dbMount->putQueueToSleep(diskSystemName, sleepTime, logContext);
}

//------------------------------------------------------------------------------
// setJobBatchTransferred()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setJobBatchTransferred(std::queue<std::unique_ptr<cta::RetrieveJob>>& successfulRetrieveJobs,
                                                cta::log::LogContext& logContext) {
#ifdef CTA_PGSCHED
  std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>> validatedSuccessfulDBRetrieveJobs;
#else
  std::list<cta::SchedulerDatabase::RetrieveJob*> validatedSuccessfulDBRetrieveJobs;
#endif
  std::list<std::unique_ptr<cta::RetrieveJob>>
    validatedSuccessfulRetrieveJobs;  //List to ensure the destruction of the retrieve jobs at the end of this method
  std::unique_ptr<cta::RetrieveJob> job;
  double waitUpdateCompletionTime = 0;
  double jobBatchFinishingTime = 0;
  double schedulerDbTime = 0;
  uint64_t files = 0;
  uint64_t bytes = 0;
  utils::Timer t;
  utils::Timer ttel_total;
  auto total_files = 0;
  log::TimingList tl;
  try {
    while (!successfulRetrieveJobs.empty()) {
      job = std::move(successfulRetrieveJobs.front());
      successfulRetrieveJobs.pop();
      if (!job.get()) {
        continue;
      }
      files++;
      bytes += job->archiveFile.fileSize;
#ifdef CTA_PGSCHED
      validatedSuccessfulDBRetrieveJobs.emplace_back(std::move(job->m_dbJob));
#else
      validatedSuccessfulDBRetrieveJobs.emplace_back(job->m_dbJob.get());
#endif
      validatedSuccessfulRetrieveJobs.emplace_back(std::move(job));
      job.reset();
    }
    total_files = files;
    waitUpdateCompletionTime = t.secs(utils::Timer::resetCounter);
    tl.insertAndReset("waitUpdateCompletionTime", t);
    // Complete the cleaning up of the jobs in the mount
    utils::Timer ttel;
#ifdef CTA_PGSCHED
    m_dbMount->setJobBatchTransferred(validatedSuccessfulDBRetrieveJobs, logContext);
#else
    m_dbMount->flushAsyncSuccessReports(validatedSuccessfulDBRetrieveJobs, logContext);
#endif
    cta::telemetry::metrics::ctaSchedulerOperationDuration->Record(
      ttel.msecs(),
      {
        {cta::semconv::attr::kSchedulerOperationName,
         cta::semconv::attr::SchedulerOperationNameValues::kUpdateSchedulerDB},
        {cta::semconv::attr::kSchedulerOperationWorkflow,
         cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve     }
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
    cta::telemetry::metrics::ctaSchedulerOperationJobCount->Add(
      validatedSuccessfulRetrieveJobs.size(),
      {
        {cta::semconv::attr::kSchedulerOperationName,
         cta::semconv::attr::SchedulerOperationNameValues::kUpdateSchedulerDB},
        {cta::semconv::attr::kSchedulerOperationWorkflow,
         cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve     }
    },
      opentelemetry::context::RuntimeContext::GetCurrent());

    jobBatchFinishingTime = t.secs();
    tl.insertOrIncrement("jobBatchFinishingTime", jobBatchFinishingTime);
    schedulerDbTime = jobBatchFinishingTime + waitUpdateCompletionTime;
    tl.insertOrIncrement("schedulerDbTime", schedulerDbTime);
    {
      cta::log::ScopedParamContainer params(logContext);
      params.add("successfulRetrieveJobs", successfulRetrieveJobs.size()).add("files", files).add("bytes", bytes);
      tl.addToLog(params);
      //TODO : if repack, add log to say that the jobs were marked as RJS_Succeeded
      logContext.log(cta::log::DEBUG,
                     "In cta::RetrieveMount::setJobBatchTransferred(): deleted complete retrieve jobs.");
    }
  } catch (const cta::exception::NoSuchObject& ex) {
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionMessageValue", ex.getMessageValue());
    if (job.get()) {
      params.add("fileId", job->archiveFile.archiveFileID)
        .add("diskInstance", job->archiveFile.diskInstance)
        .add("diskFileId", job->archiveFile.diskFileId)
        .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path);
    }
    const std::string msg_error =
      "In cta::RetrieveMount::setJobBatchTransferred(): unable to access jobs, they do not exist in the objectstore.";
    logContext.log(cta::log::WARNING, msg_error);
  } catch (const cta::exception::Exception& e) {
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionMessageValue", e.getMessageValue());
    if (job.get()) {
      params.add("fileId", job->archiveFile.archiveFileID)
        .add("diskInstance", job->archiveFile.diskInstance)
        .add("diskFileId", job->archiveFile.diskFileId)
        .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path);
    }
    const std::string msg_error = "In cta::RetrieveMount::setJobBatchTransferred(): got an exception";
    logContext.log(cta::log::ERR, msg_error);
    logContext.logBacktrace(cta::log::INFO, e.backtrace());
    // Failing here does not really affect the session so we can carry on. Reported jobs are reported, non-reported ones
    // will be retried.
  } catch (const std::exception& e) {
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionWhat", e.what());
    if (job.get()) {
      params.add("fileId", job->archiveFile.archiveFileID)
        .add("diskInstance", job->archiveFile.diskInstance)
        .add("diskFileId", job->archiveFile.diskFileId)
        .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path);
    }
    const std::string msg_error = "In cta::RetrieveMount::setJobBatchTransferred(): got an standard exception";
    logContext.log(cta::log::ERR, msg_error);
    // Failing here does not really affect the session so we can carry on. Reported jobs are reported, non-reported ones
    // will be retried.
  }
  cta::telemetry::metrics::ctaSchedulerOperationDuration->Record(
    ttel_total.msecs(),
    {
      {cta::semconv::attr::kSchedulerOperationName,
       cta::semconv::attr::SchedulerOperationNameValues::kUpdateFinishedTransfer},
      {cta::semconv::attr::kSchedulerOperationWorkflow,
       cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve           }
  },
    opentelemetry::context::RuntimeContext::GetCurrent());
  cta::telemetry::metrics::ctaSchedulerOperationJobCount->Add(
    total_files,
    {
      {cta::semconv::attr::kSchedulerOperationName,
       cta::semconv::attr::SchedulerOperationNameValues::kUpdateFinishedTransfer},
      {cta::semconv::attr::kSchedulerOperationWorkflow,
       cta::semconv::attr::SchedulerOperationWorkflowValues::kRetrieve            }
  },
    opentelemetry::context::RuntimeContext::GetCurrent());
}

//------------------------------------------------------------------------------
// createDiskReporter()
//------------------------------------------------------------------------------
cta::disk::DiskReporter* cta::RetrieveMount::createDiskReporter(std::string& URL) {
  return m_reporterFactory.createDiskReporter(URL);
}

//------------------------------------------------------------------------------
// tapeComplete()
//------------------------------------------------------------------------------
void cta::RetrieveMount::tapeComplete() {
  m_tapeRunning = false;
  if (!m_diskRunning) {
    // Just record we are done with the mount
    m_sessionRunning = false;
  }
}

//------------------------------------------------------------------------------
// diskComplete()
//------------------------------------------------------------------------------
void cta::RetrieveMount::diskComplete() {
  m_diskRunning = false;
  if (!m_tapeRunning) {
    // Just record we are done with the mount
    m_sessionRunning = false;
  }
}

//------------------------------------------------------------------------------
// complete()
//------------------------------------------------------------------------------
void cta::RetrieveMount::complete() {
  diskComplete();
  tapeComplete();
}

//------------------------------------------------------------------------------
// setDriveStatus()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status,
                                        const std::optional<std::string>& reason) {
  m_dbMount->setDriveStatus(status,
                            getMountType(),
                            std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
                            reason);
}

//------------------------------------------------------------------------------
// setTapeSessionStats()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) {
  m_dbMount->setTapeSessionStats(stats);
}

//------------------------------------------------------------------------------
// setTapeMounted()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setTapeMounted(cta::log::LogContext& logContext) const {
  utils::Timer t;
  log::ScopedParamContainer spc(logContext);
  try {
    m_catalogue.Tape()->tapeMountedForRetrieve(m_dbMount->getMountInfo().vid, m_dbMount->getMountInfo().drive);
    auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);
    spc.add("catalogueTime", catalogueTime);
    logContext.log(log::INFO, "In RetrieveMount::setTapeMounted(): success.");
  } catch (cta::exception::Exception&) {
    auto catalogueTimeFailed = t.secs(cta::utils::Timer::resetCounter);
    spc.add("catalogueTime", catalogueTimeFailed);
    logContext.log(cta::log::WARNING, "Failed to update catalogue for the tape mounted for retrieve.");
  }
}

//------------------------------------------------------------------------------
// bothSidesComplete()
//------------------------------------------------------------------------------
bool cta::RetrieveMount::bothSidesComplete() {
  return !(m_diskRunning || m_tapeRunning);
}

//------------------------------------------------------------------------------
// setExternalFreeDiskSpaceScript()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setExternalFreeDiskSpaceScript(const std::string& name) {
  m_externalFreeDiskSpaceScript = name;
}

//------------------------------------------------------------------------------
// addDiskSystemToSkip()
//------------------------------------------------------------------------------
void cta::RetrieveMount::addDiskSystemToSkip(
  const cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip& diskSystem) {
  m_dbMount->addDiskSystemToSkip(diskSystem);
}
