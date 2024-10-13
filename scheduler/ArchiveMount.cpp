/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "scheduler/ArchiveMount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveMount::ArchiveMount(catalogue::Catalogue& catalogue) : m_catalogue(catalogue), m_sessionRunning(false) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveMount::ArchiveMount(catalogue::Catalogue& catalogue,
                                std::unique_ptr<SchedulerDatabase::ArchiveMount> dbMount) : m_catalogue(catalogue),
                                                                                            m_sessionRunning(false) {
  m_dbMount.reset(
    dynamic_cast<SchedulerDatabase::ArchiveMount *>(dbMount.release()));
  if (!m_dbMount) {
    throw WrongMountType(std::string(__FUNCTION__) +
                         ": could not cast mount to SchedulerDatabase::ArchiveMount");
  }
}

//------------------------------------------------------------------------------
// getMountType
//------------------------------------------------------------------------------
cta::common::dataStructures::MountType cta::ArchiveMount::getMountType() const {
  return m_dbMount->mountInfo.mountType;
}

//------------------------------------------------------------------------------
// getVid
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getVid() const {
  return m_dbMount->mountInfo.vid;
}

//------------------------------------------------------------------------------
// getDrive
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getDrive() const {
  return m_dbMount->mountInfo.drive;
}


//------------------------------------------------------------------------------
// getPoolName
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getPoolName() const {
  return m_dbMount->mountInfo.tapePool;
}

//------------------------------------------------------------------------------
// getVo
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getVo() const {
  return m_dbMount->mountInfo.vo;
}

//------------------------------------------------------------------------------
// getMediaType
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getMediaType() const {
  return m_dbMount->mountInfo.mediaType;
}

//------------------------------------------------------------------------------
// getVendor
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getVendor() const {
  return m_dbMount->mountInfo.vendor;
}

//------------------------------------------------------------------------------
// getCapacityInBytes
//------------------------------------------------------------------------------
uint64_t cta::ArchiveMount::getCapacityInBytes() const {
  return m_dbMount->mountInfo.capacityInBytes;
}

//------------------------------------------------------------------------------
// getEncryptionKeyName()
//------------------------------------------------------------------------------
std::optional<std::string> cta::ArchiveMount::getEncryptionKeyName() const {
  if(!m_dbMount)
    throw exception::Exception("In cta::RetrieveMount::getEncryptionKeyName(): got nullptr dbMount");
  return m_dbMount->mountInfo.encryptionKeyName;
}

//------------------------------------------------------------------------------
// getNbFiles
//------------------------------------------------------------------------------
uint32_t cta::ArchiveMount::getNbFiles() const {
  return m_dbMount->nbFilesCurrentlyOnTape;
}

//------------------------------------------------------------------------------
// getLabelFormat
//------------------------------------------------------------------------------
cta::common::dataStructures::Label::Format cta::ArchiveMount::getLabelFormat() const {
  return m_dbMount->mountInfo.labelFormat;
}

//------------------------------------------------------------------------------
// createDiskReporter
//------------------------------------------------------------------------------
cta::disk::DiskReporter *cta::ArchiveMount::createDiskReporter(std::string& URL) {
  return m_reporterFactory.createDiskReporter(URL);
}

//------------------------------------------------------------------------------
// getMountTransactionId
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getMountTransactionId() const {
  std::stringstream id;
  if (!m_dbMount)
    throw exception::Exception("In cta::ArchiveMount::getMountTransactionId(): got NULL dbMount");
  id << m_dbMount->mountInfo.mountId;
  return id.str();
}

//------------------------------------------------------------------------------
// updateCatalogueWithTapeFilesWritten
//------------------------------------------------------------------------------
void cta::ArchiveMount::updateCatalogueWithTapeFilesWritten(const std::set<cta::catalogue::TapeItemWrittenPointer>& tapeFilesWritten) {
  m_catalogue.TapeFile()->filesWrittenToTape(tapeFilesWritten);
}

//------------------------------------------------------------------------------
// getNextJobBatch
//------------------------------------------------------------------------------
std::list<std::unique_ptr<cta::ArchiveJob> > cta::ArchiveMount::getNextJobBatch(uint64_t filesRequested,
                                                                                uint64_t bytesRequested, log::LogContext& logContext) {
  // Check we are still running the session
  if (!m_sessionRunning)
    throw SessionNotRunning("In ArchiveMount::getNextJobBatch(): trying to get job from complete/not started session");
  // try and get a new job from the DB side
  utils::Timer t;
  std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>> dbJobBatch(m_dbMount->getNextJobBatch(filesRequested,
                                                                                                       bytesRequested, logContext));
  std::vector<std::unique_ptr<ArchiveJob>> retVector;
  retVector.reserve(dbJobBatch.size());
  // We prepare the response
  for (auto& sdaj: dbJobBatch) {
    retVector.emplace_back(std::unique_ptr<ArchiveJob>(new ArchiveJob(
            this,
            m_catalogue,
            sdaj->archiveFile,
            sdaj->srcURL,
            sdaj->tapeFile
    )));
    retVector.back()->m_dbJob.reset(sdaj.release());
  }
  log::ScopedParamContainer(logContext)
          .add("filesRequested", filesRequested)
          .add("filesFetched", dbJobBatch.size())
          .add("bytesRequested", bytesRequested)
          .add("fetchedJobs", dbJobBatch.size())
          .add("getNextJobBatchTime", t.secs())
          .log(log::INFO,
               "In SchedulerDB::ArchiveMount::getNextJobBatch(): Finished getting next job batch.");
  // Convert vector to list (which is expected as return type, to be revised in the future)
  std::list<std::unique_ptr<cta::ArchiveJob>> ret;
  ret.assign(std::make_move_iterator(retVector.begin()), std::make_move_iterator(retVector.end()));
  return ret;
}

//------------------------------------------------------------------------------
// requeueJobBatch
//------------------------------------------------------------------------------
uint64_t cta::ArchiveMount::requeueJobBatch(const std::list<std::string>& jobIDsList, cta::log::LogContext& logContext) {
  if (jobIDsList.empty()) {
    logContext.log(cta::log::INFO, "In cta::ArchiveMount::requeueJobBatch(): no job IDs provided to fail.");
    return 0;
  }
  // Forward the job IDs to the database handler's requeueJobBatch method.
  uint64_t njobs = m_dbMount->requeueJobBatch(jobIDsList, logContext);
  return njobs;
}

//------------------------------------------------------------------------------
// reportJobsBatchTransferred
//------------------------------------------------------------------------------
void cta::ArchiveMount::reportJobsBatchTransferred(std::queue<std::unique_ptr<cta::ArchiveJob>>& successfulArchiveJobs,
                                                   std::queue<cta::catalogue::TapeItemWritten>& skippedFiles,
                                                   std::queue<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>>& failedToReportArchiveJobs,
                                                   cta::log::LogContext& logContext) {
  std::set<cta::catalogue::TapeItemWrittenPointer> tapeItemsWritten;
  std::list<std::unique_ptr<cta::ArchiveJob>> validatedSuccessfulArchiveJobs;
  std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>> validatedSuccessfulDBArchiveJobs;
  std::unique_ptr<cta::ArchiveJob> job;
  std::string failedValidationJobReportURL;
  bool catalogue_updated = false;
  try {
    uint64_t files=0;
    uint64_t bytes=0;
    double catalogueTime=0;
    double schedulerDbTime=0;
    double clientReportingTime=0;
    while(!successfulArchiveJobs.empty()) {
      // Get the next job to report and make sure we will not attempt to process it twice.
      job = std::move(successfulArchiveJobs.front());
      successfulArchiveJobs.pop();
      if (!job) continue;
      cta::log::ScopedParamContainer params(logContext);
      params.add("tapeVid", job->tapeFile.vid)
            .add("mountType", cta::common::dataStructures::toString(job->m_mount->getMountType()))
            .add("fileId", job->archiveFile.archiveFileID)
            .add("type", "ReportSuccessful");
      logContext.log(cta::log::INFO, "In cta::ArchiveMount::reportJobsBatchTransferred(): archive job successful");
      try {
        tapeItemsWritten.emplace(job->validateAndGetTapeFileWritten().release());
      } catch(const cta::exception::Exception&) {
        //We put the not validated job into this list in order to insert the job
        //into the failedToReportArchiveJobs list in the exception catching block
        failedValidationJobReportURL = job->reportURL();
        validatedSuccessfulDBArchiveJobs.emplace_back(std::move(job->m_dbJob));
        throw;
      }
      files++;
      bytes += job->archiveFile.fileSize;
      validatedSuccessfulArchiveJobs.emplace_back(std::move(job));
      job.reset();
    }
    while (!skippedFiles.empty()) {
      auto tiwup = std::make_unique<cta::catalogue::TapeItemWritten>();
      *tiwup = skippedFiles.front();
      skippedFiles.pop();
      tapeItemsWritten.emplace(tiwup.release());
    }
    utils::Timer t;

    // Now get the db mount to mark the jobs as successful.
    // Extract the db jobs from the scheduler jobs.
    for (auto& schJob: validatedSuccessfulArchiveJobs) {
      validatedSuccessfulDBArchiveJobs.emplace_back(std::move(schJob->m_dbJob));
    }
    validatedSuccessfulArchiveJobs.clear();

    updateCatalogueWithTapeFilesWritten(tapeItemsWritten);
    catalogue_updated = true;
    catalogueTime=t.secs(utils::Timer::resetCounter);
    {
      cta::log::ScopedParamContainer params(logContext);
      params.add("tapeFilesWritten", tapeItemsWritten.size())
            .add("files", files)
            .add("bytes", bytes)
            .add("catalogueTime", catalogueTime);
      logContext.log(cta::log::INFO, "Catalog updated for batch of jobs");
    }

    // We can now pass the validatedSuccessfulArchiveJobs list for the dbMount to process. We are done at that point.
    // Reporting to client will be queued if needed and done in another process.
    m_dbMount->setJobBatchTransferred(validatedSuccessfulDBArchiveJobs, logContext);
    schedulerDbTime = t.secs(utils::Timer::resetCounter);
    cta::log::ScopedParamContainer params(logContext);
    params.add("files", files)
          .add("bytes", bytes)
          .add("catalogueTime", catalogueTime)
          .add("schedulerDbTime", schedulerDbTime)
          .add("totalTime", catalogueTime + schedulerDbTime + clientReportingTime);
    logContext.log(log::INFO, "In ArchiveMount::reportJobsBatchTransferred(): recorded a batch of archive jobs in metadata");
  } catch (const cta::exception::NoSuchObject& ex){
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionMessageValue", ex.getMessageValue());
    if (job) {
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path)
            .add("reportURL", failedValidationJobReportURL);
    }
    const std::string msg_error = "In ArchiveMount::reportJobsBatchTransferred(): job does not exist in the Scheduler DB";
    logContext.log(cta::log::WARNING, msg_error);
  } catch (const cta::exception::Exception& e) {
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionMessageValue", e.getMessageValue());
    if (job) {
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path)
            .add("reportURL", failedValidationJobReportURL);
    }
    const std::string msg_error = "In ArchiveMount::reportJobsBatchTransferred(): got an exception";
    logContext.log(cta::log::ERR, msg_error);
    //If validatedSuccessfulArchiveJobs has still jobs in it, it means that
    //the validation job->validateAndGetTapeFileWritten() failed for one job and
    //threw an exception. We will then have to fail all the others.
    for (auto& ctaJob: validatedSuccessfulArchiveJobs) {
      if (ctaJob)
        validatedSuccessfulDBArchiveJobs.emplace_back(std::move(ctaJob->m_dbJob));
    }
    for (auto& aj: validatedSuccessfulDBArchiveJobs) {
      if (aj)
        failedToReportArchiveJobs.push(std::move(aj));
    }
    if (catalogue_updated) {
      throw cta::ArchiveMount::FailedReportMoveToQueue(msg_error);
    }
    else {
      throw cta::ArchiveMount::FailedReportCatalogueUpdate(msg_error);
    }
  } catch(const std::exception& e){
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionWhat", e.what());
    if (job) {
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path);
    }
    const std::string msg_error = "In ArchiveMount::reportJobsBatchTransferred(): got a standard exception";
    logContext.log(cta::log::ERR, msg_error);
    for (auto& aj: validatedSuccessfulDBArchiveJobs) {
      if (aj)
        failedToReportArchiveJobs.push(std::move(aj));
    }
    if (catalogue_updated) {
      throw cta::ArchiveMount::FailedReportMoveToQueue(msg_error);
    }
    else {
      throw cta::ArchiveMount::FailedReportCatalogueUpdate(msg_error);
    }
  }
}

//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::ArchiveMount::complete() {
  // Just record we are done with the mount
  m_sessionRunning = false;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveMount::~ArchiveMount() noexcept = default;

//------------------------------------------------------------------------------
// setDriveStatus()
//------------------------------------------------------------------------------
void cta::ArchiveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status, const std::optional<std::string>& reason) {
  m_dbMount->setDriveStatus(status, getMountType(), time(nullptr), reason);
}

//------------------------------------------------------------------------------
// setTapeSessionStats()
//------------------------------------------------------------------------------
void cta::ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) {
  m_dbMount->setTapeSessionStats(stats);
}

//------------------------------------------------------------------------------
// setTapeMounted()
//------------------------------------------------------------------------------
void cta::ArchiveMount::setTapeMounted(cta::log::LogContext& logContext) const {
  utils::Timer t;
  log::ScopedParamContainer spc(logContext);
  try {
    m_catalogue.Tape()->tapeMountedForArchive(m_dbMount->getMountInfo().vid, m_dbMount->getMountInfo().drive);
    auto catalogueTime = t.secs(cta::utils::Timer::resetCounter);
    spc.add("catalogueTime", catalogueTime);
    logContext.log(log::INFO, "In ArchiveMount::setTapeMounted(): success.");
  } catch (cta::exception::Exception& ex) {
    auto catalogueTimeFailed = t.secs(cta::utils::Timer::resetCounter);
    spc.add("catalogueTime", catalogueTimeFailed);
    logContext.log(cta::log::WARNING,
                   "Failed to update catalogue for the tape mounted for archive.");
  }
}

//------------------------------------------------------------------------------
// setTapeFull()
//------------------------------------------------------------------------------
void cta::ArchiveMount::setTapeFull() {
  m_catalogue.Tape()->noSpaceLeftOnTape(m_dbMount->getMountInfo().vid);
}
