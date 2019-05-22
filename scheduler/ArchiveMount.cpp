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

#include "scheduler/ArchiveMount.hpp"
#include "common/make_unique.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveMount::ArchiveMount(catalogue::Catalogue & catalogue): m_catalogue(catalogue), m_sessionRunning(false){
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::ArchiveMount::ArchiveMount(catalogue::Catalogue & catalogue,
  std::unique_ptr<SchedulerDatabase::ArchiveMount> dbMount): m_catalogue(catalogue), 
    m_sessionRunning(false) {
  m_dbMount.reset(
    dynamic_cast<SchedulerDatabase::ArchiveMount*>(dbMount.release()));
  if(!m_dbMount.get()) {
    throw WrongMountType(std::string(__FUNCTION__) +
      ": could not cast mount to SchedulerDatabase::ArchiveMount");
  }
}

//------------------------------------------------------------------------------
// getMountType
//------------------------------------------------------------------------------
cta::common::dataStructures::MountType cta::ArchiveMount::getMountType() const {
  return cta::common::dataStructures::MountType::ArchiveForUser;
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
std::string cta::ArchiveMount::getMediaType() const{
    return m_dbMount->mountInfo.mediaType;
}

//------------------------------------------------------------------------------
// getVendor
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getVendor() const{
    return m_dbMount->mountInfo.vendor;
}

//------------------------------------------------------------------------------
// getCapacityInBytes
//------------------------------------------------------------------------------
uint64_t cta::ArchiveMount::getCapacityInBytes() const
{
    return m_dbMount->mountInfo.capacityInBytes;
}

//------------------------------------------------------------------------------
// getNbFiles
//------------------------------------------------------------------------------
uint32_t cta::ArchiveMount::getNbFiles() const {
  return m_dbMount->nbFilesCurrentlyOnTape;
}

//------------------------------------------------------------------------------
// createDiskReporter
//------------------------------------------------------------------------------
cta::disk::DiskReporter* cta::ArchiveMount::createDiskReporter(std::string& URL) {
  return m_reporterFactory.createDiskReporter(URL);
}

//------------------------------------------------------------------------------
// getMountTransactionId
//------------------------------------------------------------------------------
std::string cta::ArchiveMount::getMountTransactionId() const {
  std::stringstream id;
  if (!m_dbMount.get())
    throw exception::Exception("In cta::ArchiveMount::getMountTransactionId(): got NULL dbMount");
  id << m_dbMount->mountInfo.mountId;
  return id.str();
}

//------------------------------------------------------------------------------
// updateCatalogueWithTapeFilesWritten
//------------------------------------------------------------------------------
void cta::ArchiveMount::updateCatalogueWithTapeFilesWritten(const std::set<cta::catalogue::TapeItemWrittenPointer> &tapeFilesWritten) {
  m_catalogue.filesWrittenToTape(tapeFilesWritten);
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
  std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>> dbJobBatch(m_dbMount->getNextJobBatch(filesRequested, 
    bytesRequested, logContext));
  std::list<std::unique_ptr<ArchiveJob>> ret;
  // We prepare the response
  for (auto & sdaj: dbJobBatch) {
    ret.emplace_back(new ArchiveJob(this, m_catalogue,
      sdaj->archiveFile, sdaj->srcURL, sdaj->tapeFile));
    ret.back()->m_dbJob.reset(sdaj.release());
  }
  return ret;
}

//------------------------------------------------------------------------------
// reportJobsBatchWritten
//------------------------------------------------------------------------------
void cta::ArchiveMount::reportJobsBatchTransferred(std::queue<std::unique_ptr<cta::ArchiveJob> > & successfulArchiveJobs,
    std::queue<cta::catalogue::TapeItemWritten> & skippedFiles, cta::log::LogContext& logContext) {
  std::set<cta::catalogue::TapeItemWrittenPointer> tapeItemsWritten;
  std::list<std::unique_ptr<cta::ArchiveJob> > validatedSuccessfulArchiveJobs;
  std::unique_ptr<cta::ArchiveJob> job;
  try{
    uint64_t files=0;
    uint64_t bytes=0;
    double catalogueTime=0;
    double schedulerDbTime=0;
    double clientReportingTime=0;
    while(!successfulArchiveJobs.empty()) {
      // Get the next job to report and make sure we will not attempt to process it twice.
      job = std::move(successfulArchiveJobs.front());
      successfulArchiveJobs.pop();
      if (!job.get()) continue;
      tapeItemsWritten.emplace(job->validateAndGetTapeFileWritten().release());
      files++;
      bytes+=job->archiveFile.fileSize;
      validatedSuccessfulArchiveJobs.emplace_back(std::move(job));      
      job.reset();
    }
    while (!skippedFiles.empty()) {
      auto tiwup = cta::make_unique<cta::catalogue::TapeItemWritten>();
      *tiwup = skippedFiles.front();
      skippedFiles.pop();
      tapeItemsWritten.emplace(tiwup.release());
    }
    utils::Timer t;
    // Note: former content of ReportFlush::updateCatalogueWithTapeFilesWritten
    updateCatalogueWithTapeFilesWritten(tapeItemsWritten);
    catalogueTime=t.secs(utils::Timer::resetCounter);
    {
      cta::log::ScopedParamContainer params(logContext);
      params.add("tapeFilesWritten", tapeItemsWritten.size())
            .add("files", files)
            .add("bytes", bytes)
            .add("catalogueTime", catalogueTime);
      logContext.log(cta::log::INFO, "Catalog updated for batch of jobs");   
    }
    
    // Now get the db mount to mark the jobs as successful.
    // Extract the db jobs from the scheduler jobs.
    std::list<std::unique_ptr<cta::SchedulerDatabase::ArchiveJob>> validatedSuccessfulDBArchiveJobs;
    for (auto &schJob: validatedSuccessfulArchiveJobs) {
      validatedSuccessfulDBArchiveJobs.emplace_back(std::move(schJob->m_dbJob));
    }
    
    // We can now pass this list for the dbMount to process. We are done at that point.
    // Reporting to client will be queued if needed and done in another process.
    m_dbMount->setJobBatchTransferred(validatedSuccessfulDBArchiveJobs, logContext);
    schedulerDbTime=t.secs(utils::Timer::resetCounter);
    cta::log::ScopedParamContainer params(logContext);
    params.add("files", files)
          .add("bytes", bytes)
          .add("catalogueTime", catalogueTime)
          .add("schedulerDbTime", schedulerDbTime)
          .add("totalTime", catalogueTime  + schedulerDbTime + clientReportingTime);
    logContext.log(log::INFO, "In ArchiveMount::reportJobsBatchWritten(): recorded a batch of archive jobs in metadata.");
  } catch(cta::exception::Exception& e){
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionMessageValue", e.getMessageValue());
    if (job.get()) {
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path)
            .add("reportURL", job->reportURL());
    }
    const std::string msg_error="In ArchiveMount::reportJobsBatchWritten(): got an exception";
    logContext.log(cta::log::ERR, msg_error);
    throw cta::ArchiveMount::FailedMigrationRecallResult(msg_error);
  } catch(std::exception& e){
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionWhat", e.what());
    if (job.get()) {
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path);
    }
    const std::string msg_error="In ArchiveMount::reportJobsBatchWritten(): got an standard exception";
    logContext.log(cta::log::ERR, msg_error);
    throw cta::ArchiveMount::FailedMigrationRecallResult(msg_error);
  }
}


//------------------------------------------------------------------------------
// complete
//------------------------------------------------------------------------------
void cta::ArchiveMount::complete() {
  // Just set the session as complete in the DB.
  m_dbMount->complete(time(NULL));
  // and record we are done with the mount
  m_sessionRunning = false;
}

//------------------------------------------------------------------------------
// abort
//------------------------------------------------------------------------------
void cta::ArchiveMount::abort() {
  complete();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::ArchiveMount::~ArchiveMount() throw() {
}

//------------------------------------------------------------------------------
// setDriveStatus()
//------------------------------------------------------------------------------
void cta::ArchiveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status) {
  m_dbMount->setDriveStatus(status, time(NULL));
}

//------------------------------------------------------------------------------
// setTapeSessionStats()
//------------------------------------------------------------------------------
void cta::ArchiveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  m_dbMount->setTapeSessionStats(stats);
}

//------------------------------------------------------------------------------
// setTapeFull()
//------------------------------------------------------------------------------
void cta::ArchiveMount::setTapeFull() {
  m_catalogue.noSpaceLeftOnTape(m_dbMount->getMountInfo().vid);
}
