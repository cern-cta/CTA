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
  return cta::common::dataStructures::MountType::Archive;
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
// getNbFiles
//------------------------------------------------------------------------------
uint32_t cta::ArchiveMount::getNbFiles() const {
  return m_dbMount->nbFilesCurrentlyOnTape;
}

//------------------------------------------------------------------------------
// createDiskReporter
//------------------------------------------------------------------------------
cta::eos::DiskReporter* cta::ArchiveMount::createDiskReporter(std::string& URL, std::promise<void> &reporterState) {
  return m_reporterFactory.createDiskReporter(URL, reporterState);
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
void cta::ArchiveMount::updateCatalogueWithTapeFilesWritten(const std::set<cta::catalogue::TapeFileWritten> &tapeFilesWritten) {
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
    ret.emplace_back(new ArchiveJob(*this, m_catalogue,
      sdaj->archiveFile, sdaj->srcURL, sdaj->tapeFile));
    ret.back()->m_dbJob.reset(sdaj.release());
  }
  return ret;
}

//------------------------------------------------------------------------------
// reportJobsBatchWritten
//------------------------------------------------------------------------------
void cta::ArchiveMount::reportJobsBatchWritten(std::queue<std::unique_ptr<cta::ArchiveJob> > successfulArchiveJobs,
  cta::log::LogContext& logContext) {
  std::set<cta::catalogue::TapeFileWritten> tapeFilesWritten;
  std::list<std::unique_ptr<cta::ArchiveJob> > validatedSuccessfulArchiveJobs;
  std::unique_ptr<cta::ArchiveJob> job;
  try{
    while(!successfulArchiveJobs.empty()) {
      // Get the next job to report and make sure we will not attempt to process it twice.
      job = std::move(successfulArchiveJobs.front());
      successfulArchiveJobs.pop();
      if (!job.get()) continue;        
      tapeFilesWritten.insert(job->validateAndGetTapeFileWritten());
      validatedSuccessfulArchiveJobs.emplace_back(std::move(job));      
      job.reset(nullptr);
    }
    
    // Note: former content of ReportFlush::updateCatalogueWithTapeFilesWritten
    updateCatalogueWithTapeFilesWritten(tapeFilesWritten);
    {
      cta::log::ScopedParamContainer params(logContext);
      params.add("tapeFilesWritten", tapeFilesWritten.size());
      logContext.log(cta::log::INFO,"Catalog updated for batch of jobs");   
    }
    for (auto &job: validatedSuccessfulArchiveJobs) {
      job->asyncSetJobSucceed();
    }
    
    // Note:  former content of ReportFlush::checkAndAsyncReportCompletedJobs
    std::list<std::unique_ptr <cta::ArchiveJob> > reportedArchiveJobs;

    for (auto &job: validatedSuccessfulArchiveJobs){
      cta::log::ScopedParamContainer params(logContext);
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path);
      logContext.log(cta::log::DEBUG,
        "In MigrationReportPacker::ReportFlush::checkAndAsyncReportCompletedJobs()"
        " check for async backend update finished");
      if(job->checkAndAsyncReportComplete()) { 
        params.add("reportURL", job->reportURL());
        reportedArchiveJobs.emplace_back(std::move(job));
        logContext.log(cta::log::INFO,"Sent to the client a full file archival");
      } else {
        logContext.log(cta::log::INFO, "Recorded the partial migration of a file");
      }
    }

    for (const auto &job: reportedArchiveJobs){
      try {
        job->waitForReporting();
        cta::log::ScopedParamContainer params(logContext);
        params.add("fileId", job->archiveFile.archiveFileID)
              .add("diskInstance", job->archiveFile.diskInstance)
              .add("diskFileId", job->archiveFile.diskFileId)
              .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path)
              .add("reportURL", job->reportURL())
              .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path)
              .add("reportTime", job->reportTime());
        logContext.log(cta::log::INFO,"Reported to the client a full file archival");
      } catch(cta::exception::Exception &ex) {
        cta::log::ScopedParamContainer params(logContext);
          params.add("fileId", job->archiveFile.archiveFileID)
                .add("diskInstance", job->archiveFile.diskInstance)
                .add("diskFileId", job->archiveFile.diskFileId)
                .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path).add("reportURL", job->reportURL())
                .add("errorMessage", ex.getMessage().str());
          logContext.log(cta::log::ERR,"Unsuccessful report to the client a full file archival:");
      }
    }
           
    logContext.log(cta::log::INFO,"Reported to the client that a batch of files was written on tape");    
  } catch(const cta::exception::Exception& e){
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionMessageValue", e.getMessageValue());
    if (job.get()) {
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path)
            .add("reportURL", job->reportURL());
    }
    const std::string msg_error="An exception was caught trying to call reportMigrationResults";
    logContext.log(cta::log::ERR, msg_error);
    throw cta::ArchiveMount::FailedMigrationRecallResult(msg_error);
  } catch(const std::exception& e){
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionWhat", e.what());
    if (job.get()) {
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path);
    }
    const std::string msg_error="An std::exception was caught trying to call reportMigrationResults";
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

