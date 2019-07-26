/*
 * The CERN Tape Retrieve (CTA) project
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

#include "scheduler/RetrieveMount.hpp"
#include "common/Timer.hpp"
#include "common/log/TimingList.hpp"
#include "disk/DiskSystem.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveMount::RetrieveMount():
  m_sessionRunning(false) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrieveMount::RetrieveMount(
  std::unique_ptr<SchedulerDatabase::RetrieveMount> dbMount): 
  m_sessionRunning(false) {
  m_dbMount.reset(dbMount.release());
}

//------------------------------------------------------------------------------
// getMountType()
//------------------------------------------------------------------------------
cta::common::dataStructures::MountType cta::RetrieveMount::getMountType() const{
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
std::string cta::RetrieveMount::getVid() const{
  return m_dbMount->mountInfo.vid;
}

//------------------------------------------------------------------------------
// getActivity()
//------------------------------------------------------------------------------
cta::optional<std::string> cta::RetrieveMount::getActivity() const {
  return m_dbMount->mountInfo.activity;
}

//------------------------------------------------------------------------------
// getMountTransactionId()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getMountTransactionId() const{
  std::stringstream id;
  if (!m_dbMount.get())
    throw exception::Exception("In cta::RetrieveMount::getMountTransactionId(): got NULL dbMount");
  id << m_dbMount->mountInfo.mountId;
  return id.str();
}

//------------------------------------------------------------------------------
// getMountTapePool()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getPoolName() const{
  std::stringstream sTapePool;
  if (!m_dbMount.get())
    throw exception::Exception("In cta::RetrieveMount::getPoolName(): got NULL dbMount");
  sTapePool << m_dbMount->mountInfo.tapePool;
  return sTapePool.str();
}

//------------------------------------------------------------------------------
// getVo()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getVo() const
{
    std::stringstream sVo;
    if(!m_dbMount.get())
        throw exception::Exception("In cta::RetrieveMount::getVo(): got NULL dbMount");
    sVo<<m_dbMount->mountInfo.vo;
    return sVo.str();
}

//------------------------------------------------------------------------------
// getMediaType()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getMediaType() const
{
    std::stringstream sMediaType;
    if(!m_dbMount.get())
        throw exception::Exception("In cta::RetrieveMount::getMediaType(): got NULL dbMount");
    sMediaType<<m_dbMount->mountInfo.mediaType;
    return sMediaType.str();
}

//------------------------------------------------------------------------------
// getVendor()
//------------------------------------------------------------------------------
std::string cta::RetrieveMount::getVendor() const
{
    std::stringstream sVendor;
    if(!m_dbMount.get())
        throw exception::Exception("In cta::RetrieveMount::getVendor(): got NULL dbMount");
    sVendor<<m_dbMount->mountInfo.vendor;
    return sVendor.str();
}

//------------------------------------------------------------------------------
// getCapacityInBytes()
//------------------------------------------------------------------------------
uint64_t cta::RetrieveMount::getCapacityInBytes() const {
    if(!m_dbMount.get())
        throw exception::Exception("In cta::RetrieveMount::getVendor(): got NULL dbMount");
    return m_dbMount->mountInfo.capacityInBytes;
}

//------------------------------------------------------------------------------
// getNextJobBatch()
//------------------------------------------------------------------------------
std::list<std::unique_ptr<cta::RetrieveJob> > cta::RetrieveMount::getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested,
    log::LogContext& logContext) {
  if (!m_sessionRunning)
    throw SessionNotRunning("In RetrieveMount::getNextJobBatch(): trying to get job from complete/not started session");
  // Get the current file systems list from the catalogue
  disk::DiskSystemList diskSystemList;
  if (m_catalogue) diskSystemList = m_catalogue->getAllDiskSystems();
  // TODO: the diskSystemFreeSpaceList could be made a member of the retrieve mount and cache the fetched values, limiting the re-querying
  // of the disk systems free space.
  disk::DiskSystemFreeSpaceList diskSystemFreeSpaceList (diskSystemList);
  // Try and get a new job from the DB. The DB mount (in memory object) is taking care of reserving the free space for the popped 
  // elements and query the disk systems, via the diskSystemFreeSpaceList object.
  auto dbJobBatch = m_dbMount->getNextJobBatch(filesRequested, bytesRequested, diskSystemFreeSpaceList, logContext);
//  std::list<std::unique_ptr<cta::SchedulerDatabase::RetrieveJob>> dbJobBatch;
//  { 
//    retryBatchAllocation:
//    dbJobBatch = m_dbMount->getNextJobBatch(filesRequested, bytesRequested, diskSystemFreeSpaceList, logContext);
//    // Compute the necessary space in each targeted disk system.
//    SchedulerDatabase::DiskSpaceReservationRequest diskSpaceReservationRequest;
//    std::map<std::string, uint64_t> spaceMap;
//    for (auto &j: dbJobBatch)
//      if (j->diskSystemName)
//        diskSpaceReservationRequest.addRequest(j->diskSystemName.value(), j->archiveFile.fileSize);
//    // Reserve the space.
//    // We will update this information on-demand during iterations if needed. 
//    disk::DiskSystemFreeSpaceList diskSystemFreeSpaceList(diskSystemList);
//    retrySpaceAllocation:
//    try {
//      m_dbMount->reserveDiskSpace(diskSpaceReservationRequest);
//    } catch (SchedulerDatabase::OutdatedDiskSystemInformation &odsi) {
//      // Update information for missing/outdated disk systems.
//      diskSystemFreeSpaceList.fetchFileSystemFreeSpace(odsi.getDiskSsytems());
//      goto retrySpaceAllocation;
//    } catch (SchedulerDatabase::FullDiskSystem &fds) {
//      // Mark the disk systems as full for the mount. Re-queue all requests, repeat the pop attempt.
//      for (auto &ds: fds.getDiskSsytems()) m_fullDiskSystems.insert(ds);
//      m_dbMount->requeueJobBatch(dbJobBatch, logContext);
//      dbJobBatch.clear();
//      goto retryBatchAllocation;
//    }
//  }
  std::list<std::unique_ptr<RetrieveJob>> ret;
  // We prepare the response
  for (auto & sdrj: dbJobBatch) {
    ret.emplace_back(new RetrieveJob(this,
      sdrj->retrieveRequest, sdrj->archiveFile, sdrj->selectedCopyNb,
      PositioningMethod::ByBlock));
    ret.back()->m_dbJob.reset(sdrj.release());
  }
  return ret;
}

//------------------------------------------------------------------------------
// waitAndFinishSettingJobsBatchRetrieved()
//------------------------------------------------------------------------------
void cta::RetrieveMount::flushAsyncSuccessReports(std::queue<std::unique_ptr<cta::RetrieveJob> >& successfulRetrieveJobs, cta::log::LogContext& logContext) {
  std::list<std::unique_ptr<cta::RetrieveJob> > validatedSuccessfulRetrieveJobs; //List to ensure the destruction of the retrieve jobs at the end of this method
  std::list<cta::SchedulerDatabase::RetrieveJob *> validatedSuccessfulDBRetrieveJobs;
  std::unique_ptr<cta::RetrieveJob> job;
  double waitUpdateCompletionTime=0;
  double jobBatchFinishingTime=0;
  double schedulerDbTime=0;
  uint64_t files=0;
  uint64_t bytes=0;
  utils::Timer t;
  log::TimingList tl;
  try {
    while (!successfulRetrieveJobs.empty()) {
      job = std::move(successfulRetrieveJobs.front());
      successfulRetrieveJobs.pop();
      if (!job.get()) continue;
      files++;
      bytes+=job->archiveFile.fileSize;
      validatedSuccessfulDBRetrieveJobs.emplace_back(job->m_dbJob.get());
      validatedSuccessfulRetrieveJobs.emplace_back(std::move(job));
      job.reset();
    }
    waitUpdateCompletionTime=t.secs(utils::Timer::resetCounter);
    tl.insertAndReset("waitUpdateCompletionTime",t);
    // Complete the cleaning up of the jobs in the mount
    m_dbMount->flushAsyncSuccessReports(validatedSuccessfulDBRetrieveJobs, logContext);
    jobBatchFinishingTime=t.secs();
    tl.insertOrIncrement("jobBatchFinishingTime",jobBatchFinishingTime);
    schedulerDbTime=jobBatchFinishingTime + waitUpdateCompletionTime;
    tl.insertOrIncrement("schedulerDbTime",schedulerDbTime);
    {
      cta::log::ScopedParamContainer params(logContext);
      params.add("successfulRetrieveJobs", successfulRetrieveJobs.size())
            .add("files", files)
            .add("bytes", bytes);
      tl.addToLog(params);
      //TODO : if repack, add log to say that the jobs were marked as RJS_Succeeded
      logContext.log(cta::log::DEBUG,"In RetrieveMout::waitAndFinishSettingJobsBatchRetrieved(): deleted complete retrieve jobs.");
    }
  } catch(const cta::exception::Exception& e){
    cta::log::ScopedParamContainer params(logContext);
    params.add("exceptionMessageValue", e.getMessageValue());
    if (job.get()) {
      params.add("fileId", job->archiveFile.archiveFileID)
            .add("diskInstance", job->archiveFile.diskInstance)
            .add("diskFileId", job->archiveFile.diskFileId)
            .add("lastKnownDiskPath", job->archiveFile.diskFileInfo.path);
    }
    const std::string msg_error="In RetrieveMount::waitAndFinishSettingJobsBatchRetrieved(): got an exception";
    logContext.log(cta::log::ERR, msg_error);
    logContext.logBacktrace(cta::log::ERR, e.backtrace());
    // Failing here does not really affect the session so we can carry on. Reported jobs are reported, non-reported ones
    // will be retried.
  } catch(const std::exception& e){
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
    // Failing here does not really affect the session so we can carry on. Reported jobs are reported, non-reported ones
    // will be retried.
  }
}

//------------------------------------------------------------------------------
// createDiskReporter()
//------------------------------------------------------------------------------
cta::disk::DiskReporter* cta::RetrieveMount::createDiskReporter(std::string& URL) {
  return m_reporterFactory.createDiskReporter(URL);
}

//------------------------------------------------------------------------------
// setCatalogue()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setCatalogue(catalogue::Catalogue* catalogue) {
  if (m_catalogue)
    throw exception::Exception("In RetrieveMount::setCatalogue(): catalogue already set.");
  if (!catalogue)
    throw exception::Exception("In RetrieveMount::setCatalogue(): trying to set a null catalogue.");
  m_catalogue = catalogue;
}

//------------------------------------------------------------------------------
// tapeComplete()
//------------------------------------------------------------------------------
void cta::RetrieveMount::tapeComplete() {
  m_tapeRunning = false;
  if (!m_diskRunning) {
    // Just set the session as complete in the DB.
    m_dbMount->complete(time(NULL));
    // and record we are done with the mount
    m_sessionRunning = false;
  } else {
    // This is a special case: we have to report the tape server is draining
    // its memory to disk
    setDriveStatus(cta::common::dataStructures::DriveStatus::DrainingToDisk);
  }
}

//------------------------------------------------------------------------------
// diskComplete()
//------------------------------------------------------------------------------
void cta::RetrieveMount::diskComplete() {
  m_diskRunning = false;
  if (!m_tapeRunning) {
    // Just set the session as complete in the DB.
    cta::SchedulerDatabase::RetrieveMount  * ptr = m_dbMount.get();
    ptr=ptr;
    m_dbMount->complete(time(NULL));
    // and record we are done with the mount
    m_sessionRunning = false;
  }
}

//------------------------------------------------------------------------------
// abort()
//------------------------------------------------------------------------------
void cta::RetrieveMount::abort() {
  diskComplete();
  tapeComplete();
}

//------------------------------------------------------------------------------
// setDriveStatus()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setDriveStatus(cta::common::dataStructures::DriveStatus status) {
  m_dbMount->setDriveStatus(status, time(NULL));
}

//------------------------------------------------------------------------------
// setTapeSessionStats()
//------------------------------------------------------------------------------
void cta::RetrieveMount::setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) {
  m_dbMount->setTapeSessionStats(stats);
}

//------------------------------------------------------------------------------
// bothSidesComplete()
//------------------------------------------------------------------------------
bool cta::RetrieveMount::bothSidesComplete() {
  return !(m_diskRunning || m_tapeRunning);
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrieveMount::~RetrieveMount() throw() {
}
