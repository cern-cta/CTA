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

#include "common/archiveNS/TapeFileLocation.hpp"
#include "common/archiveNS/ArchiveFile.hpp"
#include "common/exception/Exception.hpp"
#include "common/remoteFS/RemotePathAndStatus.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "nameserver/NameServer.hpp"

#include <stdint.h>
#include <string>

namespace cta {

// Forward declaration
class ArchiveMount;

/**
 * Class representing the transfer of a single copy of a remote file to tape.
 */
class ArchiveJob {

  /**
   * The ArchiveMount class is a friend so that it can call the private
   * constructor of ArchiveJob.
   */
  friend class ArchiveMount;

protected:
  /**
   * Constructor.
   * 
   * @param mount the mount that generated this job
   * @param archiveFile informations about the file that we are storing
   * @param remotePathAndStatus location and properties of the remote file
   * @param tapeFileLocation the location within the tape
   */
  ArchiveJob(
  ArchiveMount &mount,
  NameServer & ns,
  const ArchiveFile &archiveFile,
  const RemotePathAndStatus &remotePathAndStatus,
  const NameServerTapeFile &nameServerTapeFile);

public:

  /**
   * Destructor.
   */
  virtual ~ArchiveJob() throw();
  
  CTA_GENERATE_EXCEPTION_CLASS(BlockIdNotSet);
  CTA_GENERATE_EXCEPTION_CLASS(ChecksumNotSet);
  /**
   * Indicates that the job was successful and updates the backend store
   *
   */
  virtual void complete();
  
  /**
   * Triggers a scheduler update following the failure of the job.
   * The reason for the failure should have been set beforehand by calling
   * setFailureReason(), but failure to do it is non-fatal (a standard error
   * reason will be used)
   * This 2 step approach allows the reason to be recorded fast in the 
   * tape writing thread, and the slow(er) update of the DB to be executed
   * in a second thread.
   *
   */
  virtual void failed(const cta::exception::Exception &ex);

  /**
   * Indicates that the job should be tried again (typically reaching the end 
   * of the tape).
   */
  virtual void retry();
  
private:
  std::unique_ptr<cta::SchedulerDatabase::ArchiveJob> m_dbJob;
  
  /**
   * The mount that generated this job
   */
  ArchiveMount &m_mount;
  
  /**
   * Reference to the name server
   */
  NameServer &m_ns;
public:
  
  CTA_GENERATE_EXCEPTION_CLASS(NotImplemented);
  
  /**
   * The NS archive file information
   */
  ArchiveFile archiveFile;
  
  /**
   * The remote file information
   */
  RemotePathAndStatus remotePathAndStatus; 
  
  /**
   * The file archive result for the NS
   */
  NameServerTapeFile nameServerTapeFile;

}; // class ArchiveJob

} // namespace cta
