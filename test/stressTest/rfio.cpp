/******************************************************************************
 *                      rfio.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * g++ -g -pthread -Wall -Werror -D_LARGEFILE64_SOURCE -fPIC -I $CASTOR_ROOT -I $CASTOR_ROOT/h /usr/lib64/libshift.so.2.1 -o rfio rfio.cpp
 *
 * Not the best code in the world but it works!
 *  - Still need to add race condition test, same file random operations
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <iostream>
#include <pthread.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <vector>

#include "castor/client/BaseClient.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/rh/FileQryResponse.hpp"
#include "castor/rh/FileResponse.hpp"
#include "castor/rh/Response.hpp"
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/StagePrepareToPutRequest.hpp"
#include "castor/stager/StagePutDoneRequest.hpp"
#include "castor/stager/SubRequest.hpp"

#include "Cns_api.h"
#include "Cthread_api.h"
#include "Cuuid.h"
#include "rfio_api.h"
#include "serrno.h"
#include "stager_client_commandline.h"
#include "stager_errmsg.h"
#include "u64subr.h"

// Definitions (Defaults)
#define DEFAULT_NUM_THREADS       1
#define DEFAULT_NUM_ITERATIONS    10

#define DEFAULT_RFCP_BUFFERSIZE   128 * 1024
#define DEFAULT_FILESIZE          3 * 1024

#define DEFAULT_STAGER_HOST       "lxcastordevXX"
#define DEFAULT_STAGER_SVCCLASS   "default"

// Definitions (Options)
#define OPTION_SIZE               1
#define OPTION_WRITERS            2
#define OPTION_STAGER             3
#define OPTION_SVCCLASS           4
#define OPTION_BUFFERSIZE         5
#define OPTION_DELAY              6
#define OPTION_NBFILES            7
#define OPTION_PUTDONECYCLE       8
#define OPTION_FILEQUERY          9
#define OPTION_NBREADS            10
#define OPTION_FILESTAT           11
#define OPTION_DAEMONIZE          12

// Definitions (Maximum)
#define MAX_NBTHREADS             50
#define MAX_RFIO_BUFFERSIZE       512 * 1024
#define MAX_NBREADS               10

// Definitions (Minimum)
#define MIN_FILESIZE              2048

// Global Variables
static std::string baseDirectory  = "";
static std::string stagerHost     = DEFAULT_STAGER_HOST;
static std::string stagerSvcClass = DEFAULT_STAGER_SVCCLASS;
static uint64_t    rfioBufferSize = DEFAULT_RFCP_BUFFERSIZE;
static uint64_t    iterations     = DEFAULT_NUM_ITERATIONS;
static uint64_t    writeFileSize  = DEFAULT_FILESIZE;
static int64_t     writeDelay     = 0;
static int64_t     nbReads        = 0;
static bool        putDoneCycle   = false;
static bool        fileQuery      = false;
static bool        fileStat       = false;
static bool        daemonize      = false;

// Macros (Logging and Timing)
#define IDENT "[" << stagerHost << "/" << stagerSvcClass << " "  << tid << "] "
#define ELAPSED_TIME " (elapsed: " << elapsed * 0.000001 << ")"

#define PROCESSING_START                        \
  {                                             \
    gettimeofday(&tv, NULL);                    \
  }                                               
#define PROCESSING_END                                  \
  {                                                     \
    gettimeofday(&end, NULL);                           \
    elapsed = (((end.tv_sec * 1000000) + end.tv_usec) - \
               (tv.tv_sec * 1000000) + tv.tv_usec);     \
  }

//-----------------------------------------------------------------------------
// CreateParentDirectories
//-----------------------------------------------------------------------------
int createParentDirectories(const std::string &dirpath,
                            const int tid) {

  // Variables
  std::string::size_type index =
    dirpath.rfind("/", baseDirectory.length() + 1);

  // Loop over the path components of the directory ignoring those that belong
  // to the BASE_DIRECTORY. I.e the BASE_DIRECTORY must already exist!
  while ((index = dirpath.find("/", index + 1)) !=
         std::string::npos) {
    
    // Attempt to create the directory. If it already exists ignore the error
    // and try the next directory.
    int rc = Cns_mkdir(dirpath.substr(0, index).c_str(), 0755);
    if ((rc != 0) && (serrno != EEXIST)) {
      std::cerr << IDENT << "[ERROR] "
                << "Failed to createParentDirectories()"
                << " - Path: "  << dirpath
                << " - Errno: " << sstrerror(serrno)
                << std::endl;
      return -1;
    }
  }

  return 0;
}

//-----------------------------------------------------------------------------
// UnlinkFile
//-----------------------------------------------------------------------------
int unlinkFile(const std::string &filepath,
               const int tid) {

  if (Cns_unlink(filepath.c_str()) != 0) {
    if (serrno != ENOENT) {
      std::cerr << IDENT << "[ERROR] "
                << "Failed to unlinkFile()"
                << " - Path: "  << filepath
                << " - Errno: " << sstrerror(serrno)
                << std::endl;
      return -1;
    }
  }
  return 0;
}

//-----------------------------------------------------------------------------
// RandAlnum (Helper)
// Credit: http://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
//-----------------------------------------------------------------------------
char randAlnum() {
  char c;
  while (!std::isalnum(c = static_cast<char>(std::rand())));
  return c;
}

//-----------------------------------------------------------------------------
// ReadFileUsingRFIO
//-----------------------------------------------------------------------------
int readFileUsingRFIO(const std::string &filepath,
                      const uint64_t expectedFileSize) {

  // Setup the RFIO transfer options
  int v = RFIO_STREAM;
  rfiosetopt(RFIO_READOPT, &v, 4);

  // Open the file for read
  rfio_errno = serrno = 0;
  int fd = rfio_open64((char *)filepath.c_str(), O_RDONLY, 0644);
  if (fd < 0) {
    return -1;
  }

  // Create the temporary buffer for storing data before rfio_write
  std::string buffer;
  std::string::size_type bufsize = (size_t)rfioBufferSize;

  // If the expectedFileSize is less than the buffer reset the size and then
  // reserve the amount of storage required.
  if (bufsize > expectedFileSize) {
    bufsize = expectedFileSize;
  }
  buffer.reserve(bufsize);

  // Read the data into memory
  uint64_t bytesrecv = 0;
  do {
    rfio_errno = serrno = 0;
    uint64_t nbbytes = rfio_read(fd, (void *)(buffer.c_str()), 
                                 (bufsize < (expectedFileSize - bytesrecv)) ?
                                 bufsize : (expectedFileSize - bytesrecv));
    if (nbbytes < 0) {
      rfio_errno = serrno = 0;
      rfio_close(fd);
      return -1;
    }
    bytesrecv += nbbytes;
  } while ((expectedFileSize - bytesrecv) != 0);
  
  // Close the file. Note: we don't ignore errors here so that we can detect
  // checksum problems.
  rfio_errno = serrno = 0;
  if (rfio_close(fd) != 0) {
    return -1;
  }

  return 0;
}

//-----------------------------------------------------------------------------
// WriteFileUsingRFIO
//-----------------------------------------------------------------------------
int writeRFIOBuffer(int fd,
                    const std::string buffer,
                    const std::string::size_type bufsize) {
  
  // Variables
  unsigned int offset = 0;
  uint64_t nbbytes    = 0;

  // Write the data to the file descriptor
  rfio_errno = serrno = 0;
  while ((offset != bufsize) &&
         (nbbytes = rfio_write(fd, (void *)(buffer.c_str() + offset),
                               bufsize - offset)) > 0) {
    offset += nbbytes;
  }
  if (nbbytes < 0) {
    return -1; // We encountered an error
  }

  return offset;
}

//-----------------------------------------------------------------------------
// WriteFileUsingRFIO
//-----------------------------------------------------------------------------
int writeFileUsingRFIO(const std::string &filepath,
                       const uint64_t filesize) {

  // Setup the RFIO transfer options
  int v = RFIO_STREAM;
  rfiosetopt(RFIO_READOPT, &v, 4);

  // Open the file for write
  rfio_errno = serrno = 0;
  int fd = rfio_open64((char *)filepath.c_str(),
                       O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd < 0) {
    return -1;
  }

  // Create the temporary buffer for storing data before rfio_write
  std::string buffer;
  std::string::size_type bufsize = (size_t)rfioBufferSize;

  // If the filesize is less than the buffer reset the size and then reserve
  // the amount of storage required.
  if (bufsize > filesize) {
    bufsize = filesize;
  }
  buffer.reserve(bufsize);

  // The first buffer to be written to the file contains the filepath used at
  // creation time. This allows us in the future to identify the file by its
  // contents independently of the namespace. Note: It would probably be nice
  // to include the fileid but this would require an additional nameserver
  // operation. The idea behind the program is to be as fast as possible.
  buffer += filepath;
  buffer += " ";

  // The rest of the buffer should contain random data
  generate_n(std::back_inserter(buffer), bufsize - buffer.length(), randAlnum);

  // Write the first buffer
  uint64_t bytessent = 0;
  bytessent = writeRFIOBuffer(fd, buffer, bufsize);
  if (bytessent < 0) {
    rfio_errno = serrno = 0;
    rfio_close(fd);
    return -1;
  }

  // If the amount of data sent in the first buffer is enough to satisfy the
  // total file size, close the file and return to the callee.
  if (bytessent == filesize) {
    rfio_errno = serrno = 0;
    rfio_close(fd);
    return 0;
  }

  // Generate the second random data buffer. Note: It would have been nice here
  // to generate random buffers every time but the CPU cost is too expensive
  // and results in poor bandwidth.
  buffer.clear();
  if (bufsize > (filesize - bytessent)) {
    bufsize = (filesize - bytessent);
  }
  generate_n(std::back_inserter(buffer), bufsize, randAlnum);
  
  // Write the second buffer to the file until we reach the desired filesize or
  // an error is encountered.
  do {
    uint64_t nbbytes = 
      writeRFIOBuffer(fd, buffer,
                      (bufsize < (filesize - bytessent)) ? bufsize :
                      (filesize - bytessent));
    if (nbbytes < 0) {
      rfio_close(fd);
      return -1;
    }
    bytessent += nbbytes;
  } while ((filesize - bytessent) != 0);

  // Close the file
  rfio_errno = serrno = 0;
  rfio_close(fd);

  return 0;
}

//-----------------------------------------------------------------------------
// PrepareStagerRequest
//-----------------------------------------------------------------------------
void PrepareStagerRequest(castor::stager::SubRequest **subRequest,
                          castor::client::BaseClient &client,
                          const std::string &filepath) {

  // Variables
  stage_options opts;
  
  // Prepare the SubRequest object
  if (subRequest != NULL) {
    (*subRequest)->setFileName(filepath);
    (*subRequest)->setModeBits(0644);
    (*subRequest)->setProtocol("rfio3");
  }

  // Prepare the stager options
  opts.stage_host    = (char *)stagerHost.c_str();
  opts.service_class = (char *)stagerSvcClass.c_str();
  opts.stage_version = 2;
  opts.stage_port    = 0;
  
  // Set the stager options
  client.setOptions(&opts);
}

//-----------------------------------------------------------------------------
// SendStagerRequest
//-----------------------------------------------------------------------------
int ProcessStagerResponse(castor::stager::SubRequest *subRequest,
                          std::vector<castor::rh::Response *> &responses,
                          std::string &requestStatus) { 
  // Free resources
  delete subRequest;

  // Process the responses. Note: We only sent one file therefore there should
  // only be one response!
  if (responses.size() != 1) {
    serrno = SEINTERNAL;
    return -1;
  }

  // Cast the response into a FileResponse
  castor::rh::FileResponse *fr =
    dynamic_cast<castor::rh::FileResponse *>(responses[0]);
  if (fr == NULL) {
    serrno = SEINTERNAL;
    return -1;
  }
  
  // Set the request status
  requestStatus = stage_statusName(fr->status());
 
  // Set serrno equal to the error code of the response if non zero
  serrno = 0;
  if (fr->errorCode() != 0) {
    serrno = fr->errorCode();
    delete responses[0];  // Free resources
    return -1;
  }
  delete responses[0];  // Free resources

  return 0;
}

//-----------------------------------------------------------------------------
// SendPrepareToPutRequest
//-----------------------------------------------------------------------------
int SendPrepareToPutRequest(const std::string &filepath,
                            std::string &requestId,
                            std::string &requestStatus) {

  // Variables
  castor::stager::StagePrepareToPutRequest request;
  castor::stager::SubRequest *subRequest = new castor::stager::SubRequest();
  castor::client::BaseClient client(stage_getClientTimeout());
  std::vector<castor::rh::Response *> responses;
  castor::client::VectorResponseHandler rh(&responses);

  // Prepare the stager request
  PrepareStagerRequest(&subRequest, client, filepath);

  // Complete the SubRequest object
  subRequest->setRequest(&request);

  // Prepare the StagePrepareToPutRequest object
  mode_t mask;
  umask(mask = umask(0));

  request.setSvcClassName(stagerSvcClass);
  request.setEuid(geteuid());
  request.setEgid(getegid());
  request.setMask(mask);
  
  // Add the SubRequest to the StagePrepareToPutRequest request
  request.addSubRequests(subRequest);

  // Send the request
  try {
    requestId = client.sendRequest(&request, &rh);
  } catch (castor::exception::Exception& e) {
    serrno = e.code();
    delete subRequest;  // Free resources
    return -1;
  }

  // Processing the stager response
  return (ProcessStagerResponse(subRequest, responses, requestStatus));
}

//-----------------------------------------------------------------------------
// SendPutDoneRequest
//-----------------------------------------------------------------------------
int SendPutDoneRequest(const std::string &filepath,
                       std::string &requestId,
                       std::string &requestStatus) {

  // Variables
  castor::stager::StagePutDoneRequest request;
  castor::stager::SubRequest *subRequest = new castor::stager::SubRequest();
  castor::client::BaseClient client(stage_getClientTimeout());
  std::vector<castor::rh::Response *> responses;
  castor::client::VectorResponseHandler rh(&responses);

  // Prepare the stager request
  PrepareStagerRequest(&subRequest, client, filepath);

  // Complete the SubRequest object
  subRequest->setRequest(&request);

  // Complete the StagePutDoneRequest object
  request.setSvcClassName(stagerSvcClass);
  request.setEuid(geteuid());
  request.setEgid(getegid());
  
  // Add the SubRequest to the StagePutDoneRequest request
  request.addSubRequests(subRequest);

  // Send the request
  try {
    requestId = client.sendRequest(&request, &rh);
  } catch (castor::exception::Exception& e) {
    serrno = e.code();
    delete subRequest;  // Free resources
    return -1;
  }  

  // Processing the stager response
  return (ProcessStagerResponse(subRequest, responses, requestStatus));
}

//-----------------------------------------------------------------------------
// SendFileQueryRequest
//-----------------------------------------------------------------------------
int SendFileQueryRequest(const std::string &filepath,
                         std::string &requestId,
                         std::string &requestStatus) {
  
  // Variables
  castor::stager::StageFileQueryRequest request;
  castor::stager::QueryParameter *par = new castor::stager::QueryParameter();
  castor::client::BaseClient client(stage_getClientTimeout());
  std::vector<castor::rh::Response *> responses;
  castor::client::VectorResponseHandler rh(&responses);

  // Prepare the Query parameter
  par->setQueryType((castor::stager::RequestQueryType)BY_FILENAME);
  par->setValue(filepath);
  par->setQuery(&request);

  // Prepare the stager request
  PrepareStagerRequest(NULL, client, filepath);

  // Complete the StageFileQuery object
  request.setSvcClassName(stagerSvcClass);
  request.setEuid(geteuid());
  request.setEgid(getegid());

  // Add the QueryParameter to the StageFileQueryRequest object
  request.addParameters(par);
 
  // Send the request
  try {
    requestId = client.sendRequest(&request, &rh);
  } catch (castor::exception::Exception& e) {
    serrno = e.code();
    delete par;  // Free resources
    return -1;
  }  
  delete par;  // Free resources;

  // Process the responses. Note: We only sent one file therefore there should
  // only be one response!
  if (responses.size() != 1) {
    serrno = SEINTERNAL;
    return -1;
  }

  // Cast the response into a FileResponse
  castor::rh::FileQryResponse *fr =
    dynamic_cast<castor::rh::FileQryResponse *>(responses[0]);
  if (fr == NULL) {
    serrno = SEINTERNAL;
    return -1;
  }
  
  // Set the request status
  requestStatus = stage_fileStatusName(fr->status());
 
  // Set serrno equal to the error code of the response if non zero
  serrno = 0;
  if (fr->errorCode() != 0) {
    serrno = fr->errorCode();
    delete responses[0];  // Free resources
    return -1;
  }
  delete responses[0];  // Free resources

  return 0;
}

//-----------------------------------------------------------------------------
// Worker
//-----------------------------------------------------------------------------
void worker(void) {

  // Variables
  unsigned int i, j;
  unsigned int emlinkoffset = 0;
  int      tid = (int)syscall(__NR_gettid);
  char     buffer[1024];
  char     errbuf[ERRBUFSIZE + 1];
  char     hostname[1024];

  int      rc;
  int64_t  elapsed;

  std::string requestId;
  std::string requestStatus;

  // Set the error buffers for stager related errors
  stager_seterrbuf(errbuf, ERRBUFSIZE + 1);
  
  // Get the hostname of the machine
  gethostname(hostname, 1024);

  // Main transfer loop
  for (i = 0; (i < iterations) || (iterations == 0); i++) {

    // Reset key variables
    errbuf[0] = 0;
    requestId = "";
    requestStatus = "";

    // Generate a UUID
    Cuuid_t cuuid = nullCuuid;
    Cuuid_create(&cuuid);
    char uuid[CUUID_STRING_LEN];
    Cuuid2string(uuid, CUUID_STRING_LEN + 1, &cuuid);

    // Construct the directory path
    // Example: BASE_DIR/hostname/1432/20110406/01
    std::ostringstream dirpath("");
    dirpath << baseDirectory;              // Base directory
    dirpath << "/";
    dirpath << hostname;                   // Hostname
    dirpath << "/";
    dirpath << tid;                        // Thread id
    dirpath << "/";

    // Get the current time information. A) To construct the date and hour
    // components of the directory name and B) to record elapsed time.
    struct timeval tv, end;
    struct tm *tm;
    struct tm tm_buf;
    gettimeofday(&tv, NULL);
    tm = localtime_r((time_t *) &(tv.tv_sec), &tm_buf);
    sprintf(buffer, "%04d%02d%02d/%02d%d/",
            tm->tm_year + 1900,
            tm->tm_mon  + 1,
            tm->tm_mday,
            tm->tm_hour,
            emlinkoffset);

    dirpath << buffer;                     // Date and Hour

    // Construct the filepath
    std::ostringstream filepath("");
    filepath << dirpath.str();
    filepath << uuid;                      // Universal Unique Identifier

    // If we are in a PrepareToPut, Put, PutDone mode invoke PrepareToPut
    if (putDoneCycle) {
      PROCESSING_START;
      rc = SendPrepareToPutRequest(filepath.str(), requestId, requestStatus);
      if (rc != 0) {
        // If the error message is ENOENT (No such file or directory) it's most
        // likely that A) This is the first time any file has been written by
        // the test program and the parent directories haven yet to be created
        // or B) The hour or date component of the filepath have changed.
        // Create the parent directory on ENOENT (No such file or directory)
        if (serrno == ENOENT) {
          if (createParentDirectories(dirpath.str(), tid) == 0) {
            i--;
            continue;
          }
          // Unrecoverable error
          break;
        } 

        // If the error message is EMLINK (Too many links) this indicates that
        // the target directory is full, i.e. has more than 999,999 files in it.
        // This is highly unlikely but we cover the user case anyway by updating
        // the directory emlinkoffset. This will cause the next write to fail
        // resulting in a new directory being created.
        else if (serrno == EMLINK) {
          emlinkoffset++;
        }
        
        // An unexpected error, sleep a while and try again later.
        else {
          std::cerr << IDENT << "[ERROR] "
                    << "Failed to SendPrepareToPutRequest()"
                    << " - Path: " << filepath.str()
                    << " - Errno: " << sstrerror(serrno)
                    << std::endl;
          sleep(10);
        }
        
        // Unlink the target file from the namespace
        unlinkFile(filepath.str(), tid);
        continue;
      }
      
      // Log the StagePutRequest completion
      PROCESSING_END
        std::cout << IDENT << "[INFO] --- StagePutRequest: "
                  << requestId << " " << requestStatus
                  << ELAPSED_TIME << std::endl;
    }

    // Write the file
    PROCESSING_START;
    rc = writeFileUsingRFIO(filepath.str(), writeFileSize);
    if (rc != 0) {
      // Create the parent directory on ENOENT (No such file or directory). See
      // comment in putDoneCyclye block above.
      if (serrno == ENOENT) {
        if (createParentDirectories(dirpath.str(), tid) == 0) {
          i--;
          continue;
        }
        // Unrecoverable error
        break;
      }

      // If the error message is ENOSPC (No space left on device) then there
      // isn't much we can do but sleep a while and try again later.
      else if (serrno == ENOSPC) {
        sleep(10);
      }

      // If the error message is EMLINK (Too many links) this indicates that
      // the target directory is full, i.e. has more than 999,999 files in it.
      // This is highly unlikely but we cover the user case anyway by updating
      // the directory emlinkoffset. This will cause the next write to fail
      // resulting in a new directory being created.
      else if (serrno == EMLINK) {
        emlinkoffset++;
      }

      // An unexpected error, sleep a while and try again later.
      else {
        std::cerr << IDENT << "[ERROR] "
                  << "Failed to writeFileUsingRFIO()"
                  << " - Path: "    << filepath.str()
                  << " - Errno: "   << sstrerror(serrno)
                  << " - Message: " << errbuf
                  << std::endl;
        sleep(10);
      }
      
      // Unlink the target file from the namespace
      unlinkFile(filepath.str(), tid);
      continue;
    }
        
    // Log the writeFileUsingRFIO completion
    PROCESSING_END;
    std::cout << IDENT << "[INFO] --> RfioWrite: " << filepath.str()
              << ELAPSED_TIME << std::endl;

    // If we are in a PrepareToPut, Put, PutDone mode invoke PutDone
    if (putDoneCycle) {
      PROCESSING_START;
      rc = SendPutDoneRequest(filepath.str(), requestId, requestStatus);
      if (rc != 0) {
        // Unlink the previous calls to the stager and rfio the handling of
        // error conditions is much simpler as we have already taken care of
        // parent directory creation and too many files in a directory.
        unlinkFile(filepath.str(), tid);
        sleep(10);
        continue;
      }

      // Log the StagePutDonerequest completion
      PROCESSING_END
        std::cout << IDENT << "[INFO] --- StagePutDoneRequest: "
                  << requestId << " " << requestStatus
                  << ELAPSED_TIME << std::endl;
    }

    // Stat the file
    if (fileStat) {
      PROCESSING_START;
      Cns_filestatcs statbuf;
      rc = Cns_statcs(filepath.str().c_str(), &statbuf);
      if (rc != 0) {
        sleep(10);
        continue;
      }
      
      // Log the result of the stat operation
      PROCESSING_END;
      std::cout << IDENT << "[INFO[ --- CnsStat:"
                << " FileMode: " << statbuf.filemode
                << " Uid: " << statbuf.uid
                << " Gid: " << statbuf.gid
                << " FileSize: " << statbuf.filesize
                << " FileClass: " << statbuf.fileclass
                << " Status: " << statbuf.status
                << " ChecksumType: " << statbuf.csumtype
                << " ChecksumValue: " << statbuf.csumvalue
                << ELAPSED_TIME << std::endl;
    }

    // Perform a file query if needed
    if (fileQuery) {
      PROCESSING_START;
      rc = SendFileQueryRequest(filepath.str(), requestId, requestStatus);
      if (rc != 0) {
        sleep(10);
        continue;
      }

      // Log the StageFileQueryRequest completion
      PROCESSING_END;
      std::cout << IDENT << "[INFO] --- StageFileQueryRequest: "
                << requestId << " " << requestStatus
                << ELAPSED_TIME << std::endl;
    }

    // Read the file back
    for (j = 0; j < nbReads; j++) {
      PROCESSING_START;
      rc = readFileUsingRFIO(filepath.str(), writeFileSize);
      if (rc != 0) {
        std::cerr << IDENT << "[ERROR] "
                  << "Failed to readFileUsingRFIO()"
                  << " - Path: "    << filepath.str()
                  << " - Errno: "   << sstrerror(serrno)
                  << " - Message: " << errbuf
                  << std::endl;
        sleep(10);
        continue;
      }
      
      // Log the writeFileUsingRFIO completion
      PROCESSING_END;
      std::cout << IDENT << "[INFO] <-- RfioRead: " << filepath.str()
                << ELAPSED_TIME << std::endl;
    }

    // Sleep a bit if delay is greater than 0
    if (writeDelay > 0) {
      sleep(writeDelay);
    }
  }

  // Exit
  pthread_exit(0);
}

//-----------------------------------------------------------------------------
// Usage
//-----------------------------------------------------------------------------
void usage(const std::string programname) {
  std::cout
    << "Usage: " << programname << " [options]"
    << std::endl << " -h, --help                Display this help and exit"
    << std::endl << " -d, --basedir <path>      The CASTOR HSM directory where files will be written"
    << std::endl << "                           too and/or read from"
    << std::endl << "     --size <bytes>        The size of the file to transfer"
    << std::endl << "     --writers <nbthreads> The number of threads to use writing file into CASTOR"
    << std::endl << "     --nbfiles <count>     The number of files to create per thread, set to 0"
    << std::endl << "                           for infinite"
    << std::endl << "     --stager <hostname>   The name of the STAGER_HOST"
    << std::endl << "     --svcclass <name>     The name of the STAGE_SVCCCLASS"
    << std::endl << "     --delay <seconds>     Delay in seconds between each file transfer"
    << std::endl << "     --putdone             Enable PrepareToPut, Put, PutDone cycle"
    << std::endl << "     --fileqry             Perform a file query at the end of each transfer"
    << std::endl << "     --readers <times>     The number of times to read back a file"
    << std::endl << "     --stat                Stat the file in the namespace after creation"
    << std::endl << "     --daemonize           Run in the background"
    << std::endl
    << std::endl << "Report bugs to <castor-dev@cern.ch>"
    << std::endl;
}

//-----------------------------------------------------------------------------
// Main
//-----------------------------------------------------------------------------
int main (int argc, char **argv) {

  // Variables
  std::vector<pthread_t> threads;
  unsigned int nbthreads = DEFAULT_NUM_THREADS;
  unsigned int i = 0;
  int rc   = 0;
  int pid  = 0;
  char c   = 0;
  char *dp = NULL;

  // Long options
  static struct option longopts[] = {
    { "help",        no_argument,       NULL, 'h'                   },
    { "basedir",     required_argument, NULL, 'd'                   },
    { "size",        required_argument, NULL,  OPTION_SIZE          },
    { "writers",     required_argument, NULL,  OPTION_WRITERS       },
    { "stager",      required_argument, NULL,  OPTION_STAGER        },
    { "svcclass",    required_argument, NULL,  OPTION_SVCCLASS      },
    { "buffer-size", required_argument, NULL,  OPTION_BUFFERSIZE    },
    { "delay",       required_argument, NULL,  OPTION_DELAY         },
    { "nbfiles",     required_argument, NULL,  OPTION_NBFILES       },
    { "putdone",     no_argument,       NULL,  OPTION_PUTDONECYCLE  },
    { "fileqry",     no_argument,       NULL,  OPTION_FILEQUERY     },
    { "readers",     required_argument, NULL,  OPTION_NBREADS       },
    { "stat",        no_argument,       NULL,  OPTION_FILESTAT      },
    { "daemonize",   no_argument,       NULL,  OPTION_DAEMONIZE     },
    { NULL,          0,                 NULL,  0                    }
  };

  // Parse command line options
  while ((c = getopt_long(argc, argv, "hd:", longopts, NULL)) != EOF) {
    switch(c) {
      // Commands with no short option
    case OPTION_SIZE:
      writeFileSize = strutou64(optarg);
      if (writeFileSize < MIN_FILESIZE) {
        std::cerr << "Invalid argument: " << optarg
                  << " for option --size, must by > " << MIN_FILESIZE
                  << std::endl;
        return 2;
      }
      break;
    case OPTION_WRITERS:
      nbthreads = atoi(optarg);
      if ((nbthreads < 0) || (nbthreads > MAX_NBTHREADS)) {
        std::cerr << "Invalid argument: " << optarg
                  << " for option --writers, must be > 0 and < "
                  << MAX_NBTHREADS
                  << std::endl;
        return 2;
      }
      break;
    case OPTION_STAGER:
      stagerHost = optarg;
      break;
    case OPTION_SVCCLASS:
      stagerSvcClass = optarg;
      break;
    case OPTION_BUFFERSIZE:
      rfioBufferSize = atoi(optarg);
      if ((rfioBufferSize < 0) || (rfioBufferSize > MAX_RFIO_BUFFERSIZE)) {
        std::cerr << "Invalid argument: " << optarg
                  << " for option --buffer-sizer, must be > 0 and < "
                  << MAX_RFIO_BUFFERSIZE
                  << std::endl;
        return 2;
      }
      break;
    case OPTION_DELAY:
      writeDelay = atoi(optarg);
      break;
    case OPTION_NBFILES:
      if (((iterations = strtoull(optarg, &dp, 0)) < 0) ||
          (*dp != '\0') || (errno == ERANGE)) {
        std::cerr << "Invalid argument: " << optarg
                  << " for option --nbfiles, not a valid number"
                  << std::endl;
        return 2;
      }
      break;
    case OPTION_PUTDONECYCLE:
      putDoneCycle = true;
      break;
    case OPTION_FILEQUERY:
      fileQuery = true;
      break;
    case OPTION_NBREADS:
      nbReads = atoi(optarg);
      if ((nbReads < 0) || (nbReads > MAX_NBREADS)) {
        std::cerr << "Invalid argument: " << optarg
                  << " for option --readers, must be >= 0 and < "
                  << MAX_NBREADS
                  << std::endl;
        return 2;
      }
      break;
    case OPTION_FILESTAT:
      fileStat = true;
      break;
    case OPTION_DAEMONIZE:
      daemonize = true;
      break;

      // Commands with short option
    case 'h':  // --help
      usage(argv[0]);
      return 0;
    case 'd':  // --basedir
      baseDirectory = optarg;
      break;

      // Default
    case '?':
      break;
    default:
      break;
    }
  }

  // Check for unparsed command line options
  if (optind < argc) {
    std::cerr << "Unknown options: ";
    while (optind < argc)
      std::cerr << argv[optind++];
    std::cerr << std::endl;
    return 2;
  }

  // Check that mandatory options have been defined
  if (baseDirectory == "") {
    std::cerr << "Mandatory option --basedir not defined" << std::endl;
    usage(argv[0]);
    return 2;
  }

  // Initialize CASTOR threading library
  Cthread_init();
  
  // Log the test parameters
  std::cout << "Stager Host:        " << stagerHost
            << std::endl << "Stager SvcClass:    " << stagerSvcClass
            << std::endl << "NameServer Host:    " << stagerHost
            << std::endl
            << std::endl << "RFIO Buffer Size:   " << rfioBufferSize 
            << std::endl << "HSM Base Directory: " << baseDirectory
            << std::endl << "File Size:          " << writeFileSize
            << std::endl << "Nb Threads:         " << nbthreads
            << std::endl << "Loops per thread:   " << iterations
            << std::endl;

  // Run in the background if requested to do so
  if (daemonize) {
    pid = fork();
    if (pid < 0) {
      std::cerr << "Failed to fork(): " << strerror(errno) << std::endl;
      return 1;
    } else if (pid > 0) {
      return 0;  // The parent
    }
    
    // Create a new session
    setsid();
    
    // Redirect stdin to /dev/null
    freopen("/dev/null", "r", stdin);
  }

  // Setup the STAGER environment variables
  setenv("STAGE_HOST",    stagerHost.c_str(),     1);
  setenv("STAGE_SVCCLASS", stagerSvcClass.c_str(), 1);

  // Setup the CNS environment variables
  // Note: This assumes that a nameserver and rhd server run on the same target
  // stager
  if (getenv("CNS_HOST") == NULL) {
    setenv("CNS_HOST", stagerHost.c_str(), 1);
  }

  // Create a set of threads
  threads.reserve(nbthreads);
  for (i = 0; i < nbthreads; i++) {
    rc = pthread_create(&threads[i], NULL, (void *(*)(void *))worker, NULL);
    if (rc != 0) {
      std::cerr << "[ERROR] " << strerror(errno) << std::endl;
      return 1;
    }
  }

  // Wait for the threads to end
  for (i = 0; i < nbthreads; i++) {
    pthread_join(threads[i], NULL);
  }

  return 0;
}
