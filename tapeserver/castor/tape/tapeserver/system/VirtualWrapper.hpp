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

#pragma once
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>

#include <gmock/gmock.h>
#include <map>
#include <vector>
#include <string>
#include <fstream>

#include "FileWrappers.hpp"

namespace castor::tape {

/** 
 * Forward declaration for pointer type in virutalWrapper.
 */
namespace tapeserver::drive {
  class DriveInterface;
}

namespace System {

  /**
   * Interface class definition, allowing common ancestor between
   * realWrapper, mockWrapper and fakeWrapper
   */
  
  class virtualWrapper {
  public:
    virtual DIR* opendir(const char *name) = 0;
    virtual struct dirent * readdir(DIR* dirp) = 0;
    virtual int closedir(DIR* dirp) = 0;
    virtual int readlink(const char* path, char* buf, size_t len) = 0;
    virtual char * realpath(const char* name, char* resolved) = 0;
    virtual int open(const char* file, int oflag) = 0;
    virtual ssize_t read(int fd, void* buf, size_t nbytes) = 0;
    virtual ssize_t write(int fd, const void *buf, size_t nbytes) = 0;
    /* The ... (variable arguments) notation will not work with GMock.
     * We have to create one overload for each case we encounter. */
    virtual int ioctl(int fd, unsigned long int request, struct mtop * mt_cmd) = 0;
    virtual int ioctl(int fd, unsigned long int request, struct mtget * mt_status) = 0;
    virtual int ioctl(int fd, unsigned long int request, sg_io_hdr_t * sgh) = 0;
    virtual int close(int fd) = 0;
    virtual int stat(const char * path, struct stat *buf) = 0;
    virtual ~virtualWrapper() = default;
    /** Hook allowing the pre-allocation of a tape drive in the test environment.
     * will  */
    virtual castor::tape::tapeserver::drive::DriveInterface *
      getDriveByPath(const std::string & path) = 0;
  };

} // namespace System
} // namespace castor::tape
