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

#include "VirtualWrapper.hpp"
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
   * Wrapper class the all system calls used, allowing writing of test harnesses
   * for unit testing. For simplicity, the members are virtual functions, and
   * the class implements the same interface as the unit test versions.
   * This add a virtual table lookup + function call for each system call,
   * but it is assumed to be affordable.
   */
  class realWrapper: public virtualWrapper {
  public:

    virtual DIR* opendir(const char *name) { return ::opendir(name); }
    virtual struct dirent * readdir(DIR* dirp) { return ::readdir(dirp); }
    virtual int closedir(DIR* dirp) { return ::closedir(dirp); }
    virtual int readlink(const char* path, char* buf, size_t len) { return ::readlink(path, buf, len); }
    virtual char * realpath(const char* name, char* resolved) { return ::realpath(name, resolved); }
    virtual int open(const char* file, int oflag) { return ::open(file, oflag); }
    virtual int ioctl(int fd, unsigned long int request, struct mtop * mt_cmd) {
      return ::ioctl(fd, request, mt_cmd);
    }
    virtual int ioctl(int fd, unsigned long int request, struct mtget * mt_status) {
      return ::ioctl(fd, request, mt_status);
    }
    virtual int ioctl(int fd, unsigned long int request, sg_io_hdr_t * sgh) {
      return ::ioctl(fd, request, sgh);
    }
    virtual ssize_t read(int fd, void* buf, size_t nbytes) { return ::read(fd, buf, nbytes); }
    virtual ssize_t write(int fd, const void *buf, size_t nbytes) { return ::write(fd, buf, nbytes); }
    virtual int close(int fd) { return ::close(fd); }
    virtual int stat(const char * path, struct stat *buf) { return ::stat(path, buf); }
    virtual castor::tape::tapeserver::drive::DriveInterface * 
      getDriveByPath(const std::string &) { return nullptr; }
  };

} // namespace System
} // namespace castor::tape
