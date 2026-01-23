/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once
#include "FileWrappers.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <gmock/gmock.h>
#include <map>
#include <string>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

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
  virtual DIR* opendir(const char* name) = 0;
  virtual struct dirent* readdir(DIR* dirp) = 0;
  virtual int closedir(DIR* dirp) = 0;
  virtual int readlink(const char* path, char* buf, size_t len) = 0;
  virtual char* realpath(const char* name, char* resolved) = 0;
  virtual int open(const char* file, int oflag) = 0;
  virtual ssize_t read(int fd, void* buf, size_t nbytes) = 0;
  virtual ssize_t write(int fd, const void* buf, size_t nbytes) = 0;
  /* The ... (variable arguments) notation will not work with GMock.
     * We have to create one overload for each case we encounter. */
  virtual int ioctl(int fd, unsigned long int request, struct mtop* mt_cmd) = 0;
  virtual int ioctl(int fd, unsigned long int request, struct mtget* mt_status) = 0;
  virtual int ioctl(int fd, unsigned long int request, sg_io_hdr_t* sgh) = 0;
  virtual int close(int fd) = 0;
  virtual int stat(const char* path, struct stat* buf) = 0;
  virtual ~virtualWrapper() = default;
  /** Hook allowing the pre-allocation of a tape drive in the test environment.
     * will  */
  virtual castor::tape::tapeserver::drive::DriveInterface* getDriveByPath(const std::string& path) = 0;
};

}  // namespace System
}  // namespace castor::tape
