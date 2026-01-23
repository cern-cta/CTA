/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once
#include "FileWrappers.hpp"
#include "VirtualWrapper.hpp"

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
   * Wrapper class the all system calls used, allowing writing of test harnesses
   * for unit testing. For simplicity, the members are virtual functions, and
   * the class implements the same interface as the unit test versions.
   * This add a virtual table lookup + function call for each system call,
   * but it is assumed to be affordable.
   */
class realWrapper : public virtualWrapper {
public:
  virtual DIR* opendir(const char* name) { return ::opendir(name); }

  virtual struct dirent* readdir(DIR* dirp) { return ::readdir(dirp); }

  virtual int closedir(DIR* dirp) { return ::closedir(dirp); }

  virtual int readlink(const char* path, char* buf, size_t len) { return ::readlink(path, buf, len); }

  virtual char* realpath(const char* name, char* resolved) { return ::realpath(name, resolved); }

  virtual int open(const char* file, int oflag) { return ::open(file, oflag); }

  virtual int ioctl(int fd, unsigned long int request, struct mtop* mt_cmd) { return ::ioctl(fd, request, mt_cmd); }

  virtual int ioctl(int fd, unsigned long int request, struct mtget* mt_status) {
    return ::ioctl(fd, request, mt_status);
  }

  virtual int ioctl(int fd, unsigned long int request, sg_io_hdr_t* sgh) { return ::ioctl(fd, request, sgh); }

  virtual ssize_t read(int fd, void* buf, size_t nbytes) { return ::read(fd, buf, nbytes); }

  virtual ssize_t write(int fd, const void* buf, size_t nbytes) { return ::write(fd, buf, nbytes); }

  virtual int close(int fd) { return ::close(fd); }

  virtual int stat(const char* path, struct stat* buf) { return ::stat(path, buf); }

  virtual castor::tape::tapeserver::drive::DriveInterface* getDriveByPath(const std::string&) { return nullptr; }
};

}  // namespace System
}  // namespace castor::tape
