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
#include <fstream>

#include "../drive/DriveInterface.hpp"
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
   * Fake class for system wrapper. Allows recording of pre-cooked filesystem elements,
   * once for each call separately.
   * Each test can then delegate (from mock) and configure
   */
class fakeWrapper : public virtualWrapper {
public:
  fakeWrapper() {};
  virtual DIR* opendir(const char* name);
  virtual struct dirent* readdir(DIR* dirp);
  virtual int closedir(DIR* dirp);
  virtual int readlink(const char* path, char* buf, size_t len);
  virtual char* realpath(const char* name, char* resolved);
  virtual int open(const char* file, int oflag);
  virtual int ioctl(int fd, unsigned long int request, struct mtop* mt_cmd);
  virtual int ioctl(int fd, unsigned long int request, struct mtget* mt_status);
  virtual int ioctl(int fd, unsigned long int request, sg_io_hdr_t* sgh);
  virtual ssize_t read(int fd, void* buf, size_t nbytes);
  virtual ssize_t write(int fd, const void* buf, size_t nbytes);
  virtual int close(int fd);
  virtual int stat(const char* path, struct stat* buf);
  virtual castor::tape::tapeserver::drive::DriveInterface* getDriveByPath(const std::string& path);
  std::map<std::string, std::vector<std::string>> m_directories;
  std::map<std::string, std::string> m_links;
  std::map<std::string, std::string> m_realpathes;
  std::map<std::string, vfsFile*> m_files;
  std::map<std::string, struct stat> m_stats;
  std::map<std::string, regularFile> m_regularFiles;
  std::map<std::string, stDeviceFile*> m_stFiles;
  std::map<std::string, std::unique_ptr<tapeserver::drive::DriveInterface>> m_pathToDrive;
  void setupSLC5();
  void setupSLC6();
  void setupForVirtualDriveSLC6();
  void referenceFiles();
  virtual ~fakeWrapper();

private:
  struct ourDIR {
    std::string path;
    unsigned int nextIdx;
    struct dirent dent;
    std::string dent_name;
  };

  std::map<int, vfsFile*> m_openFiles;
  int m_nextFD = 0;
};

/**
   * Mock class for system wrapper, used to develop tests.
   */
class mockWrapper : public virtualWrapper {
public:
  mockWrapper();
  MOCK_METHOD1(opendir, DIR*(const char* name));
  MOCK_METHOD1(readdir, dirent*(DIR* dirp));
  MOCK_METHOD1(closedir, int(DIR* dirp));
  MOCK_METHOD3(readlink, int(const char* path, char* buf, size_t len));
  MOCK_METHOD2(realpath, char*(const char* name, char* resolved));
  MOCK_METHOD2(open, int(const char* file, int oflag));
  MOCK_METHOD3(read, ssize_t(int fd, void* buf, size_t nbytes));
  MOCK_METHOD3(write, ssize_t(int fd, const void* buf, size_t nbytes));
  MOCK_METHOD3(ioctl, int(int fd, unsigned long int request, struct mtop* mt_cmd));
  MOCK_METHOD3(ioctl, int(int fd, unsigned long int request, struct mtget* mt_status));
  MOCK_METHOD3(ioctl, int(int fd, unsigned long int request, sg_io_hdr_t* sgh));
  MOCK_METHOD1(close, int(int fd));
  MOCK_METHOD2(stat, int(const char*, struct stat*));
  MOCK_METHOD1(getDriveByPath, castor::tape::tapeserver::drive::DriveInterface*(const std::string&) );
  DIR* m_DIR;
  int m_DIRfake;
  void delegateToFake();
  void disableGMockCallsCounting();
  fakeWrapper fake;
};
}  // namespace System
}  // namespace castor::tape
