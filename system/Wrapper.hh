// ----------------------------------------------------------------------
// File: System/Wrapper.hh
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#pragma once
#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <gmock/gmock.h>
#include <map>
#include <vector>
#include <string>
#include <fstream>

namespace Tape {
namespace System {
  /**
   * Wrapper class the all system calls used, allowing writing of test harnesses
   * for unit testing.
   * The member functions are purposedly non-virtual, allowing full
   * performance with inline member functions.
   */
  class realWrapper{
  public:
    DIR* opendir(const char *name) { return ::opendir(name); }
    struct dirent * readdir(DIR* dirp) { return ::readdir(dirp); }
    int closedir(DIR* dirp) { return ::closedir(dirp); }
    int readlink(const char* path, char* buf, size_t len)  { return ::readlink(path, buf, len); }
    char * realpath(const char* name, char* resolved) { return ::realpath(name, resolved); }
    int open(const char* file, int oflag) { return ::open(file, oflag); }
    ssize_t read(int fd, void* buf, size_t nbytes) { return ::read( fd, buf, nbytes); }
    int close(int fd) { return ::close(fd); }
  };
  
  /**
   * Intermediate class definition, allowing common ancestor between
   * mockWrapper and fakeWrapper (pure virtual)
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
    virtual int close(int fd) = 0;
  };
  
  /**
   * Fake class for system wrapper. Allows recording of pre-cooked filesystem elements,
   * once for each call separately.
   * Each test can then delegate (from mock) and configure 
   */
  class fakeWrapper: public virtualWrapper {
  public:
    fakeWrapper(): m_nextFD(0) {};
    virtual DIR* opendir(const char *name);
    virtual struct dirent * readdir(DIR* dirp); 
    virtual int closedir(DIR* dirp);
    virtual int readlink(const char* path, char* buf, size_t len);
    virtual char * realpath(const char* name, char* resolved);
    virtual int open(const char* file, int oflag);
    virtual ssize_t read(int fd, void* buf, size_t nbytes);
    virtual int close(int fd);
    std::map<std::string, std::vector<std::string> > m_directories;
    std::map<std::string, std::string> m_links;
    std::map<std::string, std::string> m_realpathes;
    std::map<std::string, std::string> m_files;
    void setupSLC5();
  private:
    struct ourDIR {
      std::string path;
      int nextIdx;
      struct dirent dent;
      std::string dent_name;
    };
    struct outFD {
      std::string data;
      size_t read_pointer;
    };
    std::map<int, outFD> m_openFiles;
    int m_nextFD;
  };
  
    /**
   * Mock class for system wrapper, used to develop tests.
   */
  class mockWrapper: public virtualWrapper {
  public:
    mockWrapper(): m_DIR((DIR*)&m_DIRfake) {
      ON_CALL(*this, opendir(::testing::_))
              .WillByDefault(::testing::Return(m_DIR));
    }
    MOCK_METHOD1(opendir, DIR*(const char *name));
    MOCK_METHOD1(readdir, dirent*(DIR* dirp));
    MOCK_METHOD1(closedir, int(DIR* dirp));
    MOCK_METHOD3(readlink, int(const char* path, char* buf, size_t len));
    MOCK_METHOD2(realpath, char *(const char* name, char* resolved));
    MOCK_METHOD2(open, int(const char* file, int oflag));
    MOCK_METHOD3(read, ssize_t(int fd, void* buf, size_t nbytes));
    MOCK_METHOD1(close, int(int fd));
    DIR* m_DIR;
    int m_DIRfake;
    void delegateToFake();
    fakeWrapper fake;
  };
} // namespace System
} // namespace Tape

