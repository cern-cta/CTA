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

#include "Wrapper.hh"
#include <errno.h>
#include <limits.h>
#include <stdexcept>

using ::testing::_;
using ::testing::Invoke;

DIR* Tape::System::fakeWrapper::opendir(const char* name) {
  /* Manage absence of directory */
  if (m_directories.end() == m_directories.find(std::string(name))) {
    errno = ENOENT;
    return NULL;
  }
  /* Dirty pointer gymnastics. Good enough for a test harness */
  ourDIR * dir = new ourDIR;
  dir->nextIdx = 0;
  dir->path = name;
  return (DIR*) dir;
}

int Tape::System::fakeWrapper::closedir(DIR* dirp) {
  delete ((ourDIR *) dirp);
  return 0;
}

struct dirent * Tape::System::fakeWrapper::readdir(DIR* dirp) {
  /* Dirty pointer gymnastics. Good enough for a test harness */
  ourDIR & dir = *((ourDIR *) dirp);
  /* Check we did not reach end of directory. This will create a new
   * entry in the map if it does not exist, but we should be protected by
   * opendir.
   */
  if (dir.nextIdx + 1 > m_directories[dir.path].size())
    return NULL;
  dir.dent_name = m_directories[dir.path][dir.nextIdx++];
  strncpy(dir.dent.d_name, dir.dent_name.c_str(), NAME_MAX);
  return & (dir.dent);
}

int Tape::System::fakeWrapper::readlink(const char* path, char* buf, size_t len) {
  /*
   * Mimic readlink. see man 3 readlink.
   */
  if (m_links.end() == m_links.find(std::string(path))) {
    errno = ENOENT;
    return -1;
  }
  const std::string & link = m_links[std::string(path)];
  strncpy(buf, link.c_str(), len);
  return len > link.size() ? link.size() : len;
}

char * Tape::System::fakeWrapper::realpath(const char* name, char* resolved) {
  /*
   * Mimic realpath. see man 3 realpath.
   */
  if (m_realpathes.end() == m_realpathes.find(std::string(name))) {
    errno = ENOENT;
    return NULL;
  }
  strncpy(resolved, m_realpathes[std::string(name)].c_str(), PATH_MAX);
  return resolved;
}

int Tape::System::fakeWrapper::open(const char* file, int oflag) {
  /* 
   * Mimic open. See man 2 open.
   * We only allow read for the moment.
   */
  if (oflag & (O_APPEND | O_CREAT)) {
    errno = EACCES;
    return -1;
  }
  if (m_files.end() == m_files.find(std::string(file))) {
    errno = ENOENT;
    return -1;
  }
  int ret = m_nextFD++;
  m_openFiles[ret].data = m_files[std::string(file)];
  m_openFiles[ret].read_pointer = 0;
  return ret;
}

ssize_t Tape::System::fakeWrapper::read(int fd, void* buf, size_t nbytes) {
  /*
   * Mimic read. See man 2 read
   */
  if (m_openFiles.end() == m_openFiles.find(fd)) {
    errno = EBADF;
    return -1;
  }
  /*
   * Read. std::string::copy nicely does the heavy lifting.
   */
  try {
    size_t ret;
    ret = m_openFiles[fd].data.copy((char *) buf, nbytes, m_openFiles[fd].read_pointer);
    m_openFiles[fd].read_pointer += ret;
    return ret;
  } catch (std::out_of_range & e) {
    return 0;
  }
}

int Tape::System::fakeWrapper::close(int fd) {
  /*
   * Mimic close. See man 2 close
   */
  if (m_openFiles.end() == m_openFiles.find(fd)) {
    errno = EBADF;
    return -1;
  }
  m_openFiles.erase(fd);
  return 0;
}

void Tape::System::mockWrapper::delegateToFake() {
  ON_CALL(*this, opendir(_)).WillByDefault(Invoke(&fake, &fakeWrapper::opendir));
  ON_CALL(*this, readdir(_)).WillByDefault(Invoke(&fake, &fakeWrapper::readdir));
  ON_CALL(*this, closedir(_)).WillByDefault(Invoke(&fake, &fakeWrapper::closedir));
  ON_CALL(*this, readlink(_, _, _)).WillByDefault(Invoke(&fake, &fakeWrapper::readlink));
  ON_CALL(*this, realpath(_, _)).WillByDefault(Invoke(&fake, &fakeWrapper::realpath));
  ON_CALL(*this, open(_, _)).WillByDefault(Invoke(&fake, &fakeWrapper::open));
  ON_CALL(*this, read(_, _, _)).WillByDefault(Invoke(&fake, &fakeWrapper::read));
  ON_CALL(*this, close(_)).WillByDefault(Invoke(&fake, &fakeWrapper::close));
}

void Tape::System::fakeWrapper::setupSLC5() {
  /*
   * Setup an tree similar to what we'll find in
   * and SLC5 system with mvhtl library (one media exchanger, 2 drives)
   */
  /*
   * First of, the description of all devices in sysfs.
   * In SLC5, sysfs is mounted on /sys/. If other mount point appear in the
   * future, we'll have to provide /proc/mounts (and use it).
   * SLC6 is similar, so this is not necessary at the time of writing.
   */
  m_directories["/sys/bus/scsi/devices"].push_back("3:0:0:0");
  m_directories["/sys/bus/scsi/devices"].push_back("3:0:1:0");
  m_directories["/sys/bus/scsi/devices"].push_back("3:0:2:0");
  m_realpathes["/sys/bus/scsi/devices/3:0:0:0"]
          = "/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0";
  m_realpathes["/sys/bus/scsi/devices/3:0:1:0"]
          = "/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0";
  m_realpathes["/sys/bus/scsi/devices/3:0:2:0"]
          = "/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0";
  m_files["/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0/type"] = "8\n";
  m_files["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0/type"] = "1\n";
  m_files["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0/type"] = "1\n";
  m_links["/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0/generic"] 
          = "../../../../../../class/scsi_generic/sg2";
  m_links["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0/generic"]
          = "../../../../../../class/scsi_generic/sg0";
  m_links["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0/generic"]
          = "../../../../../../class/scsi_generic/sg1";
}
