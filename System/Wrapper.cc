// ----------------------------------------------------------------------
// File: System/Wrapper.cc
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
#include <scsi/sg.h>

using ::testing::_;
using ::testing::A;
using ::testing::An;
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
  m_openFiles[ret] = m_files[std::string(file)];
  m_openFiles[ret]->reset();
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
  return m_openFiles[fd]->read(buf, nbytes);
}

ssize_t Tape::System::fakeWrapper::write(int fd, const void *buf, size_t nbytes) {
  if (m_openFiles.end() == m_openFiles.find(fd)) {
    errno = EBADF;
    return -1;
  }
  return m_openFiles[fd]->write(buf, nbytes);
}

int Tape::System::fakeWrapper::ioctl(int fd, unsigned long int request, mtop * mt_cmd) {
  /*
   * Mimic ioctl. Actually delegate the job to a vfsFile
   */
  if (m_openFiles.end() == m_openFiles.find(fd)) {
    errno = EBADF;
    return -1;
  }
  return m_openFiles[fd]->ioctl(request, mt_cmd);
}

int Tape::System::fakeWrapper::ioctl(int fd, unsigned long int request, mtget* mt_status) {
  /*
   * Mimic ioctl. Actually delegate the job to a vfsFile
   */
  if (m_openFiles.end() == m_openFiles.find(fd)) {
    errno = EBADF;
    return -1;
  }
  return m_openFiles[fd]->ioctl(request, mt_status);
}

int Tape::System::fakeWrapper::ioctl(int fd, unsigned long int request, sg_io_hdr_t * sgh) {
  /*
   * Mimic ioctl. Actually delegate the job to a vfsFile
   */
  if (m_openFiles.end() == m_openFiles.find(fd)) {
    errno = EBADF;
    return -1;
  }
  return m_openFiles[fd]->ioctl(request, sgh);
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

int Tape::System::fakeWrapper::stat(const char* path, struct stat* buf) {
  /*
   * Mimic stat. See man 2 stat
   */
  if (m_stats.end() == m_stats.find(std::string(path))) {
    errno = ENOENT;
    return -1;
  }
  *buf = m_stats[std::string(path)];
  return 0;
}

/**
 * Function merging all types of files into a single pointer
 * based map. This allows usage of polymorphic 
 */
void Tape::System::fakeWrapper::referenceFiles() {
  for (std::map<std::string, regularFile>::iterator i = m_regularFiles.begin();
          i != m_regularFiles.end(); i++)
    m_files[i->first] = &m_regularFiles[i->first];
  for (std::map<std::string, stDeviceFile>::iterator i = m_stFiles.begin();
          i != m_stFiles.end(); i++)
    m_files[i->first] = &m_stFiles[i->first];
  for (std::map<std::string, tapeGenericDeviceFile>::iterator i = m_genericFiles.begin();
      i != m_genericFiles.end(); i++)
    m_files[i->first] = &m_genericFiles[i->first];
}

Tape::System::mockWrapper::mockWrapper() {
  m_DIR = reinterpret_cast<DIR*> (& m_DIRfake);
  ON_CALL(*this, opendir(::testing::_))
      .WillByDefault(::testing::Return(m_DIR));
}

void Tape::System::mockWrapper::delegateToFake() {
  ON_CALL(*this, opendir(_)).WillByDefault(Invoke(&fake, &fakeWrapper::opendir));
  ON_CALL(*this, readdir(_)).WillByDefault(Invoke(&fake, &fakeWrapper::readdir));
  ON_CALL(*this, closedir(_)).WillByDefault(Invoke(&fake, &fakeWrapper::closedir));
  ON_CALL(*this, readlink(_, _, _)).WillByDefault(Invoke(&fake, &fakeWrapper::readlink));
  ON_CALL(*this, realpath(_, _)).WillByDefault(Invoke(&fake, &fakeWrapper::realpath));
  ON_CALL(*this, open(_, _)).WillByDefault(Invoke(&fake, &fakeWrapper::open));
  ON_CALL(*this, read(_, _, _)).WillByDefault(Invoke(&fake, &fakeWrapper::read));
  ON_CALL(*this, write(_, _, _)).WillByDefault(Invoke(&fake, &fakeWrapper::write));
  /* We have an overloaded function. Have to use a static_cast trick to indicate
   the pointer to which function we want.*/
  ON_CALL(*this, ioctl(_, _, A<struct mtop *>())).WillByDefault(Invoke(&fake, 
        static_cast<int(fakeWrapper::*)(int , unsigned long int , mtop*)>(&fakeWrapper::ioctl)));
  ON_CALL(*this, ioctl(_, _, A<struct mtget *>())).WillByDefault(Invoke(&fake, 
        static_cast<int(fakeWrapper::*)(int , unsigned long int , mtget*)>(&fakeWrapper::ioctl)));
  ON_CALL(*this, ioctl(_, _, A<struct sg_io_hdr *>())).WillByDefault(Invoke(&fake, 
        static_cast<int(fakeWrapper::*)(int , unsigned long int , sg_io_hdr_t*)>(&fakeWrapper::ioctl)));
  ON_CALL(*this, close(_)).WillByDefault(Invoke(&fake, &fakeWrapper::close));
  ON_CALL(*this, stat(_, _)).WillByDefault(Invoke(&fake, &fakeWrapper::stat));
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
  m_directories["/sys/bus/scsi/devices"].push_back(".");
  m_directories["/sys/bus/scsi/devices"].push_back("..");
  m_directories["/sys/bus/scsi/devices"].push_back("3:0:0:0");
  m_directories["/sys/bus/scsi/devices"].push_back("3:0:1:0");
  m_directories["/sys/bus/scsi/devices"].push_back("3:0:2:0");
  m_realpathes["/sys/bus/scsi/devices/3:0:0:0"]
          = "/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0";
  m_realpathes["/sys/bus/scsi/devices/3:0:1:0"]
          = "/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0";
  m_realpathes["/sys/bus/scsi/devices/3:0:2:0"]
          = "/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0";
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0/type"] = "8\n";
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0/type"] = "1\n";
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0/type"] = "1\n";
  m_links["/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0/generic"]
          = "../../../../../../class/scsi_generic/sg2";
  m_links["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0/generic"]
          = "../../../../../../class/scsi_generic/sg0";
  m_links["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0/generic"]
          = "../../../../../../class/scsi_generic/sg1";
  m_stats["/dev/sg0"].st_rdev = makedev(21, 0);
  m_stats["/dev/sg0"].st_mode = S_IFCHR;
  m_stats["/dev/sg1"].st_rdev = makedev(21, 1);
  m_stats["/dev/sg1"].st_mode = S_IFCHR;
  m_stats["/dev/sg2"].st_rdev = makedev(21, 2);
  m_stats["/dev/sg2"].st_mode = S_IFCHR;
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:0/3:0:0:0/generic/dev"] = "21:2\n";
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0/generic/dev"] = "21:0\n";
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0/generic/dev"] = "21:1\n";
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back(".");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("..");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("bus");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("delete");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("device_blocked");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("driver");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("generic");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("iocounterbits");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("iodone_cnt");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("ioerr_cnt");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("iorequest_cnt");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("model");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("power");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("queue_depth");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("queue_type");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("rescan");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("rev");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_device:3:0:1:0");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_generic:sg0");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_level");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_tape:nst0");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_tape:nst0a");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_tape:nst0l");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_tape:nst0m");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_tape:st0");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_tape:st0a");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_tape:st0l");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("scsi_tape:st0m");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("state");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("subsystem");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("tape");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("timeout");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("type");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("uevent");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0"].push_back("vendor");
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0/scsi_tape:st0/dev"] =
          "9:0\n";
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:1/3:0:1:0/scsi_tape:nst0/dev"] =
          "9:128\n";
  m_stats["/dev/st0"].st_rdev = makedev(9, 0);
  m_stats["/dev/st0"].st_mode = S_IFCHR;
  m_stats["/dev/nst0"].st_rdev = makedev(9, 128);
  m_stats["/dev/nst0"].st_mode = S_IFCHR;
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back(".");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("..");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("bus");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("delete");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("device_blocked");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("driver");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("generic");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("iocounterbits");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("iodone_cnt");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("ioerr_cnt");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("iorequest_cnt");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("model");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("power");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("queue_depth");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("queue_type");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("rescan");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("rev");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_device:3:0:2:0");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_generic:sg1s");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_level");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_tape:nst1");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_tape:nst1a");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_tape:nst1l");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_tape:nst1m");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_tape:st1");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_tape:st1a");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_tape:st1l");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("scsi_tape:st1m");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("state");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("subsystem");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("tape");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("timeout");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("type");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("uevent");
  m_directories["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0"].push_back("vendor");
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0/scsi_tape:st1/dev"] =
          "9:1\n";
  m_regularFiles["/sys/devices/pseudo_0/adapter0/host3/target3:0:2/3:0:2:0/scsi_tape:nst1/dev"] =
          "9:129\n";
  m_stats["/dev/st1"].st_rdev = makedev(9, 1);
  m_stats["/dev/st1"].st_mode = S_IFCHR;
  m_stats["/dev/nst1"].st_rdev = makedev(9, 129);
  m_stats["/dev/nst1"].st_mode = S_IFCHR;
  m_stFiles["/dev/nst0"];
  m_stFiles["/dev/nst1"];
  m_genericFiles["/dev/sg0"];
  m_genericFiles["/dev/sg1"];
  m_genericFiles["/dev/sg2"];
  referenceFiles();
}
