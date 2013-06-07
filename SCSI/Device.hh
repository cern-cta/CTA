// ----------------------------------------------------------------------
// File: SCSI/Device.hh
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
#include <string>
#include <vector>
#include <sys/types.h>
#include <dirent.h>
#include "../system/Wrapper.hh"

#include "../Exception/Exception.hh"

#include <scsi/scsi.h>

namespace SCSI {

  /**
   * Bare-bones representation of a SCSI device
   */
  struct DeviceInfo {
    std::string sysfs_entry;
    int type;
    std::string sg_dev;
  };

  /**
   * Automatic lister of the system's SCSI devices 
   * @param sysWrapper 
   */
  template<class sysWrapperClass>
  class DeviceVector : public std::vector<DeviceInfo> {
  public:
    /**
     * Fill up the array that the device list is with all the system's
     * SCSI devices information.
     * 
     * (all code using templates must be in the header file)
     */

    /**
     * 
     * @param sysWrapper
     */
    DeviceVector(sysWrapperClass & sysWrapper) : m_sysWrapper(sysWrapper) {
      std::string sysDevsPath = "/sys/bus/scsi/devices";
      DIR* dirp = m_sysWrapper.opendir(sysDevsPath.c_str());
      if (!dirp) throw Tape::Exceptions::Errnum("Error opening sysfs scsi devs");
      while (struct dirent * dent = m_sysWrapper.readdir(dirp)) {
        std::string fullpath = sysDevsPath + "/" + std::string(dent->d_name);
        /* We expect only symbolic links in this directory, */
        char rp[PATH_MAX];
        if (NULL == m_sysWrapper.realpath(fullpath.c_str(), rp))
          throw Tape::Exceptions::Errnum();
        this->push_back(getDeviceInfo(rp));
      }
      sysWrapper.closedir(dirp);
    }
  private:
#undef ConvenientCoding
#ifdef ConvenientCoding
    synthax error here!!!; /* So we don't compile in this configuration */
    /* This makes code completion more efficient in editors */
    Tape::System::fakeWrapper & m_sysWrapper;
#else
    sysWrapperClass & m_sysWrapper;
#endif


    static const size_t readfileBlockSize = 1024;

    std::string readfile(std::string path) {
      int fd = m_sysWrapper.open(path.c_str(), 0);
      if (-1 == fd) {
        throw Tape::Exceptions::Errnum();
      }
      char buf[readfileBlockSize];
      std::string ret;
      while (ssize_t sread = m_sysWrapper.read(fd, buf, readfileBlockSize)) {
        if (-1 == sread) throw Tape::Exceptions::Errnum();
        ret.append(buf, sread);
      }
      if (m_sysWrapper.close(fd)) throw Tape::Exceptions::Errnum();
      return ret;
    }

    /**
     * Extract information from sysfs about a SCSI device.
     * @param path Path to the directory with information about 
     * @return 
     */
    DeviceInfo getDeviceInfo(const char * path) {
      DeviceInfo ret;
      ret.sysfs_entry = path;
      std::string buf;
      /* Get device type */
      {
        buf = readfile(ret.sysfs_entry + "/type");
        if (!sscanf(buf.c_str(), "%d", &ret.type))
          throw Tape::Exception(std::string("Could not parse file: ") + ret.sysfs_entry + "/type");
      }
      /* Get name of sg device */
      {
        char rl[PATH_MAX];
        std::string lp = ret.sysfs_entry + "/generic";
        if (-1 == m_sysWrapper.readlink(lp.c_str(), rl, sizeof (rl)))
          throw Tape::Exceptions::Errnum();
        std::string gl(rl);
        size_t pos = gl.find_last_of("/");
        if (pos == std::string::npos)
          throw Tape::Exception(std::string("Could not find last / in link: ") + gl +
                " read from " + ret.sysfs_entry + "/generic");
        ret.sg_dev = gl.substr(pos + 1);
      }
      return ret;
    }

  }; /* class DeviceVector */
};
