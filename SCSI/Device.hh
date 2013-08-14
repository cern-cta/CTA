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
#include <regex.h>
#include <sys/types.h>
#include <dirent.h>
#include "../System/Wrapper.hh"
#include "../Exception/Exception.hh"
#include "../Utils/Regex.hh"
#include "Constants.hh"

namespace SCSI {
  /**
   * Bare-bones representation of a SCSI device
   */
  struct DeviceInfo {
    std::string sysfs_entry;
    int type;
    std::string sg_dev;
    std::string st_dev;
    std::string nst_dev;

    class DeviceFile {
    public:

      /* We avoid being hit by the macros major() and minor() by using a longer syntax */
      DeviceFile() {
        major = -1;
        minor = -1;
      }
      int major;
      int minor;

      bool operator !=(const DeviceFile& b) const {
        return major != b.major || minor != b.minor;
      }
    };
    DeviceFile sg;
    DeviceFile st;
    DeviceFile nst;
  };

  /**
   * Automatic lister of the system's SCSI devices 
   */
  class DeviceVector : public std::vector<DeviceInfo> {
  public:

    /**
     * Fill up the array that the device list is with all the system's
     * SCSI devices information.
     * 
     * (all code using templates must be in the header file)
     */
    DeviceVector(Tape::System::virtualWrapper & sysWrapper);
  private:
    Tape::System::virtualWrapper & m_sysWrapper;

    static const size_t readfileBlockSize = 1024;

    std::string readfile(std::string path);

    DeviceInfo::DeviceFile readDeviceFile(std::string path);

    DeviceInfo::DeviceFile statDeviceFile(std::string path);

    /**
     * Part factored out of getDeviceInfo: get the tape specifics
     * from sysfs.
     * @param devinfo
     */
    void getTapeInfo(DeviceInfo & devinfo);

    /**
     * Extract information from sysfs about a SCSI device.
     * @param path Path to the directory with information about 
     * @return 
     */
    DeviceInfo getDeviceInfo(const char * path);

  }; /* class DeviceVector */

  class Device {
  public:
    Device(int fd): m_fd(fd) {};
    
  private:
    int m_fd;
  }; // class Device
} /* namespace SCSI */
