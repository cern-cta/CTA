/******************************************************************************
 *                      SCSI/Device.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once
#include <string>
#include <vector>
#include <regex.h>
#include <sys/types.h>
#include <dirent.h>
#include "../System/Wrapper.hpp"
#include "../Exception/Exception.hpp"
#include "../Utils/Regex.hpp"
#include "Constants.hpp"

namespace castor {
namespace tape {
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
    
    std::string vendor;
    std::string product;
    std::string productRevisionLevel;
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
    DeviceVector(castor::tape::System::virtualWrapper & sysWrapper);
  private:
    castor::tape::System::virtualWrapper & m_sysWrapper;

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
} // namespace SCSI
} // namespace tape
} // namespace castor
