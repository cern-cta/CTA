/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once


#include <vector>
#include <regex.h>
#include <sys/types.h>
#include <dirent.h>
#include "../system/Wrapper.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/Regex.hpp"
#include "Constants.hpp"
#include <string>

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
        major = 0;
        minor = 0;
      }
      unsigned int major;
      unsigned int minor;

      bool operator !=(const DeviceFile& b) const {
        return major != b.major || minor != b.minor;
      }
      
      bool operator ==(const struct stat & sbuff) const {
        return major == major(sbuff.st_rdev) && minor == minor(sbuff.st_rdev);
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
    
    /**
     * Find an array element that shares the same device files as one pointed
     * to as a path. This is designed to be used with a symlink like:
     * /dev/tape_T10D6116 -> /dev/nst0
     */
    DeviceInfo & findBySymlink(std::string path);
    
    /**
     * Exception for previous function
     */
    class NotFound: public cta::exception::Exception {
    public:
      NotFound(const std::string& what): cta::exception::Exception(what) {}
    };
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

} // namespace SCSI
} // namespace tape
} // namespace castor
