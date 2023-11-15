/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <regex.h>
#include <sys/types.h>
#include <dirent.h>

#include <vector>
#include <string>

#include "../system/Wrapper.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/Regex.hpp"
#include "Constants.hpp"

namespace castor::tape::SCSI {

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
  explicit DeviceVector(castor::tape::System::virtualWrapper & sysWrapper);

  /**
   * Find an array element that shares the same device files as one pointed
   * to as a path. This is designed to be used with a symlink like:
   * /dev/tape_T10D6116 -> /dev/nst0
   */
  DeviceInfo & findBySymlink(const std::string& path);

  /**
   * Exception for previous function
   */
  class NotFound: public cta::exception::Exception {
   public:
    explicit NotFound(const std::string& what): cta::exception::Exception(what) {}
  };

 private:
  castor::tape::System::virtualWrapper & m_sysWrapper;

  static const size_t readfileBlockSize = 1024;

  std::string readfile(const std::string& path);

  DeviceInfo::DeviceFile readDeviceFile(const std::string& path);

  DeviceInfo::DeviceFile statDeviceFile(const std::string& path);

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

} // namespace castor::tape::SCSI
