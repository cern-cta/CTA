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

#include <string>
#include <vector>
#include <sys/types.h>
#include <dirent.h>

namespace SCSI {

    /**
   * Bare-bones representation of a SCSI device
   */
  struct Device {
    std::string sg_path;

  };

  /**
   * Automatic lister of the system's SCSI devices 
   */
  template<class sysWrapperClass>
  class DeviceList : public std::vector<Device> {
  public:
    /**
     * Fill up the array that the device list is with all the system's
     * SCSI devices information.
     * 
     * (all code using templates must be in the header file)
     */
    DeviceList(sysWrapperClass & sysWrapper) {
      std::string sysDevsPath = "/sys/bus/scsi/devices";
      DIR* dirp = sysWrapper.opendir(sysDevsPath.c_str());
      if (!dirp) return; /* TODO: throw exception? */
      while (struct dirent * dent = sysWrapper.readdir(dirp)) {
        struct Device dev;
        dev.sg_path = sysDevsPath + "/" + dent->d_name;
      }
      sysWrapper.closedir(dirp);
    }
  };
};
