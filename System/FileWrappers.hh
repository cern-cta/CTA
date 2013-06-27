// ----------------------------------------------------------------------
// File: System/FileWrapper.hh
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
#include <sys/types.h>
#include <sys/mtio.h>

namespace Tape {
namespace System {
/**
 * An abstract class allowing simple open/read/close/ioctl interface simulating
 * different types of files (regular files, device files (like tape devices)
 */
  class vfsFile {
  public:
    virtual ~vfsFile() {};
    virtual ssize_t read(void* buf, size_t nbytes) = 0;
    virtual ssize_t write(const void *buf, size_t nbytes) = 0;
    virtual int ioctl(unsigned long int request, struct mtget * mt_status) = 0;
    /** Reset the read/write pointers at open. With this, we can use the trivial copy operator. */
    virtual void reset() = 0;
  };
  
  /**
   * Class representing real files
   */
  class regularFile: public vfsFile {
  public:
    regularFile(): m_read_pointer(0) {};
    regularFile(const std::string & c): m_content(c), m_read_pointer(0) {};
    virtual void reset() { m_read_pointer = 0; };
    void operator = (const std::string & s) { m_content = s; m_read_pointer = 0; }
    virtual ssize_t read(void* buf, size_t nbytes);
    virtual ssize_t write(const void *buf, size_t nbytes);
    virtual int ioctl(unsigned long int request, struct mtget * mt_status);
  private:
    std::string m_content;
    int m_read_pointer;
  };
  /**
     * Class representing a tape device
     */
  class stDeviceFile : public vfsFile {
  public:
    stDeviceFile();
    virtual void reset() {};
    virtual ssize_t read(void* buf, size_t nbytes);
    virtual ssize_t write(const void *buf, size_t nbytes);
    virtual int ioctl(unsigned long int request, struct mtget * mt_status);
  private:
    struct mtget m_mtStat;
  };
  
  /**
   * Generic SCSI devices
   */
  class genericDeviceFile: public vfsFile {
  public:
    genericDeviceFile() {};
    virtual void reset() {};
    virtual ssize_t read(void* buf, size_t nbytes);
    virtual ssize_t write(const void *buf, size_t nbytes);
    virtual int ioctl(unsigned long int request, struct mtget * mt_status);
  };
} // namespace System
} // namespace Tape
