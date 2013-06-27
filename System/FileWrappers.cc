// ----------------------------------------------------------------------
// File: System/FileWrapper.cc
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

#include <errno.h>
#include <stddef.h>
#include <stdexcept> 
#include <sys/mtio.h>

#include "FileWrappers.hh"

int Tape::System::regularFile::ioctl(unsigned long int request, mtget* mt_status)
{
  /* Standard "no such operation" reply. */
  errno = EINVAL;
  return -1;
}

ssize_t Tape::System::regularFile::read(void* buf, size_t nbytes)
{
  try {
    ssize_t ret;
    ret = m_content.copy((char *) buf, nbytes, m_read_pointer);
    m_read_pointer += ret;
    return ret;
  } catch (std::out_of_range & e) {
    return 0;
  }
}

ssize_t Tape::System::regularFile::write(const void *buf, size_t nbytes)
{
  try {
    m_content.assign((const char *) buf, nbytes);
    return nbytes;
  } catch (std::length_error & e) {
    return -1;
  } catch (std::bad_alloc & e) {
    return -1;
  }
}

/**
 * Constructor for fake tape server: fill up status registers.
 */
Tape::System::stDeviceFile::stDeviceFile()
{
  m_mtStat.mt_type = 1;
  m_mtStat.mt_resid = 0;
  m_mtStat.mt_dsreg = (((256 * 0x400) & MT_ST_BLKSIZE_MASK) << MT_ST_BLKSIZE_SHIFT)
      | ((1 & MT_ST_DENSITY_MASK) << MT_ST_DENSITY_SHIFT);
  m_mtStat.mt_gstat = GMT_EOT(~0) | GMT_BOT(~0);
}

ssize_t Tape::System::stDeviceFile::read(void* buf, size_t nbytes)
{
  errno = EIO;
  return -1;
}

ssize_t Tape::System::stDeviceFile::write(const void *buf, size_t nbytes)
{
  errno = EIO;
  return -1;
}

int Tape::System::stDeviceFile::ioctl(unsigned long int request, mtget* mt_status)
{
  switch (request) {
    case MTIOCGET:
      *mt_status = m_mtStat;
      return 0;
  }
  errno = EINVAL;
  return -1;
}

ssize_t Tape::System::genericDeviceFile::read(void* buf, size_t nbytes)
{
  errno = EIO;
  return -1;
}

ssize_t Tape::System::genericDeviceFile::write(const void *buf, size_t nbytes)
{
  errno = EINVAL;
  return -1;
}

int Tape::System::genericDeviceFile::ioctl(unsigned long int request, mtget* mt_status)
{
  errno = EINVAL;
  return -1;
}
