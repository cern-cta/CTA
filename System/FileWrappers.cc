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
#include "../SCSI/Structures.hh"

ssize_t Tape::System::vfsFile::read(void* buf, size_t nbytes)
{
  /* The vfsFile's operations always fail */
  errno = EINVAL;
  return -1;
}

ssize_t Tape::System::vfsFile::write(const void* buf, size_t nbytes)
{
  /* The vfsFile's operations always fail */
  errno = EINVAL;
  return -1;
}

int Tape::System::vfsFile::ioctl(unsigned long int request, mtget* mt_status)
{
  /* The vfsFile's operations always fail */
  errno = EINVAL;
  return -1;
}

int Tape::System::vfsFile::ioctl(unsigned long int request, sg_io_hdr_t * sgio_h)
{
  /* The vfsFile's operations always fail */
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

int Tape::System::stDeviceFile::ioctl(unsigned long int request, sg_io_hdr_t * sgio_h)
{
  /* for the moment, just implement the SG_IO ioctl */
  switch (request) {
    case SG_IO:
      if (sgio_h->interface_id != 'S') {
        errno = ENOSYS;
        return -1;
      }
      switch (sgio_h->cmdp[0]) { // Operation Code for CDB
        case SCSI::Commands::READ_POSITION:
          SCSI::Structures::readPositionDataShortForm_t & positionData = 
              *(SCSI::Structures::readPositionDataShortForm_t *) sgio_h->dxferp;
          if (sizeof (positionData) > sgio_h->dxfer_len) {
            errno = EINVAL;
            return -1;
          }
          /* fill the replay with random data */
          srandom(SCSI::Commands::READ_POSITION);
          memset(sgio_h->dxferp, random(),sizeof (positionData)) ;
          
          /* we need this field to make the replay valid*/
          positionData.PERR = 0; 
          /* fill with expected values */
          positionData.firstBlockLocation[0] = 0xAB;
          positionData.firstBlockLocation[1] = 0xCD;
          positionData.firstBlockLocation[2] = 0xEF;
          positionData.firstBlockLocation[3] = 0x12;
          positionData.lastBlockLocation[3]  = 0xAB;
          positionData.lastBlockLocation[2]  = 0xCD;
          positionData.lastBlockLocation[1]  = 0xEF;
          positionData.lastBlockLocation[0]  = 0x12;
          positionData.blocksInBuffer[0]     = 0xAB;
          positionData.blocksInBuffer[1]     = 0xCD;
          positionData.blocksInBuffer[2]     = 0xEF;
          positionData.bytesInBuffer[3]      = 0xAB;
          positionData.bytesInBuffer[2]      = 0xCD;
          positionData.bytesInBuffer[1]      = 0xEF;
          positionData.bytesInBuffer[0]      = 0x12;       
          break;
        }
        return 0;
  }
  errno = EINVAL;
  return -1;
}

int Tape::System::tapeGenericDeviceFile::ioctl(unsigned long int request, sg_io_hdr_t * sgio_h)
{
  /* for the moment, just implement the SG_IO ioctl */
  switch (request) {
    case SG_IO:
      if (sgio_h->interface_id != 'S') {
        errno = ENOSYS;
        return -1;
      }
      /* TODO */
      return 0;
  }
  errno = EINVAL;
  return -1;
}

