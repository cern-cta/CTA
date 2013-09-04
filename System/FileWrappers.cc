// ----------------------------------------------------------------------
// File: System/FileWrapper.cc
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

int Tape::System::vfsFile::ioctl(unsigned long int request, mtop * mt_cmd)
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
 * Constructor for fake tape server: fill up status registers
 * and internal structures.
 */
Tape::System::stDeviceFile::stDeviceFile()
{
  m_mtStat.mt_type = 1;
  m_mtStat.mt_resid = 0;
  m_mtStat.mt_dsreg = (((256 * 0x400) & MT_ST_BLKSIZE_MASK) << MT_ST_BLKSIZE_SHIFT)
      | ((1 & MT_ST_DENSITY_MASK) << MT_ST_DENSITY_SHIFT);
  m_mtStat.mt_gstat = GMT_EOT(~0) | GMT_BOT(~0);
  
  blockID = 0xFFFFFFFF; // Logical Object ID - position on tape 
  
  clearCompressionStats = false;
}

int Tape::System::stDeviceFile::ioctl(unsigned long int request, struct mtop * mt_cmd)
{
  switch (request) {
    case MTIOCTOP:
      *mt_cmd = m_mtCmd;
      return 0;
  }
  errno = EINVAL;
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
          return ioctlReadPosition(sgio_h);
        case SCSI::Commands::LOG_SELECT:
          return ioctlLogSelect(sgio_h);
        case SCSI::Commands::LOCATE_10:
          return ioctlLocate10(sgio_h);
        case SCSI::Commands::LOG_SENSE:
          return ioctlLogSense(sgio_h);       
        case SCSI::Commands::MODE_SENSE_6:
          return ioctlModSense6(sgio_h);
        case SCSI::Commands::MODE_SELECT_6:
          return ioctlModSelect6(sgio_h);
        case SCSI::Commands::INQUIRY:
          return ioctlInquiry(sgio_h);
      }
      return 0;
  }
  errno = EINVAL;
  return -1;
}

int Tape::System::stDeviceFile::ioctlReadPosition(sg_io_hdr_t* sgio_h) {
  if (SG_DXFER_FROM_DEV != sgio_h->dxfer_direction) {
    errno = EINVAL;
    return -1;
  }
  SCSI::Structures::readPositionDataShortForm_t & positionData =
          *(SCSI::Structures::readPositionDataShortForm_t *) sgio_h->dxferp;
  if (sizeof (positionData) > sgio_h->dxfer_len) {
    errno = EINVAL;
    return -1;
  }
  if (0xFFFFFFFF == blockID) { // there was no seek on tape
    /* fill the replay with random data */
    srandom(SCSI::Commands::READ_POSITION);
    memset(sgio_h->dxferp, random(), sizeof (positionData));

    /* we need this field to make the replay valid*/
    positionData.PERR = 0;
    /* fill with expected values */
    positionData.firstBlockLocation[0] = 0xAB;
    positionData.firstBlockLocation[1] = 0xCD;
    positionData.firstBlockLocation[2] = 0xEF;
    positionData.firstBlockLocation[3] = 0x12;
    positionData.lastBlockLocation[3] = 0xAB;
    positionData.lastBlockLocation[2] = 0xCD;
    positionData.lastBlockLocation[1] = 0xEF;
    positionData.lastBlockLocation[0] = 0x12;
    positionData.blocksInBuffer[0] = 0xAB;
    positionData.blocksInBuffer[1] = 0xCD;
    positionData.blocksInBuffer[2] = 0xEF;
    positionData.bytesInBuffer[3] = 0xAB;
    positionData.bytesInBuffer[2] = 0xCD;
    positionData.bytesInBuffer[1] = 0xEF;
    positionData.bytesInBuffer[0] = 0x12;
  } else { // we did seek on tape so we have real values
    /* we need this field to make the replay valid*/
    positionData.PERR = 0;
    /* fill with internal values 
     * lastBlockLocation=firstBlockLocation as soon as Buffer is empty.
     */
    uint32_t blkId = SCSI::Structures::fromLtoB32(blockID);
    memcpy(positionData.firstBlockLocation, &blkId, sizeof (positionData.firstBlockLocation));
    memcpy(positionData.lastBlockLocation, &blkId, sizeof (positionData.lastBlockLocation));
    SCSI::Structures::zeroStruct(positionData.blocksInBuffer);
    SCSI::Structures::zeroStruct(positionData.bytesInBuffer);
  }
  return 0;
}

int Tape::System::stDeviceFile::ioctlLogSelect(sg_io_hdr_t * sgio_h) {
  if (SG_DXFER_NONE != sgio_h->dxfer_direction) {
    errno = EINVAL;
    return -1;
  }
  /* we check CDB structure only and do not need to replay */
  SCSI::Structures::logSelectCDB_t & cdb =
          *(SCSI::Structures::logSelectCDB_t *) sgio_h->cmdp;
  if (1 != cdb.PCR || 0x3 != cdb.PC) {
    errno = EINVAL;
    return -1;
  }
  clearCompressionStats = true; /* set clear compression stats tag */
  return 0;
}

int Tape::System::stDeviceFile::ioctlLocate10(sg_io_hdr_t * sgio_h) {
  if (SG_DXFER_NONE != sgio_h->dxfer_direction) {
    errno = EINVAL;
    return -1;
  }
  /* perform logical seek on tape */
  SCSI::Structures::locate10CDB_t & cdb =
          *(SCSI::Structures::locate10CDB_t *) sgio_h->cmdp;
  blockID = SCSI::Structures::toU32(cdb.logicalObjectID);
  return 0;
}

int Tape::System::stDeviceFile::ioctlLogSense(sg_io_hdr_t * sgio_h) {
  if (SG_DXFER_FROM_DEV != sgio_h->dxfer_direction) {
    errno = EINVAL;
    return -1;
  }
  SCSI::Structures::logSenseCDB_t & cdb =
          *(SCSI::Structures::logSenseCDB_t *) sgio_h->cmdp;
  if (1 != cdb.PC || 0 == SCSI::Structures::toU16(cdb.allocationLength)) {
    errno = EINVAL;
    return -1;
  }
  switch (cdb.pageCode) {
    case SCSI::logSensePages::sequentialAccessDevicePage:
      return logSenseSequentialAccessDevicePage(sgio_h);
    case SCSI::logSensePages::dataCompression32h:
      return logSenseDataCompression32h(sgio_h);
    case SCSI::logSensePages::blockBytesTransferred:
      return logSenseBlockBytesTransferred(sgio_h);
    case SCSI::logSensePages::tapeAlert:
      return logSenseTapeAlerts(sgio_h);
  }
  errno = EINVAL;
  return -1;
}
int Tape::System::stDeviceFile::logSenseSequentialAccessDevicePage(sg_io_hdr_t * sgio_h) {
  /**
   * This is a real reply from the enterprise T10000C STK drive. 
   * We only fill the testing values in the corresponding fields for the 
   * compression statistics. The replay1 is the replay with not empty
   * data and the replay2 is the replay with zero statistics.
   */
  unsigned char replay1[] = {
    0x0c, 0x00, 0x00, 0x40, 0x00, 0x00, 0x74, 0x08, 0xab, 0xcd, 0xef, 0x11, 0x22, 0x33, 0x44, 0x55, // 0x00
    0x00, 0x01, 0x74, 0x08, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x00, 0x02, 0x74, 0x08, // 0x10
    0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x00, 0x03, 0x74, 0x08, 0x22, 0x33, 0x44, 0x55, // 0x20
    0x66, 0x77, 0x88, 0x99, 0x01, 0x00, 0x74, 0x04, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x74, 0x04, // 0x30 
    0x00, 0x00, 0x00, 0x00 // 0x40 
  };
  unsigned char replay2[] = {
    0x0c, 0x00, 0x00, 0x40, 0x00, 0x00, 0x74, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 0x00
    0x00, 0x01, 0x74, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x74, 0x08, // 0x10
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x74, 0x08, 0x00, 0x00, 0x00, 0x00, // 0x20
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x74, 0x04, 0x00, 0x00, 0x00, 0x00, 0x80, 0x00, 0x74, 0x04, // 0x30 
    0x00, 0x00, 0x00, 0x00 // 0x40 
  };

  if (sizeof (replay1) > sgio_h->dxfer_len) {
    errno = EINVAL;
    return -1;
  }
  if (clearCompressionStats) {
    memcpy(sgio_h->dxferp, replay2, sizeof (replay2));
  } else {
    memcpy(sgio_h->dxferp, replay1, sizeof (replay1));
  }
  return 0;
}

int Tape::System::stDeviceFile::logSenseDataCompression32h(sg_io_hdr_t * sgio_h) {
  /**
   * This is a real reply from the IBM ULTRIUM-TD5 LTO5 drive. 
   * We only fill the testing values in the corresponding fields for the 
   * compression statistics. The replay1 is the replay with not empty
   * data and the replay2 is the replay with zero statistics.
   */
  unsigned char replay1[] = {
    0x32, 0x00, 0x00, 0x4c, 0x00, 0x00, 0x40, 0x02, 0x00, 0x00, 0x00, 0x01, 0x40, 0x02, 0x00, 0x64, // 0x00
    0x00, 0x02, 0x40, 0x04, 0x11, 0x22, 0x33, 0x44, 0x00, 0x03, 0x40, 0x04, 0x55, 0x66, 0x77, 0x88, // 0x10
    0x00, 0x04, 0x40, 0x04, 0x99, 0xaa, 0xbb, 0xcc, 0x00, 0x05, 0x40, 0x04, 0xdd, 0xee, 0xff, 0x11, // 0x20
    0x00, 0x06, 0x40, 0x04, 0x22, 0x33, 0x44, 0x55, 0x00, 0x07, 0x40, 0x04, 0x66, 0x77, 0x88, 0x99, // 0x30
    0x00, 0x08, 0x40, 0x04, 0xaa, 0xbb, 0xcc, 0xdd, 0x00, 0x09, 0x40, 0x04, 0xee, 0xff, 0x11, 0x22  // 0x40
  };
  unsigned char replay2[] = {
    0x32, 0x00, 0x00, 0x4c, 0x00, 0x00, 0x40, 0x02, 0x00, 0x00, 0x00, 0x01, 0x40, 0x02, 0x00, 0x64, // 0x00
    0x00, 0x02, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00, // 0x10
    0x00, 0x04, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00, // 0x20
    0x00, 0x06, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00, // 0x30
    0x00, 0x08, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x40, 0x04, 0x00, 0x00, 0x00, 0x00  // 0x40
  };

  if (sizeof (replay1) > sgio_h->dxfer_len) {
    errno = EINVAL;
    return -1;
  }
  if (clearCompressionStats) {
    memcpy(sgio_h->dxferp, replay2, sizeof (replay2));
  } else {
    memcpy(sgio_h->dxferp, replay1, sizeof (replay1));
  }
  return 0;
}

int Tape::System::stDeviceFile::logSenseBlockBytesTransferred(sg_io_hdr_t * sgio_h) {
  /**
   * This is a real reply from the enterprise IBM 03592E06 drive. 
   * We only fill the testing values in the corresponding fields for the 
   * compression statistics. The replay1 is the replay with not empty
   * data and the replay2 is the replay with zero statistics.
   */
  unsigned char replay1[] = {
    0x38, 0x00, 0x00, 0x8a, 0x00, 0x00, 0x60, 0x04, 0x00, 0x00, 0x1f, 0xfe, 0x00, 0x01, 0x60, 0x04, // 0x00
    0x11, 0x22, 0x33, 0x44, 0x00, 0x02, 0x60, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x60, 0x04, // 0x10
    0x55, 0x66, 0x77, 0x88, 0x00, 0x04, 0x60, 0x04, 0x00, 0x00, 0x03, 0x21, 0x00, 0x05, 0x60, 0x04, // 0x20
    0x99, 0xaa, 0xbb, 0xcc, 0x00, 0x06, 0x60, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x60, 0x04, // 0x30
    0xdd, 0xee, 0xff, 0x11, 0x00, 0x08, 0x60, 0x04, 0x00, 0x00, 0x03, 0x21, 0x00, 0x09, 0x60, 0x04, // 0x40
    0x00, 0x13, 0x47, 0xeb, 0x00, 0x0a, 0x60, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x60, 0x04, // 0x50
    0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x60, 0x04, 0x3a, 0x35, 0x29, 0x44, 0x00, 0x0d, 0x60, 0x01, // 0x60
    0x00, 0x00, 0x0e, 0x60, 0x04, 0x3a, 0x35, 0x29, 0x44, 0x00, 0x0f, 0x60, 0x01, 0x00, 0x00, 0x10, // 0x70
    0x60, 0x04, 0x3a, 0x21, 0x9b, 0x54, 0x00, 0x11, 0x60, 0x04, 0x3a, 0x21, 0x9b, 0x54              // 0x80
  };
  unsigned char replay2[] = {
    0x38, 0x00, 0x00, 0x8a, 0x00, 0x00, 0x60, 0x04, 0x00, 0x00, 0x1f, 0xfe, 0x00, 0x01, 0x60, 0x04, // 0x00
    0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x60, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x60, 0x04, // 0x10
    0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x60, 0x04, 0x00, 0x00, 0x03, 0x21, 0x00, 0x05, 0x60, 0x04, // 0x20
    0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x60, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x60, 0x04, // 0x30
    0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x60, 0x04, 0x00, 0x00, 0x03, 0x21, 0x00, 0x09, 0x60, 0x04, // 0x40
    0x00, 0x13, 0x47, 0xeb, 0x00, 0x0a, 0x60, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x60, 0x04, // 0x50
    0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x60, 0x04, 0x3a, 0x35, 0x29, 0x44, 0x00, 0x0d, 0x60, 0x01, // 0x60
    0x00, 0x00, 0x0e, 0x60, 0x04, 0x3a, 0x35, 0x29, 0x44, 0x00, 0x0f, 0x60, 0x01, 0x00, 0x00, 0x10, // 0x70
    0x60, 0x04, 0x3a, 0x21, 0x9b, 0x54, 0x00, 0x11, 0x60, 0x04, 0x3a, 0x21, 0x9b, 0x54              // 0x80
  };
  if (sizeof (replay1) > sgio_h->dxfer_len) {
    errno = EINVAL;
    return -1;
  }
  if (clearCompressionStats) {
    memcpy(sgio_h->dxferp, replay2, sizeof (replay2));
  } else {
    memcpy(sgio_h->dxferp, replay1, sizeof (replay1));
  }
  return 0;
}

int Tape::System::stDeviceFile::logSenseTapeAlerts(sg_io_hdr_t* sgio_h) {
  size_t remaining = sgio_h->dxfer_len;
  /* Truncation of any field should yield an error */
  if (remaining < (4 + 320)) {
    errno = EINVAL;
    return -1;
  }
  /* Header as-is from mhvtl. */
  unsigned char * data = (unsigned char *) sgio_h->dxferp;
  data[0] = 0x2eU;
  /* 145h bytes, with parameters of 5 bytes means 65 parameters */
  data[2] = 0x1U;
  data[3] = 0x45U;
  data += 4; remaining -= 4;
  /* This array was extracted from p/x in gdb of the tape alert log page from
   * mhvtl, then processed through:
   * cat mhvtlAlerts.txt  | tr -d "\n" | perl -p -e 's/\},/}\n/g' |  grep parameterCode | 
   * perl -e 'while (<>) { if ( /\{\s*(0x[[:xdigit:]]+),\s*(0x[[:xdigit:]]+)\}/ ) { print (hex($1) * 256 + hex($2)); print ", " } }'*/
  /* We also add an out-of-range 65 */
  uint16_t parameterCodes[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 
  15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 
  34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 
  53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65 };
  size_t i = 0;
  while (remaining > 5 && i < 65) {
    /* small gymnastic to turn a bare buffer into a SCSI u16 storage */
    struct u16wrap { unsigned char u16[2]; };
    struct u16wrap * s((struct u16wrap *) &(data[0]));
    SCSI::Structures::setU16(s->u16, parameterCodes[i]);
    data[2] = 0U;
    data[3] = 1U;
    data[4] = 0U; /* TODO: at least some parameters should get set */
    switch (parameterCodes[i]) {
      case 0x28:
      case 0x10:
      case 65:
        data[4] |= 1; /* Set flag */
    }
    i++; data += 5; remaining -=5;
  }
  return 0;
}

int Tape::System::stDeviceFile::ioctlModSense6(sg_io_hdr_t * sgio_h) {
  if (SG_DXFER_FROM_DEV != sgio_h->dxfer_direction) {
    errno = EINVAL;
    return -1;
  }
  SCSI::Structures::modeSense6CDB_t & cdb =
          *(SCSI::Structures::modeSense6CDB_t *) sgio_h->cmdp;
  if (SCSI::modeSensePages::deviceConfiguration != cdb.pageCode) {
    errno = EINVAL;
    return -1;
  }
  SCSI::Structures::modeSenseDeviceConfiguration_t & devConfig =
          *(SCSI::Structures::modeSenseDeviceConfiguration_t *) sgio_h->dxferp;

  if (sizeof (devConfig) > sgio_h->dxfer_len) {
    errno = EINVAL;
    return -1;
  }
  /* fill the replay with random data */
  srandom(SCSI::Commands::MODE_SENSE_6);
  memset(sgio_h->dxferp, random(), sizeof (devConfig));
  return 0;
}

int Tape::System::stDeviceFile::ioctlModSelect6(sg_io_hdr_t * sgio_h) {
  if (SG_DXFER_TO_DEV != sgio_h->dxfer_direction) {
    errno = EINVAL;
    return -1;
  }
  SCSI::Structures::modeSelect6CDB_t & cdb =
          *(SCSI::Structures::modeSelect6CDB_t *) sgio_h->cmdp;
  SCSI::Structures::modeSenseDeviceConfiguration_t & devConfig =
          *(SCSI::Structures::modeSenseDeviceConfiguration_t *) sgio_h->dxferp;
  if (sizeof (devConfig) > sgio_h->dxfer_len) {
    errno = EINVAL;
    return -1;
  }

  if (1 != cdb.PF || sizeof (devConfig) != cdb.paramListLength ||
          0 != devConfig.header.modeDataLength) {
    errno = EINVAL;
    return -1;
  }

  if (1 != devConfig.modePage.selectDataComprAlgorithm &&
          0 != devConfig.modePage.selectDataComprAlgorithm) {
    errno = EINVAL;
    return -1;
  }
  return 0;
}

int Tape::System::stDeviceFile::ioctlInquiry(sg_io_hdr_t * sgio_h) {
 if (SG_DXFER_FROM_DEV != sgio_h->dxfer_direction) {
    errno = EINVAL;
    return -1;
  }
  SCSI::Structures::inquiryCDB_t & cdb =
          *(SCSI::Structures::inquiryCDB_t *) sgio_h->cmdp;

  if (0 == cdb.EVPD && 0 == cdb.pageCode) {
    /* the Standard Inquiry Data is returned*/
    SCSI::Structures::inquiryData_t & inqData =
            *(SCSI::Structures::inquiryData_t *) sgio_h->dxferp;
    if (sizeof (inqData) > sgio_h->dxfer_len) {
      errno = EINVAL;
      return -1;
    }
    /* fill the replay with random data */
    srandom(SCSI::Commands::INQUIRY);
    memset(sgio_h->dxferp, random(), sizeof (inqData));
    /* We fill only fields we need.
     * The emptiness in the strings fields we fill with spaces.
     * And we do not need '\0' in the end of strings. For the tests
     * there are mhvtl data.
     */
    const char *prodId = "T10000B                       ";
    memcpy(inqData.prodId, prodId, sizeof (inqData.prodId));
    const char *prodRevLvl = "0104                      ";
    memcpy(inqData.prodRevLvl, prodRevLvl, sizeof (inqData.prodRevLvl));
    const char *T10Vendor = "STK                        ";
    memcpy(inqData.T10Vendor, T10Vendor, sizeof (inqData.T10Vendor));
  } else if (1 == cdb.EVPD && SCSI::inquiryVPDPages::unitSerialNumber == cdb.pageCode) {
    /* the unit serial number VPD page is returned*/
    SCSI::Structures::inquiryUnitSerialNumberData_t & inqSerialData =
            *(SCSI::Structures::inquiryUnitSerialNumberData_t *) sgio_h->dxferp;
    if (sizeof (inqSerialData) > sgio_h->dxfer_len) {
      errno = EINVAL;
      return -1;
    }
    /* fill the replay with random data */
    srandom(SCSI::Commands::INQUIRY);
    memset(sgio_h->dxferp, random(), sgio_h->dxfer_len);
    const char serialNumber[11] = "XYZZY_A2  ";
    memcpy(inqSerialData.productSerialNumber, serialNumber, 10);
    inqSerialData.pageLength = 10;
  } else {
    errno = EINVAL;
    return -1;
  }
  return 0;
}
