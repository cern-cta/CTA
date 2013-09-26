// ----------------------------------------------------------------------
// File: Drive/Drive.cc
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

#include "Drive.hh"

Tape::DriveGeneric::DriveGeneric(SCSI::DeviceInfo di, System::virtualWrapper& sw) : m_SCSIInfo(di),
m_tapeFD(-1),  m_sysWrapper(sw) {
  /* Open the device files */
  /* We open the tape device file non-blocking as blocking open on rewind tapes (at least)
   * will fail after a long timeout when no tape is present (at least with mhvtl) 
   */
  m_tapeFD = m_sysWrapper.open(m_SCSIInfo.nst_dev.c_str(), O_RDWR | O_NONBLOCK);
  if (-1 == m_tapeFD)
    throw Tape::Exceptions::Errnum(std::string("Could not open device file: " + m_SCSIInfo.nst_dev));
  /* Read drive status */
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &m_mtInfo))
    throw Tape::Exceptions::Errnum(std::string("Could not read drive status: " + m_SCSIInfo.nst_dev));
}

/**
 * Reset all statistics about data movements on the drive.
 * All comulative and threshold log counter values will be reset to their
 * default values as specified in that pages reset behavior section.
 */
void Tape::DriveGeneric::clearCompressionStats() throw (Tape::Exception) {
  SCSI::Structures::logSelectCDB_t cdb;
  cdb.PCR = 1; /* PCR set */
  cdb.PC = 0x3; /* PC = 11b  for T10000 only*/

  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  sgh.setCDB(&cdb);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_NONE;

  /* Manage both system error and SCSI errors. */
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, "SCSI error in clearCompressionStats:");
}

/**
 * Information about the drive. The vendor id is used in the user labels of the files.
 * @return    The deviceInfo structure with the information about the drive.
 */
Tape::deviceInfo Tape::DriveGeneric::getDeviceInfo() throw (Tape::Exception) {
  SCSI::Structures::inquiryCDB_t cdb;
  SCSI::Structures::inquiryData_t inquiryData;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;
  deviceInfo devInfo;

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&inquiryData);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getDeviceInfo: ") +
      SCSI::statusToString(sgh.status));

  devInfo.product = SCSI::Structures::toString(inquiryData.prodId);
  devInfo.productRevisionLevel = SCSI::Structures::toString(inquiryData.prodRevLvl);
  devInfo.vendor = SCSI::Structures::toString(inquiryData.T10Vendor);
  devInfo.serialNumber = getSerialNumber();

  return devInfo;
}

/**
 * Information about the serial number of the drive. 
 * @return   Right-aligned ASCII data for the vendor-assigned serial number.
 */
std::string Tape::DriveGeneric::getSerialNumber() throw (Tape::Exception) {
  SCSI::Structures::inquiryCDB_t cdb;
  SCSI::Structures::inquiryUnitSerialNumberData_t inquirySerialData;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  cdb.EVPD = 1; /* Enable Vital Product Data */
  cdb.pageCode = SCSI::inquiryVPDPages::unitSerialNumber;
  cdb.allocationLength[0] = 0;
  cdb.allocationLength[1] = sizeof (SCSI::Structures::inquiryUnitSerialNumberData_t);

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&inquirySerialData);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getSerialNumber: ") +
      SCSI::statusToString(sgh.status));
  std::string serialNumber;
  serialNumber.append(inquirySerialData.productSerialNumber, inquirySerialData.pageLength);

  return serialNumber;
}

/**
 * Position to logical object identifier (i.e. block address). 
 * This function is blocking: the immediate bit is not set.
 * The device server will not return status until the locate operation
 * has completed.
 * @param blockId The blockId, represented in local endianness.
 */
void Tape::DriveGeneric::positionToLogicalObject(uint32_t blockId)
throw (Tape::Exception) {
  SCSI::Structures::locate10CDB_t cdb;
  uint32_t blkId = SCSI::Structures::fromLtoB32(blockId);

  memcpy(cdb.logicalObjectID, &blkId, sizeof (cdb.logicalObjectID));

  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  sgh.setCDB(&cdb);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_NONE;
  sgh.timeout = 180000; // TODO castor.conf LOCATE_TIMEOUT or default

  /* Manage both system error and SCSI errors. */
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, "SCSI error in positionToLogicalObject:");
}

/**
 * Return logical position of the drive. This is the address of the next object
 * to read or write.
 * @return positionInfo class. This contains the logical position, plus information
 * on the dirty data still in the write buffer.
 */
Tape::positionInfo Tape::DriveGeneric::getPositionInfo()
throw (Tape::Exception) {
  SCSI::Structures::readPositionCDB_t cdb;
  SCSI::Structures::readPositionDataShortForm_t positionData;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  positionInfo posInfo;

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&positionData);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getPositionInfo: ") +
      SCSI::statusToString(sgh.status));

  if (0 == positionData.PERR) { // Location fields are valid
    posInfo.currentPosition = SCSI::Structures::toU32(positionData.firstBlockLocation);
    posInfo.oldestDirtyObject = SCSI::Structures::toU32(positionData.lastBlockLocation);
    posInfo.dirtyObjectsCount = SCSI::Structures::toU32(positionData.blocksInBuffer);
    posInfo.dirtyBytesCount = SCSI::Structures::toU32(positionData.bytesInBuffer);
  } else {
    /* An overflow has occurred in at least one of the returned position
     * data fields. The application should use the LONG FORM to obtain the 
     * current position or the application should use the EXTENDED FORM to
     * obtain the current position and number of bytes in the object buffer.
     * (note) For T10000 we have only SHORT FORM.
     */
    throw Tape::Exception(std::string("An overflow has occurred in getPostitionInfo"));
  }
  return posInfo;
}

/**
 * Get tape alert information from the drive. There is a quite long list of possible tape alerts.
 * They are described in SSC-4, section 4.2.20: TapeAlert application client interface.
 * Section is 4.2.17 in SSC-3.
 * @return list of tape alerts descriptions. They are simply used for logging.
 */
std::vector<std::string> Tape::DriveGeneric::getTapeAlerts() throw (Tape::Exception) {
  /* return vector */
  std::vector<std::string> ret;
  /* We don't know how many elements we'll get. Prepare a 100 parameters array */
  SCSI::Structures::tapeAlertLogPage_t<100> tal;
  /* Prepare a sense buffer of 255 bytes */
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::logSenseCDB_t cdb;
  cdb.pageCode = SCSI::logSensePages::tapeAlert;
  cdb.PC = 0x01; // Current Comulative Values
  SCSI::Structures::LinuxSGIO_t sgh;
  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&tal);
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(tal));
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;
  /* Manage both system error and SCSI errors. */
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getTapeAlerts: ") +
      SCSI::statusToString(sgh.status));
  /* Return the ACTIVE tape alerts (this is indicated by "flag" (see 
   * SSC-4: 8.2.3 TapeAlert log page). As they are simply used for logging;
   * return strings. */
  for (size_t i = 0; i < tal.parameterNumber(); i++) {
    if (tal.parameters[i].flag)
      ret.push_back(SCSI::tapeAlertToString(
        SCSI::Structures::toU16(tal.parameters[i].parameterCode)
        ));
  }
  return ret;
}

/**
 * Set the tape density and compression. 
 * We use MODE SENSE/SELECT Device Configuration (10h) mode page.
 * As soon as there is no definition in SPC-4 or SSC-3 it depends on the 
 * drives documentation. 
 * 
 * @param densityCode  The tape specific density code.
 *                     If it is 0 (default) than we use the density code 
 *                     detected by the drive itself means no changes.
 *                                
 * @param compression  The boolean variable to enable or disable compression
 *                     on the drive for the tape. By default it is enabled.
 */
void Tape::DriveGeneric::setDensityAndCompression(unsigned char densityCode,
    bool compression) throw (Tape::Exception) {
  SCSI::Structures::modeSenseDeviceConfiguration_t devConfig;
  { // get info from the drive
    SCSI::Structures::modeSense6CDB_t cdb;
    SCSI::Structures::senseData_t<255> senseBuff;
    SCSI::Structures::LinuxSGIO_t sgh;

    cdb.pageCode = SCSI::modeSensePages::deviceConfiguration;
    cdb.allocationLenght = sizeof (devConfig);

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&devConfig);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
      throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
    SCSI::ExceptionLauncher(sgh,
        std::string("SCSI error in setDensityAndCompression: ") +
        SCSI::statusToString(sgh.status));
  }

  { // set parameters and we use filled structure devConfig from the previous SCSI call
    SCSI::Structures::modeSelect6CDB_t cdb;
    SCSI::Structures::senseData_t<255> senseBuff;
    SCSI::Structures::LinuxSGIO_t sgh;

    cdb.PF = 1; // means nothing for IBM, LTO, T10000
    cdb.paramListLength = sizeof (devConfig);

    devConfig.header.modeDataLength = 0; // must be 0 for IBM, LTO ignored by T10000
    if (0 != densityCode) devConfig.blockDescriptor.densityCode = densityCode;
    if (compression) devConfig.modePage.selectDataComprAlgorithm = 1;
    else devConfig.modePage.selectDataComprAlgorithm = 0;

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&devConfig);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_TO_DEV;

    /* Manage both system error and SCSI errors. */
    if (-1 == m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh))
      throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
    SCSI::ExceptionLauncher(sgh,
        std::string("SCSI error in setDensityAndCompression: ") +
        SCSI::statusToString(sgh.status));
  }
}

/**
 * Set the buffer write switch in the st driver. This is directly matching a configuration
 * parameter in CASTOR, so this function has to be public and usable by a higher level
 * layer, unless the parameter turns out to be disused.
 * @param bufWrite: value of the buffer write switch
 */
void Tape::DriveGeneric::setSTBufferWrite(bool bufWrite) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTSETDRVBUFFER;
  m_mtCmd.mt_count = bufWrite ? (MT_ST_SETBOOLEANS | MT_ST_BUFFER_WRITES) : (MT_ST_CLEARBOOLEANS | MT_ST_BUFFER_WRITES);
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTSETDRVBUFFER)");
}

/**
 * Jump to end of recorded media. This will use setSTFastMTEOM() to disable MT_ST_FAST_MTEOM.
 * (See TapeServer's handbook for details). This is used to rebuild the MIR (StorageTek)
 * or tape directory (IBM).
 * Tape directory rebuild is described only for IBM but currently applied to 
 * all tape drives.
 * TODO: synchronous? Timeout?
 */    
void Tape::DriveGeneric::spaceToEOM(void) throw (Tape::Exception) {
  setSTFastMTEOM(false);
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTEOM;
  m_mtCmd.mt_count = 1;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTEOM)");
}

/**
 * Set the MTFastEOM option of the ST driver. This function is used only internally in 
 * mounttape (in CAStor), so it could be a private function, not visible to 
 * the higher levels of the software (TODO: protected?).
 * @param fastMTEOM the option switch.
 */
void Tape::DriveGeneric::setSTFastMTEOM(bool fastMTEOM) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTSETDRVBUFFER;
  m_mtCmd.mt_count = fastMTEOM ? (MT_ST_SETBOOLEANS | MT_ST_FAST_MTEOM) : (MT_ST_CLEARBOOLEANS | MT_ST_FAST_MTEOM);
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTSETDRVBUFFER)");
}

/**
 * Jump to end of data. EOM in ST driver jargon, end of data (which is more accurate)
 * in SCSI terminology). This uses the fast setting (not to be used for MIR rebuild) 
 */
void Tape::DriveGeneric::fastSpaceToEOM(void) throw (Tape::Exception) {
  setSTFastMTEOM(true);
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTEOM;
  m_mtCmd.mt_count = 1;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTEOM)");
}

/**
 * Rewind tape.
 */
void Tape::DriveGeneric::rewind(void) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTREW;
  m_mtCmd.mt_count = 1;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTREW)");
}

/**
 * Space count file marks backwards.
 * @param count
 */
void Tape::DriveGeneric::spaceFileMarksBackwards(size_t count) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTBSF;
  m_mtCmd.mt_count = (int)count;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTBSF)");
}

/**
 * Space count file marks forward.
 * @param count
 */
void Tape::DriveGeneric::spaceFileMarksForward(size_t count) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTFSF;
  m_mtCmd.mt_count = (int)count;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTFSF)");
}

/**
 * Space count logical blocks backwards.
 * A logical block is the smallest user data unit accessible on tape and its 
 * length is defined at write time. This function will throw an exception if the 
 * next logical object is not a logical block (i.e. if it is a file mark instead).
 * @param count
 */
void Tape::DriveGeneric::spaceBlocksBackwards(size_t count) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTBSR;
  m_mtCmd.mt_count = (int)count;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTBSR)");
}

/**
 * Space count logical blocks forward.
 * A logical block is the smallest user data unit accessible on tape and its 
 * length is defined at write time. This function will throw an exception if the 
 * next logical object is not a logical block (i.e. if it is a file mark instead).
 * @param count
 */
void Tape::DriveGeneric::spaceBlocksForward(size_t count) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTFSR;
  m_mtCmd.mt_count = (int)count;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTFSR)");
}

/**
 * Unload the tape.
 */
void Tape::DriveGeneric::unloadTape(void) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTUNLOAD;
  m_mtCmd.mt_count = 1;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTUNLOAD)");
}

/**
 * Synch call to the tape drive. This function will not return before the 
 * data in the drive's buffer is actually comitted to the medium.
 */
void Tape::DriveGeneric::sync(void) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTNOP; //The side effect of the no-op is to actually flush the driver's buffer to tape (see "man st").
  m_mtCmd.mt_count = 1;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTNOP)");
}

/**
 * Write count file marks. The function does not return before the file marks 
 * are committed to medium.
 * @param count
 */
void Tape::DriveGeneric::writeSyncFileMarks(size_t count) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTWEOF;
  m_mtCmd.mt_count = (int)count;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTWEOF)");
}

/**
 * Write count file marks asynchronously. The file marks are just added to the drive's
 * buffer and the function return immediately.
 * @param count
 */
void Tape::DriveGeneric::writeImmediateFileMarks(size_t count) throw (Tape::Exception) {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTWEOFI; //Undocumented in "man st" needs the mtio_add.hh header file (see above)
  m_mtCmd.mt_count = (int)count;
  if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd))
    throw Tape::Exceptions::Errnum("Failed ST ioctl (MTWEOFI)");
}

/**
 * Write a data block to tape.
 * @param data pointer the the data block
 * @param count size of the data block
 */
void Tape::DriveGeneric::writeBlock(const unsigned char * data, size_t count) throw (Tape::Exception) {
  if (-1 == m_sysWrapper.write(m_tapeFD, data, count))
    throw Tape::Exceptions::Errnum("Failed ST write");
}

/**
 * Read a data block from tape.
 * @param data pointer the the data block
 * @param count size of the data block
 */
void Tape::DriveGeneric::readBlock(unsigned char * data, size_t count) throw (Tape::Exception) {
  if (-1 == m_sysWrapper.read(m_tapeFD, data, count))
    throw Tape::Exceptions::Errnum("Failed ST read");
}

void Tape::DriveGeneric::SCSI_inquiry() {
  std::cout << "Re-doing a SCSI inquiry via st device:" << std::endl;
  SCSI_inquiry(m_tapeFD);
}

void Tape::DriveGeneric::SCSI_inquiry(int fd) {
  unsigned char dataBuff[130];
  unsigned char senseBuff[256];
  SCSI::Structures::inquiryCDB_t cdb;
  memset(&dataBuff, 0, sizeof (dataBuff));
  /* Build command: nothing to do. We go with defaults. */

  sg_io_hdr_t sgh;
  memset(&sgh, 0, sizeof (sgh));
  sgh.interface_id = 'S';
  sgh.cmdp = (unsigned char *) &cdb;
  sgh.cmd_len = sizeof (cdb);
  sgh.sbp = senseBuff;
  sgh.mx_sb_len = 255;
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;
  sgh.dxferp = dataBuff;
  sgh.dxfer_len = sizeof (dataBuff);
  sgh.timeout = 30000;
  if (-1 == m_sysWrapper.ioctl(fd, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  std::cout << "INQUIRY result: " << std::endl
      << "sgh.dxfer_len=" << sgh.dxfer_len
      << " sgh.sb_len_wr=" << ((int) sgh.sb_len_wr)
      << " sgh.status=" << ((int) sgh.status)
      << " sgh.info=" << ((int) sgh.info)
      << std::endl;
  std::cout << SCSI::Structures::hexDump(dataBuff)
      << SCSI::Structures::toString(*((SCSI::Structures::inquiryData_t *) dataBuff));
}

Tape::compressionStats Tape::DriveT10000::getCompression() throw (Tape::Exception) {
  SCSI::Structures::logSenseCDB_t cdb;
  compressionStats driveCompressionStats;
  unsigned char dataBuff[1024];
  unsigned char senseBuff[255];

  memset(dataBuff, 0, sizeof (dataBuff));
  memset(senseBuff, 0, sizeof (senseBuff));

  cdb.pageCode = SCSI::logSensePages::sequentialAccessDevicePage;
  cdb.PC = 0x01; // Current Comulative Values
  cdb.allocationLength[0] = (sizeof (dataBuff) & 0xFF00) >> 8;
  cdb.allocationLength[1] = sizeof (dataBuff) & 0x00FF;

  SCSI::Structures::LinuxSGIO_t sgh;
  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  if (-1 == this->m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getCompression: ") +
      SCSI::statusToString(sgh.status));

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  unsigned char *endPage = dataBuff +
      SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof (logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof (logPageHeader);

  while (logParameter < endPage) { /* values in bytes  */
    SCSI::Structures::logSenseParameter_t & logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::sequentialAccessDevicePage::receivedFromInitiator:
        driveCompressionStats.fromHost = SCSI::Structures::toU64(reinterpret_cast<unsigned char(&)[8]> (logPageParam.parameterValue));
        break;
      case SCSI::sequentialAccessDevicePage::writtenOnTape:
        driveCompressionStats.toTape = SCSI::Structures::toU64(reinterpret_cast<unsigned char(&)[8]> (logPageParam.parameterValue));
        break;
      case SCSI::sequentialAccessDevicePage::readFromTape:
        driveCompressionStats.fromTape = SCSI::Structures::toU64(reinterpret_cast<unsigned char(&)[8]> (logPageParam.parameterValue));
        break;
      case SCSI::sequentialAccessDevicePage::readByInitiator:
        driveCompressionStats.toHost = SCSI::Structures::toU64(reinterpret_cast<unsigned char(&)[8]> (logPageParam.parameterValue));
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof (logPageParam.header);
  }

  return driveCompressionStats;
}

Tape::compressionStats Tape::DriveLTO::getCompression() throw (Tape::Exception) {
  SCSI::Structures::logSenseCDB_t cdb;
  compressionStats driveCompressionStats;
  unsigned char dataBuff[1024];
  unsigned char senseBuff[255];

  memset(dataBuff, 0, sizeof (dataBuff));
  memset(senseBuff, 0, sizeof (senseBuff));

  cdb.pageCode = SCSI::logSensePages::dataCompression32h;
  cdb.PC = 0x01; // Current Comulative Values
  cdb.allocationLength[0] = (sizeof (dataBuff) & 0xFF00) >> 8;
  cdb.allocationLength[1] = sizeof (dataBuff) & 0x00FF;

  SCSI::Structures::LinuxSGIO_t sgh;
  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  if (-1 == this->m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getCompression: ") +
      SCSI::statusToString(sgh.status));

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  unsigned char *endPage = dataBuff +
      SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof (logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof (logPageHeader);
  const uint64_t mb = 1000000; // Mega bytes as power of 10

  while (logParameter < endPage) { /* values in MB and Bytes */
    SCSI::Structures::logSenseParameter_t & logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;

    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
        // fromHost
      case SCSI::dataCompression32h::mbTransferredFromServer:
        driveCompressionStats.fromHost = SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue)) * mb;
        break;
      case SCSI::dataCompression32h::bytesTransferredFromServer:
        driveCompressionStats.fromHost += SCSI::Structures::toS32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue));
        break;
        // toTape  
      case SCSI::dataCompression32h::mbWrittenToTape:
        driveCompressionStats.toTape = SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue)) * mb;
        break;
      case SCSI::dataCompression32h::bytesWrittenToTape:
        driveCompressionStats.toTape += SCSI::Structures::toS32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue));
        break;
        // fromTape     
      case SCSI::dataCompression32h::mbReadFromTape:
        driveCompressionStats.fromTape = SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue)) * mb;
        break;
      case SCSI::dataCompression32h::bytesReadFromTape:
        driveCompressionStats.fromTape += SCSI::Structures::toS32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue));
        break;
        // toHost            
      case SCSI::dataCompression32h::mbTransferredToServer:
        driveCompressionStats.toHost = SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue)) * mb;
        break;
      case SCSI::dataCompression32h::bytesTransferredToServer:
        driveCompressionStats.toHost += SCSI::Structures::toS32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue));
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof (logPageParam.header);
  }

  return driveCompressionStats;
}

Tape::compressionStats Tape::DriveIBM3592::getCompression() throw (Tape::Exception) {
  SCSI::Structures::logSenseCDB_t cdb;
  compressionStats driveCompressionStats;
  unsigned char dataBuff[1024];
  unsigned char senseBuff[255];

  memset(dataBuff, 0, sizeof (dataBuff));
  memset(senseBuff, 0, sizeof (senseBuff));

  cdb.pageCode = SCSI::logSensePages::blockBytesTransferred;
  cdb.PC = 0x01; // Current Comulative Values
  cdb.allocationLength[0] = (sizeof (dataBuff) & 0xFF00) >> 8;
  cdb.allocationLength[1] = sizeof (dataBuff) & 0x00FF;

  SCSI::Structures::LinuxSGIO_t sgh;
  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  if (-1 == this->m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh))
    throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
  SCSI::ExceptionLauncher(sgh, std::string("SCSI error in getCompression: ") +
      SCSI::statusToString(sgh.status));

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  unsigned char *endPage = dataBuff +
      SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof (logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof (logPageHeader);

  while (logParameter < endPage) { /* values in KiBs and we use shift <<10 to get bytes */
    SCSI::Structures::logSenseParameter_t & logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::blockBytesTransferred::hostWriteKiBProcessed:
        driveCompressionStats.fromHost = (uint64_t) SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue)) << 10;
        break;
      case SCSI::blockBytesTransferred::deviceWriteKiBProcessed:
        driveCompressionStats.toTape = (uint64_t) SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue)) << 10;
        break;
      case SCSI::blockBytesTransferred::deviceReadKiBProcessed:
        driveCompressionStats.fromTape = (uint64_t) SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue)) << 10;
        break;
      case SCSI::blockBytesTransferred::hostReadKiBProcessed:
        driveCompressionStats.toHost = (uint64_t) SCSI::Structures::toU32(reinterpret_cast<unsigned char(&)[4]> (logPageParam.parameterValue)) << 10;
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof (logPageParam.header);
  }

  return driveCompressionStats;
}
