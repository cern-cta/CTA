/******************************************************************************
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

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "castor/tape/tapeserver/drive/DriveGeneric.hpp"

#include "castor/utils/Timer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {

drive::DriveInterface * drive::createDrive(SCSI::DeviceInfo di, 
    System::virtualWrapper& sw) {
  if (std::string::npos != di.product.find("T10000")) {
    return new DriveT10000(di, sw);
  } else if (std::string::npos != di.product.find("ULT") || std::string::npos != di.product.find("Ultrium")) {
    return new DriveLTO(di, sw);
  } else if (std::string::npos != di.product.find("03592")) {
    return new DriveIBM3592(di, sw);
  } else if (std::string::npos != di.product.find("VIRTUAL")) {
    /* In case of a VIRTUAL drive, it could have been pre-allocated 
     * for testing purposes (with "pre-cooked" contents). */
    drive::DriveInterface * ret = sw.getDriveByPath(di.nst_dev);
    if (ret) {
      return ret;
    } else {
      return new FakeDrive();
    }
  } else {
    throw castor::exception::Exception(std::string("Unsupported drive type: ") + di.product);
  }
}

drive::DriveGeneric::DriveGeneric(SCSI::DeviceInfo di, System::virtualWrapper& sw) : m_SCSIInfo(di),
m_tapeFD(-1),  m_sysWrapper(sw) {
  /* Open the device files */
  /* We open the tape device file non-blocking as blocking open on rewind tapes (at least)
   * will fail after a long timeout when no tape is present (at least with mhvtl) 
   */
  castor::exception::Errnum::throwOnMinusOne(
      m_tapeFD = m_sysWrapper.open(m_SCSIInfo.nst_dev.c_str(), O_RDWR | O_NONBLOCK),
      std::string("Could not open device file: ") + m_SCSIInfo.nst_dev);
}

/**
 * Reset all statistics about data movements on the drive.
 * All comulative and threshold log counter values will be reset to their
 * default values as specified in that pages reset behavior section.
 */
void drive::DriveGeneric::clearCompressionStats()  {
  SCSI::Structures::logSelectCDB_t cdb;
  cdb.PCR = 1; /* PCR set */
  cdb.PC = 0x3; /* PC = 11b  for T10000 only*/

  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  sgh.setCDB(&cdb);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_NONE;

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::clearCompressionStats");
  SCSI::ExceptionLauncher(sgh, "SCSI error in clearCompressionStats:");
}

/**
 * Information about the drive. The vendor id is used in the user labels of the files.
 * @return    The deviceInfo structure with the information about the drive.
 */
drive::deviceInfo drive::DriveGeneric::getDeviceInfo()  {
  SCSI::Structures::inquiryCDB_t cdb;
  SCSI::Structures::inquiryData_t inquiryData;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;
  deviceInfo devInfo;

  SCSI::Structures::setU16(cdb.allocationLength, sizeof(inquiryData));
  
  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&inquiryData);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::getDeviceInfo");
  SCSI::ExceptionLauncher(sgh, "SCSI error in getDeviceInfo:");

  devInfo.product = SCSI::Structures::toString(inquiryData.prodId);
  devInfo.productRevisionLevel = SCSI::Structures::toString(inquiryData.prodRevLvl);
  devInfo.vendor = SCSI::Structures::toString(inquiryData.T10Vendor);
  devInfo.serialNumber = getSerialNumber();
  devInfo.isPIsupported = inquiryData.protect;
  return devInfo;
}

/**
 * Information about the serial number of the drive. 
 * @return   Right-aligned ASCII data for the vendor-assigned serial number.
 */
std::string drive::DriveGeneric::getSerialNumber()  {
  SCSI::Structures::inquiryCDB_t cdb;
  SCSI::Structures::inquiryUnitSerialNumberData_t inquirySerialData;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  cdb.EVPD = 1; /* Enable Vital Product Data */
  cdb.pageCode = SCSI::inquiryVPDPages::unitSerialNumber;
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(inquirySerialData));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&inquirySerialData);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::getSerialNumber");
  SCSI::ExceptionLauncher(sgh, "SCSI error in getSerialNumber:");
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
void drive::DriveGeneric::positionToLogicalObject(uint32_t blockId)
 {
  SCSI::Structures::locate10CDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh; 
  
  SCSI::Structures::setU32(cdb.logicalObjectID, blockId);

  sgh.setCDB(&cdb);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_NONE;
  //sgh.timeout = defaultTimeout; // set globally by SCSI::Structures.hpp (defaultTimeout)

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::positionToLogicalObject");
  SCSI::ExceptionLauncher(sgh, "SCSI error in positionToLogicalObject:");
}

/**
 * Return logical position of the drive. This is the address of the next object
 * to read or write.
 * @return positionInfo class. This contains the logical position, plus information
 * on the dirty data still in the write buffer.
 */
drive::positionInfo drive::DriveGeneric::getPositionInfo()
 {
  SCSI::Structures::readPositionCDB_t cdb;
  SCSI::Structures::readPositionDataShortForm_t positionData;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  positionInfo posInfo;
  
  // We just go all defaults: service action = 00 (SHORT FORM BLOCK ID)
  // The result will come in fixed size and allocation length must be 0.
  // At least IBM drives will complain otherwise
  
  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&positionData);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::getPositionInfo");
  SCSI::ExceptionLauncher(sgh, "SCSI error in getPositionInfo:");

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
    throw castor::exception::Exception(std::string("An overflow has occurred in getPostitionInfo"));
  }
  return posInfo;
}

/**
 * Get tape alert information from the drive. There is a quite long list of possible tape alerts.
 * They are described in SSC-4, section 4.2.20: TapeAlert application client interface.
 * Section is 4.2.17 in SSC-3. This version gives a list of numerical codes.
 * @return list of tape alerts codes.
 */
std::vector<uint16_t> drive::DriveGeneric::getTapeAlertCodes(){
  /* return vector */
  std::vector<uint16_t> ret;
  /* We don't know how many elements we'll get. Prepare a 100 parameters array */
  SCSI::Structures::tapeAlertLogPage_t<100> tal;
  /* Prepare a sense buffer of 255 bytes */
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::LinuxSGIO_t sgh;
  
  cdb.pageCode = SCSI::logSensePages::tapeAlert;
  cdb.PC = 0x01; // Current Comulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(tal));
  
  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&tal);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;
  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::getTapeAlerts");
  SCSI::ExceptionLauncher(sgh, "SCSI error in getTapeAlerts:");
  /* Return the ACTIVE tape alerts (this is indicated by "flag" (see 
   * SSC-4: 8.2.3 TapeAlert log page). As they are simply used for logging;
   * return strings. */
  for (size_t i = 0; i < tal.parameterNumber(); i++) {
    if (tal.parameters[i].flag)
      ret.push_back(SCSI::Structures::toU16(tal.parameters[i].parameterCode));
  }
  return ret;
}

/**
 * Translate  tape alert codes into strings.
 */
std::vector<std::string> drive::DriveGeneric::getTapeAlerts(const std::vector<uint16_t>& tacs){
  /* return vector */
  std::vector<std::string> ret;
  /* convert tape alert codes to strings */
  for (std::vector<uint16_t>::const_iterator code =  tacs.begin(); code!= tacs.end(); code++) {
    ret.push_back(SCSI::tapeAlertToString(*code));
  }
  return ret;
}

/**
 * Translate tape alert codes into compact strings.
 */
std::vector<std::string> drive::DriveGeneric::getTapeAlertsCompact(const std::vector<uint16_t>& tacs){
  /* return vector */
  std::vector<std::string> ret;
  /* convert tape alert codes to strings */
  for (std::vector<uint16_t>::const_iterator code =  tacs.begin(); code!= tacs.end(); code++) {
    ret.push_back(SCSI::tapeAlertToCompactString(*code));
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
void drive::DriveGeneric::setDensityAndCompression(bool compression,
    unsigned char densityCode)  {
  SCSI::Structures::modeSenseDeviceConfiguration_t devConfig;
  { // get info from the drive
    SCSI::Structures::modeSense6CDB_t cdb;
    SCSI::Structures::senseData_t<255> senseBuff;
    SCSI::Structures::LinuxSGIO_t sgh;

    cdb.pageCode = SCSI::modeSensePages::deviceConfiguration;
    cdb.allocationLength = sizeof (devConfig);

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&devConfig);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    castor::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveGeneric::setDensityAndCompression");
    SCSI::ExceptionLauncher(sgh, "SCSI error in setDensityAndCompression:");
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
    castor::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveGeneric::setDensityAndCompression");
    SCSI::ExceptionLauncher(sgh, "SCSI error in setDensityAndCompression:");
  }
}

//------------------------------------------------------------------------------
// setLogicalBlockProtection
//------------------------------------------------------------------------------
void drive::DriveGeneric::setLogicalBlockProtection(
  const unsigned char method, const unsigned char methodLength,
  const bool enableLBPforRead, const bool enableLBPforWrite) {

  SCSI::Structures::modeSenseControlDataProtection_t controlDataProtection;
  {
    /* fetch Control Data Protection */
    SCSI::Structures::modeSense6CDB_t cdb;
    SCSI::Structures::senseData_t<255> senseBuff;
    SCSI::Structures::LinuxSGIO_t sgh;

    cdb.pageCode = SCSI::modeSensePages::controlDataProtection;
    cdb.subPageCode = SCSI::modePageControlDataProtection::subpageCode;
    cdb.allocationLength = sizeof (controlDataProtection);

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&controlDataProtection);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl"  );     
    SCSI::ExceptionLauncher(sgh,
      std::string("SCSI error fetching data in setLogicalBlockProtection: ") +
      SCSI::statusToString(sgh.status));
  }

  {
    /* Use the previously fetched page, modify fields and submit it */
    SCSI::Structures::modeSelect6CDB_t cdb;
    SCSI::Structures::senseData_t<255> senseBuff;
    SCSI::Structures::LinuxSGIO_t sgh;

    cdb.PF = 1; // means nothing for IBM, LTO, T10000
    cdb.paramListLength = sizeof (controlDataProtection.header) +
      sizeof (controlDataProtection.blockDescriptor) +
      SCSI::controlDataProtectionModePageLengthAddition + 
      SCSI::Structures::toU16(controlDataProtection.modePage.pageLength);    
    if (cdb.paramListLength > sizeof(controlDataProtection)) {
      // should never happen 
      throw castor::exception::Exception(
        std::string("cdb.paramListLength greater then size "
                    "of controlDataProtection in setLogicalBlockProtection"));
    }
    controlDataProtection.header.modeDataLength = 0; // must be 0 for IBM, LTO 
                                                     // ignored by T10000
    controlDataProtection.modePage.LBPMethod = method;
    controlDataProtection.modePage.LBPInformationLength = methodLength;
    controlDataProtection.modePage.LBP_W = enableLBPforWrite;
    controlDataProtection.modePage.LBP_R = enableLBPforRead;

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&controlDataProtection);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_TO_DEV;

    /* Manage both system error and SCSI errors. */
    castor::exception::Errnum::throwOnMinusOne(   
    m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl"  );     
    SCSI::ExceptionLauncher(sgh,
            std::string("SCSI error setting data in setDataProtection : ") +
            SCSI::statusToString(sgh.status));
  }
}

//------------------------------------------------------------------------------
// enableCRC32CLogicalBlockProtectionReadOnly
//------------------------------------------------------------------------------
void drive::DriveGeneric::enableCRC32CLogicalBlockProtectionReadOnly() {
  setLogicalBlockProtection(SCSI::LBPMethods::CRC32C,
    SCSI::LBPMethods::CRC32CLength,true,false);
}

//------------------------------------------------------------------------------
// enableCRC32CLogicalBlockProtectionReadWrite
//------------------------------------------------------------------------------
void drive::DriveGeneric::enableCRC32CLogicalBlockProtectionReadWrite() {
  setLogicalBlockProtection(SCSI::LBPMethods::CRC32C,
    SCSI::LBPMethods::CRC32CLength,true,true);
}

//------------------------------------------------------------------------------
// enableReedSolomonLogicalBlockProtectionReadOnly
//------------------------------------------------------------------------------
void drive::DriveGeneric::enableReedSolomonLogicalBlockProtectionReadOnly() {
  setLogicalBlockProtection(SCSI::LBPMethods::ReedSolomon,
    SCSI::LBPMethods::ReedSolomonLegth,true,false);
}

//------------------------------------------------------------------------------
// enableReedSolomonLogicalBlockProtectionReadWrite
//------------------------------------------------------------------------------
void drive::DriveGeneric::enableReedSolomonLogicalBlockProtectionReadWrite() {
  setLogicalBlockProtection(SCSI::LBPMethods::ReedSolomon,
    SCSI::LBPMethods::ReedSolomonLegth,true,true);
}

//------------------------------------------------------------------------------
// disableLogicalBlockProtection
//------------------------------------------------------------------------------
void drive::DriveGeneric::disableLogicalBlockProtection() {
  setLogicalBlockProtection(0,0,false,false);      
}

//------------------------------------------------------------------------------
// getLBPInfo
//------------------------------------------------------------------------------
drive::LBPInfo drive::DriveGeneric::getLBPInfo() {
  SCSI::Structures::modeSenseControlDataProtection_t controlDataProtection;
  drive::LBPInfo LBPdata;
  
  /* fetch Control Data Protection */
  SCSI::Structures::modeSense6CDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  cdb.pageCode = SCSI::modeSensePages::controlDataProtection;
  cdb.subPageCode = SCSI::modePageControlDataProtection::subpageCode;
  cdb.allocationLength = sizeof (controlDataProtection);

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&controlDataProtection);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl"  );     
  SCSI::ExceptionLauncher(sgh,
    std::string("SCSI error fetching data in getLBPInfo: ") +
    SCSI::statusToString(sgh.status));

  LBPdata.method = controlDataProtection.modePage.LBPMethod;
  LBPdata.methodLength = controlDataProtection.modePage.LBPInformationLength;  
  LBPdata.enableLBPforRead = (1 == controlDataProtection.modePage.LBP_R);
  LBPdata.enableLBPforWrite = (1 == controlDataProtection.modePage.LBP_W);

  return LBPdata;
}

/**
 * Function that checks if a tape is blank (contains no records) 
 * @return true if tape is blank, false otherwise
 */      
bool drive::DriveGeneric::isTapeBlank() {
  struct mtop mtCmd1;
  mtCmd1.mt_op = MTREW;
  mtCmd1.mt_count = 1;
  
  struct mtop mtCmd2;
  mtCmd2.mt_op = MTFSR;
  mtCmd2.mt_count = 1;
 
  struct mtget mtInfo;
  
  if((0 == m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &mtCmd1)) && (0 != m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &mtCmd2))) {
    //we are doing it the old CASTOR way (see readlbl.c)
    if(m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo)>=0) {
      if(GMT_EOD(mtInfo.mt_gstat) && GMT_BOT(mtInfo.mt_gstat)) {
        return true;
      }
    }    
  }
  
  struct mtop mtCmd3;
  mtCmd3.mt_op = MTREW;
  mtCmd3.mt_count = 1;
  m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &mtCmd3);
  
  return false;
}

/**
 * Set the buffer write switch in the st driver. This is directly matching a configuration
 * parameter in CASTOR, so this function has to be public and usable by a higher level
 * layer, unless the parameter turns out to be disused.
 * @param bufWrite: value of the buffer write switch
 */
void drive::DriveGeneric::setSTBufferWrite(bool bufWrite)  {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTSETDRVBUFFER;
  m_mtCmd.mt_count = bufWrite ? (MT_ST_SETBOOLEANS | MT_ST_BUFFER_WRITES) : (MT_ST_CLEARBOOLEANS | MT_ST_BUFFER_WRITES);
  castor::exception::Errnum::throwOnMinusOne(
     m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
     "Failed ST ioctl (MTSETDRVBUFFER) in DriveGeneric::setSTBufferWrite");
}

/**
 * Jump to end of recorded media. This will use setSTFastMTEOM() to disable MT_ST_FAST_MTEOM.
 * (See TapeServer's handbook for details). This is used to rebuild the MIR (StorageTek)
 * or tape directory (IBM).
 * Tape directory rebuild is described only for IBM but currently applied to 
 * all tape drives.
 * TODO: synchronous? Timeout?
 */    
void drive::DriveGeneric::spaceToEOM(void)  {
  setSTFastMTEOM(false);
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTEOM;
  m_mtCmd.mt_count = 1;
  castor::exception::Errnum::throwOnMinusOne(
     m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
     "Failed ST ioctl (MTEOM) in DriveGeneric::spaceToEOM");
}

/**
 * Set the MTFastEOM option of the ST driver. This function is used only internally in 
 * mounttape (in CAStor), so it could be a private function, not visible to 
 * the higher levels of the software (TODO: protected?).
 * @param fastMTEOM the option switch.
 */
void drive::DriveGeneric::setSTFastMTEOM(bool fastMTEOM)  {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTSETDRVBUFFER;
  m_mtCmd.mt_count = fastMTEOM ? (MT_ST_SETBOOLEANS | MT_ST_FAST_MTEOM) : (MT_ST_CLEARBOOLEANS | MT_ST_FAST_MTEOM);
  castor::exception::Errnum::throwOnMinusOne(
     m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
     "Failed ST ioctl (MTSETDRVBUFFER) in DriveGeneric::setSTFastMTEOM");
}

/**
 * Jump to end of data. EOM in ST driver jargon, end of data (which is more accurate)
 * in SCSI terminology). This uses the fast setting (not to be used for MIR rebuild) 
 */
void drive::DriveGeneric::fastSpaceToEOM(void)  {
  setSTFastMTEOM(true);
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTEOM;
  m_mtCmd.mt_count = 1;
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
      "Failed ST ioctl (MTEOM) in DriveGeneric::fastSpaceToEOM");
}

/**
 * Rewind tape.
 */
void drive::DriveGeneric::rewind(void)  {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTREW;
  m_mtCmd.mt_count = 1;
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
      "Failed ST ioctl (MTREW) in DriveGeneric::rewind");
}

/**
 * Space count file marks backwards.
 * @param count
 */
void drive::DriveGeneric::spaceFileMarksBackwards(size_t count)  {
  size_t tobeskipped = count;
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTBSF;
  while (tobeskipped > 0) {
    size_t c = (tobeskipped > 0x7FFFFF) ? 0x7FFFFF : tobeskipped;
    m_mtCmd.mt_count = (int)c;
    castor::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd), 
    "Failed ST ioctl (MTBSF) in DriveGeneric::spaceFileMarksBackwards");
    tobeskipped -= c;
  }  
}

/**
 * Space count file marks forward.
 * @param count
 */
void drive::DriveGeneric::spaceFileMarksForward(size_t count)  {
  size_t tobeskipped = count;
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTFSF;
  while (tobeskipped > 0) {
    size_t c = (tobeskipped > 0x7FFFFF) ? 0x7FFFFF : tobeskipped;
    m_mtCmd.mt_count = (int)c;
    castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd), 
      "Failed ST ioctl (MTFSF) in DriveGeneric::spaceFileMarksForward");
    tobeskipped -= c;
  }  
}

/**
 * Unload the tape.
 */
void drive::DriveGeneric::unloadTape(void)  {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTUNLOAD;
  m_mtCmd.mt_count = 1;
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
      "Failed ST ioctl (MTUNLOAD) in DriveGeneric::unloadTape");
}

/**
 * Synch call to the tape drive. This function will not return before the 
 * data in the drive's buffer is actually committed to the medium.
 */
void drive::DriveGeneric::flush(void)  {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTWEOF; //Not using MTNOP because it doesn't do what it claims (see st source code) so here we put "write sync file marks" with count set to 0.
  // The following text is a quote from the SCSI Stream commands manual (SSC-3):
  // NOTE 25 Upon completion of any buffered write operation, the application client may issue a WRITE FILEMARKS(16) command with the IMMED bit set to zero and the FILEMARK COUNT field set to zero to perform a synchronize operation (see 4.2.10).
  m_mtCmd.mt_count = 0;
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
      "Failed ST ioctl (MTWEOF) in DriveGeneric::flush");
}

/**
 * Write count file marks. The function does not return before the file marks 
 * are committed to medium.
 * @param count
 */
void drive::DriveGeneric::writeSyncFileMarks(size_t count)  {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTWEOF;
  m_mtCmd.mt_count = (int)count;
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
      "Failed ST ioctl (MTWEOF) in DriveGeneric::writeSyncFileMarks");
}

/**
 * Write count file marks asynchronously. The file marks are just added to the drive's
 * buffer and the function return immediately.
 * @param count
 */
void drive::DriveGeneric::writeImmediateFileMarks(size_t count)  {
  struct mtop m_mtCmd;
  m_mtCmd.mt_op = MTWEOFI; //Undocumented in "man st" needs the mtio_add.hh header file (see above)
  m_mtCmd.mt_count = (int)count;
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
      "Failed ST ioctl (MTWEOFI) in DriveGeneric::writeImmediateFileMarks");
}

/**
 * Write a data block to tape.
 * @param data pointer the the data block
 * @param count size of the data block
 */
void drive::DriveGeneric::writeBlock(const void * data, size_t count)  {
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.write(m_tapeFD, data, count),
      "Failed ST write in DriveGeneric::writeBlock");
}

/**
 * Read a data block from tape.
 * @param data pointer the the data block
 * @param count size of the data block
 * @return the actual size of read data
 */
ssize_t drive::DriveGeneric::readBlock(void * data, size_t count)  {
  ssize_t res = m_sysWrapper.read(m_tapeFD, data, count);
  castor::exception::Errnum::throwOnMinusOne(res, 
      "Failed ST read in DriveGeneric::readBlock");
  return res;
}

/**
 * Read a data block from tape. Throw an exception if the read block is not
 * the exact size of the buffer.
 * @param data pointer the the data block
 * @param count size of the data block
 * @return the actual size of read data
 */
void drive::DriveGeneric::readExactBlock(void * data, size_t count, std::string context)  {
  ssize_t res = m_sysWrapper.read(m_tapeFD, data, count);
  // First handle block too big
  if (-1 == res && ENOSPC == errno)
    throw UnexpectedSize(context);
  // Generic handling of other errors
  castor::exception::Errnum::throwOnMinusOne(res, 
      context+": Failed ST read in DriveGeneric::readExactBlock");
  // Handle block too small
  if ((size_t) res != count)
    throw UnexpectedSize(context);
}

/**
 * Read over a file mark. Throw an exception we do not read one.
 * @return the actual size of read data
 */
void drive::DriveGeneric::readFileMark(std::string context)  {
  char buff[4]; // We need to try and read at least a small amount of data
                // due to a bug in mhvtl
  ssize_t res = m_sysWrapper.read(m_tapeFD, buff, 4);
  // First handle block too big: this is not a file mark (size 0)
  if (-1 == res && ENOSPC == errno)
    throw NotAFileMark(context);
  // Generic handling of other errors
  castor::exception::Errnum::throwOnMinusOne(res, 
      context+": Failed ST read in DriveGeneric::readFileMark");
  // Handle the unlikely case when the block fits
  if (res)
    throw NotAFileMark(context);
}
   

void drive::DriveGeneric::SCSI_inquiry() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::inquiryCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  unsigned char dataBuff[130];
  
  memset(&dataBuff, 0, sizeof (dataBuff));
  
  /* Build command: just declare the bufffer's size. */
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));
  
  sgh.setCDB(&cdb);
  sgh.setSenseBuffer(&senseBuff);
  sgh.setDataBuffer(dataBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::SCSI_inquiry");
  
  /* TODO : this usage of std::cout cannot stay on the long run! */
  std::cout << "INQUIRY result: " << std::endl
      << "sgh.dxfer_len=" << sgh.dxfer_len
      << " sgh.sb_len_wr=" << ((int) sgh.sb_len_wr)
      << " sgh.status=" << ((int) sgh.status)
      << " sgh.info=" << ((int) sgh.info)
      << std::endl;
  std::cout << SCSI::Structures::hexDump(dataBuff)
      << SCSI::Structures::toString(*((SCSI::Structures::inquiryData_t *) dataBuff));
}


drive::compressionStats drive::DriveT10000::getCompression()  {
  compressionStats driveCompressionStats;
  
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  unsigned char dataBuff[1024];
  
  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::sequentialAccessDevicePage;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveT10000::getCompression");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveT10000::getCompression:");

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
        driveCompressionStats.fromHost = logPageParam.getU64Value();
        break;
      case SCSI::sequentialAccessDevicePage::writtenOnTape:
        driveCompressionStats.toTape = logPageParam.getU64Value();
        break;
      case SCSI::sequentialAccessDevicePage::readFromTape:
        driveCompressionStats.fromTape = logPageParam.getU64Value();
        break;
      case SCSI::sequentialAccessDevicePage::readByInitiator:
        driveCompressionStats.toHost = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof (logPageParam.header);
  }

  return driveCompressionStats;
}

drive::compressionStats drive::DriveLTO::getCompression()  {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  compressionStats driveCompressionStats;
  unsigned char dataBuff[1024];
  SCSI::Structures::senseData_t<255> senseBuff;

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::dataCompression32h;
  cdb.PC = 0x01; // Current Comulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveLTO::getCompression");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getCompression:");

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
        driveCompressionStats.fromHost = logPageParam.getU64Value() * mb;
        break;
      case SCSI::dataCompression32h::bytesTransferredFromServer:
        driveCompressionStats.fromHost += logPageParam.getS64Value();
        break;
        // toTape  
      case SCSI::dataCompression32h::mbWrittenToTape:
        driveCompressionStats.toTape = logPageParam.getU64Value() * mb;
        break;
      case SCSI::dataCompression32h::bytesWrittenToTape:
        driveCompressionStats.toTape += logPageParam.getS64Value();
        break;
        // fromTape     
      case SCSI::dataCompression32h::mbReadFromTape:
        driveCompressionStats.fromTape = logPageParam.getU64Value() * mb;
        break;
      case SCSI::dataCompression32h::bytesReadFromTape:
        driveCompressionStats.fromTape += logPageParam.getS64Value();
        break;
        // toHost            
      case SCSI::dataCompression32h::mbTransferredToServer:
        driveCompressionStats.toHost = logPageParam.getU64Value() * mb;
        break;
      case SCSI::dataCompression32h::bytesTransferredToServer:
        driveCompressionStats.toHost += logPageParam.getS64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof (logPageParam.header);
  }

  return driveCompressionStats;
}

drive::compressionStats drive::DriveIBM3592::getCompression()  {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  compressionStats driveCompressionStats;
  unsigned char dataBuff[1024];

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::blockBytesTransferred;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));


  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveIBM3592::getCompression");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getCompression:");

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
        driveCompressionStats.fromHost = logPageParam.getU64Value() << 10;
        break;
      case SCSI::blockBytesTransferred::deviceWriteKiBProcessed:
        driveCompressionStats.toTape = logPageParam.getU64Value() << 10;
        break;
      case SCSI::blockBytesTransferred::deviceReadKiBProcessed:
        driveCompressionStats.fromTape = logPageParam.getU64Value() << 10;
        break;
      case SCSI::blockBytesTransferred::hostReadKiBProcessed:
        driveCompressionStats.toHost = logPageParam.getU64Value() << 10;
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof (logPageParam.header);
  }

  return driveCompressionStats;
}

//------------------------------------------------------------------------------
// testUnitReady
//------------------------------------------------------------------------------
void drive::DriveGeneric::testUnitReady() const {
  SCSI::Structures::testUnitReadyCDB_t cdb;
  
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;
 
  sgh.setCDB(&cdb);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_NONE;
 
  /* Manage both system error and SCSI errors. */
  castor::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveGeneric::testUnitReady");
  
  SCSI::ExceptionLauncher(sgh,"SCSI error in testUnitReady:");
}

//------------------------------------------------------------------------------
// waitUntilReady
//------------------------------------------------------------------------------
void drive::DriveGeneric::waitUntilReady(const uint32_t timeoutSecond)  {
    
  waitTestUnitReady(timeoutSecond);

  // we need to reopen the drive to update the GMT_ONLINE cache of the st driver
  castor::exception::Errnum::throwOnMinusOne(m_sysWrapper.close(m_tapeFD),
    std::string("Could not close device file: ") + m_SCSIInfo.nst_dev);
  castor::exception::Errnum::throwOnMinusOne(
    m_tapeFD = m_sysWrapper.open(m_SCSIInfo.nst_dev.c_str(), O_RDWR | O_NONBLOCK),
          std::string("Could not open device file: ") + m_SCSIInfo.nst_dev);

  struct mtget mtInfo;

  /* Read drive status */
  castor::exception::Errnum::throwOnMinusOne(
  m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo), 
  std::string("Could not read drive status: ") + m_SCSIInfo.nst_dev);

  if(GMT_ONLINE(mtInfo.mt_gstat)==0) {
    castor::exception::Exception ex;
    ex.getMessage() << "Tape drive empty after waiting " <<
      timeoutSecond << " seconds.";
    throw ex;
  }
}

void drive::DriveGeneric::waitTestUnitReady(const uint32_t timeoutSecond)  {
  std::string lastTestUnitReadyExceptionMsg("");
  castor::utils::Timer t;
  // Query the tape drive at least once hence a do while loop/
  do  {
    // testUnitReady() uses SG_IO TEST_UNIT_READY to avoid calling open on the
    // st driver, because the open of the st driver is not as tolerant of
    // certain types of error such as when an incompatible media type is mounted
    // into a drive
    try {
      testUnitReady();
      return;
    } catch (castor::tape::SCSI::NotReadyException &ex){
      lastTestUnitReadyExceptionMsg = ex.getMessage().str();
    } catch (castor::tape::SCSI::UnitAttentionException &ex){
      lastTestUnitReadyExceptionMsg = ex.getMessage().str();
    } catch (...) {
      throw;
    }
    
    usleep(100 * 1000); // 1/10 second
  } while(t.secs() < timeoutSecond);

  // Throw an exception for the last tolerated exception that exceeded the timer
  castor::exception::Exception ex;
  ex.getMessage() << "Failed to test unit ready after waiting " <<
    timeoutSecond << " seconds: " << lastTestUnitReadyExceptionMsg;
  throw ex;
}

}}}
