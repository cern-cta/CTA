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

#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/drive/FakeDrive.hpp"
#include "castor/tape/tapeserver/drive/DriveGeneric.hpp"

#include "common/Timer.hpp"
#include "common/CRC.hpp"
#include "common/exception/MemException.hpp"

#include <errno.h>

#include <string>
#include <map>
#include <list>

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
  } else if (std::string::npos != di.product.find("MHVTL")) {
    return new DriveMHVTL(di, sw);
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
    throw cta::exception::Exception(std::string("Unsupported drive type: ") + di.product);
  }
}

drive::DriveGeneric::DriveGeneric(SCSI::DeviceInfo di, System::virtualWrapper& sw) : m_SCSIInfo(di),
m_tapeFD(-1),  m_sysWrapper(sw), m_lbpToUse(lbpToUse::disabled) {
  /* Open the device files */
  /* We open the tape device file non-blocking as blocking open on rewind tapes (at least)
   * will fail after a long timeout when no tape is present (at least with mhvtl)
   */
  cta::exception::Errnum::throwOnMinusOne(
      m_tapeFD = m_sysWrapper.open(m_SCSIInfo.nst_dev.c_str(), O_RDWR | O_NONBLOCK),
      std::string("Could not open device file: ") + m_SCSIInfo.nst_dev);
}

void drive::DriveIBM3592::clearCompressionStats()  {
  SCSI::Structures::logSelectCDB_t cdb;
  cdb.PCR = 1; /* PCR set */
  cdb.PC = 0x3; /* PC = 11b  for T10000 only*/
  cdb.pageCode = SCSI::logSensePages::blockBytesTransferred;

  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  sgh.setCDB(&cdb);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_NONE;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::clearCompressionStats");
  SCSI::ExceptionLauncher(sgh, "SCSI error in clearCompressionStats:");
}

void drive::DriveT10000::clearCompressionStats() {
  m_compressionStatsBase = getCompressionStats();
}

void drive::DriveLTO::clearCompressionStats() {
  SCSI::Structures::logSelectCDB_t cdb;
  cdb.PCR = 1; /* PCR set */
  cdb.PC = 0x3; /* PC = 11b  for T10000 only*/
  cdb.pageCode = SCSI::logSensePages::dataCompression32h;

  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  sgh.setCDB(&cdb);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_NONE;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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

drive::deviceInfo drive::DriveT10000::getDeviceInfo()  {
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
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveT10000::getDeviceInfo");
  SCSI::ExceptionLauncher(sgh, "SCSI error in getDeviceInfo:");

  devInfo.product = SCSI::Structures::toString(inquiryData.prodId);
  std::string productRevisionMinor = SCSI::Structures::toString(inquiryData.vendorSpecific1).substr(0,4);
  devInfo.productRevisionLevel = SCSI::Structures::toString(inquiryData.prodRevLvl) + productRevisionMinor;
  devInfo.vendor = SCSI::Structures::toString(inquiryData.T10Vendor);
  devInfo.serialNumber = getSerialNumber();
  devInfo.isPIsupported = inquiryData.protect;
  return devInfo;
}

drive::deviceInfo drive::DriveMHVTL::getDeviceInfo()  {
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
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveMHVTL::getDeviceInfo");
  SCSI::ExceptionLauncher(sgh, "SCSI error in getDeviceInfo:");

  devInfo.product = SCSI::Structures::toString(inquiryData.prodId);
  devInfo.productRevisionLevel = SCSI::Structures::toString(inquiryData.prodRevLvl);
  devInfo.vendor = SCSI::Structures::toString(inquiryData.T10Vendor);
  devInfo.serialNumber = getSerialNumber();
  devInfo.isPIsupported = inquiryData.protect;
  return devInfo;
}

SCSI::Structures::RAO::udsLimits drive::DriveMHVTL::getLimitUDS(){
  throw drive::DriveDoesNotSupportRAOException("MHVTL does not support RAO Enterprise.");
}

void drive::DriveMHVTL::queryRAO(std::list<SCSI::Structures::RAO::blockLims> &files, int maxSupported){
  //The query RAO method of MHVTL drive returns nothing
  //something could be implemented for testing...
}

/**
 * Generic SCSI path, used for passing to external scripts.
 * @return    Path to the generic SCSI device file.
 */
std::string drive::DriveGeneric::getGenericSCSIPath() {
  return m_SCSIInfo.sg_dev;
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
    throw cta::exception::Exception(std::string("An overflow has occurred in getPostitionInfo"));
  }
  return posInfo;
}

/**
 * Return physical position of the drive.
 *
 * @return physicalPositionInfo class. This contains the wrap and linear position (LPOS).
 */
drive::physicalPositionInfo drive::DriveGeneric::getPhysicalPositionInfo()
{
  SCSI::Structures::requestSenseCDB_t cdb;
  SCSI::Structures::requestSenseData_t requestSenseData;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::LinuxSGIO_t sgh;

  // The full Request Sense data record is 96 bytes. However, we are only interested in
  // the PHYSICAL WRAP and LPOS fields (bytes 29-33) so we can discard the rest.
  cdb.allocationLength = 34;

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&requestSenseData);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  // Manage both system error and SCSI errors
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveGeneric::getPhysicalPositionInfo");
  SCSI::ExceptionLauncher(sgh, "SCSI error in getPhysicalPositionInfo:");

  physicalPositionInfo posInfo;
  posInfo.wrap = requestSenseData.physicalWrap;
  posInfo.lpos = SCSI::Structures::toU32(requestSenseData.relativeLPOSValue);
  return posInfo;
}

/**
* Returns all the end of wrap positions of the mounted tape
*
* @return a vector of endOfWrapsPositions.
*/
std::vector<drive::endOfWrapPosition> drive::DriveGeneric::getEndOfWrapPositions() {
  throw cta::exception::Exception("In drive::DriveGeneric::getEndOfWrapPositions(), the drive does not support REOWP SCSI command.");
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
  cta::exception::Errnum::throwOnMinusOne(
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
  for (std::vector<uint16_t>::const_iterator code =  tacs.begin(); code!= tacs.end(); ++code) {
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
  for (std::vector<uint16_t>::const_iterator code =  tacs.begin(); code!= tacs.end(); ++code) {
    ret.push_back(SCSI::tapeAlertToCompactString(*code));
  }
  return ret;
}

//------------------------------------------------------------------------------
// tapeAlertsCriticalForWrite
//------------------------------------------------------------------------------
bool drive::DriveGeneric::tapeAlertsCriticalForWrite(
  const std::vector<uint16_t> & codes) {
  for(std::vector<uint16_t>::const_iterator code = codes.begin(); code != codes.end(); ++code) {
    if(SCSI::isTapeAlertCriticalForWrite(*code)) {
      return true;
    }
  }
  return false;
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
    cta::exception::Errnum::throwOnMinusOne(
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
    cta::exception::Errnum::throwOnMinusOne(
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
    cta::exception::Errnum::throwOnMinusOne(
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
      throw cta::exception::Exception(
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
    cta::exception::Errnum::throwOnMinusOne(
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
  m_lbpToUse=lbpToUse::crc32cReadOnly;
  setLogicalBlockProtection(SCSI::logicBlockProtectionMethod::CRC32C,
    SCSI::logicBlockProtectionMethod::CRC32CLength,true,false);
}

//------------------------------------------------------------------------------
// enableCRC32CLogicalBlockProtectionReadWrite
//------------------------------------------------------------------------------
void drive::DriveGeneric::enableCRC32CLogicalBlockProtectionReadWrite() {
  m_lbpToUse=lbpToUse::crc32cReadWrite;
  setLogicalBlockProtection(SCSI::logicBlockProtectionMethod::CRC32C,
    SCSI::logicBlockProtectionMethod::CRC32CLength,true,true);
}

//------------------------------------------------------------------------------
// disableLogicalBlockProtection
//------------------------------------------------------------------------------
void drive::DriveGeneric::disableLogicalBlockProtection() {
  m_lbpToUse=lbpToUse::disabled;
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
  cta::exception::Errnum::throwOnMinusOne(
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

//------------------------------------------------------------------------------
// Encryption interface
//------------------------------------------------------------------------------
void drive::DriveGeneric::setEncryptionKey(const std::string &encryption_key) {
  if(!isEncryptionCapEnabled())
    throw cta::exception::Exception("In DriveGeneric::setEncryptionKey: Tried to enable encryption on drive "
                                    "without encryption capabilities enabled.");
  if (encryption_key.empty())
    clearEncryptionKey();
  else {
    SCSI::Structures::LinuxSGIO_t sgh;
    SCSI::Structures::encryption::spoutCDB_t cdb;
    SCSI::Structures::senseData_t<255> senseBuff;
    SCSI::Structures::encryption::spoutSDEParam_t sps;

    SCSI::Structures::setU16(sps.pageCode, SCSI::encryption::spoutSecurityProtocolSpecificPages::setDataEncryptionPage);
    /*
     * Length is size of the spoutSDEParam_t - (sizeof(sps.pageCode) + sizeof(sps.length))
     */
    SCSI::Structures::setU16(sps.length, sizeof(SCSI::Structures::encryption::spoutSDEParam_t) - 4);

    sps.nexusScope = SCSI::encryption::encryptionNexusScopes::scopeLocal; // oracle compatibility
    sps.encryptionMode = SCSI::encryption::encryptionModes::modeEncrypt;
    sps.decryptionMode = SCSI::encryption::decryptionModes::modeMixed;
    sps.algorithmIndex = 0x01;
    sps.keyFormat = SCSI::encryption::keyFormatTypes::keyFormatNormal;
    std::copy(encryption_key.begin(), encryption_key.end(), sps.keyData);
    /*
     * This means that if the key given is smaller, it's right padded with zeros.
     * If the key given is bigger, we the first 256-bit are used.
     */

    cdb.securityProtocol = SCSI::encryption::spoutSecurityProtocolPages::tapeDataEncryption;
    SCSI::Structures::setU16(cdb.securityProtocolSpecific,
                             SCSI::encryption::spoutSecurityProtocolSpecificPages::setDataEncryptionPage);
    SCSI::Structures::setU32(cdb.allocationLength, sizeof(SCSI::Structures::encryption::spoutSDEParam_t));

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&sps);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_TO_DEV;

    cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveGeneric::setEncryptionKey");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveGeneric::setEncryptionKey");
  }
}

bool drive::DriveGeneric::clearEncryptionKey() {
  if (!isEncryptionCapEnabled())
    return false;
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::encryption::spoutCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::encryption::spoutSDEParam_t sps;

  SCSI::Structures::setU16(sps.pageCode, SCSI::encryption::spoutSecurityProtocolSpecificPages::setDataEncryptionPage);

  sps.nexusScope = SCSI::encryption::encryptionNexusScopes::scopeLocal; // oracle compatibility
  sps.encryptionMode = SCSI::encryption::encryptionModes::modeDisable;
  sps.decryptionMode = SCSI::encryption::decryptionModes::modeDisable;
  sps.algorithmIndex = 0x01;
  sps.keyFormat = SCSI::encryption::keyFormatTypes::keyFormatNormal;

  /*
   * Length is size of the spoutSDEParam_t - (sizeof(sps.pageCode) + sizeof(sps.length))
   */
  SCSI::Structures::setU16(sps.length, sizeof(SCSI::Structures::encryption::spoutSDEParam_t) - 4);

  cdb.securityProtocol = SCSI::encryption::spoutSecurityProtocolPages::tapeDataEncryption;
  SCSI::Structures::setU16(cdb.securityProtocolSpecific, SCSI::encryption::spoutSecurityProtocolSpecificPages::setDataEncryptionPage);
  SCSI::Structures::setU32(cdb.allocationLength, sizeof(SCSI::Structures::encryption::spoutSDEParam_t));


  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&sps);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_TO_DEV;

  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveGeneric::clearEncryptionKey");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveGeneric::clearEncryptionKey");
  return true;
}

bool drive::DriveGeneric::isEncryptionCapEnabled() {
  return false;
}

bool drive::DriveIBM3592::isEncryptionCapEnabled() {
  /*
   * We are acquiring the encryption capability from the length of the SPIN index page.
   * If it has one page (only 0x00), then encryption is disabled from the libary.
   * If it has more pages, the encryption(0x00, 0x20 at the moment of writing), then it is enabled.
   */
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::encryption::spinCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::encryption::spinPageList_t<20> pl;

  cdb.securityProtocol = SCSI::encryption::spinSecurityProtocolPages::securityProtocolInformation;
  SCSI::Structures::setU16(cdb.securityProtocolSpecific, 0x0000);
  SCSI::Structures::setU32(cdb.allocationLength, sizeof(pl));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&pl);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveIBM3592::isEncryptionCapEnabled");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::isEncryptionCapEnabled");

  return SCSI::Structures::toU16(pl.supportedProtocolListLength) > 1;
}

bool drive::DriveLTO::isEncryptionCapEnabled() {
  /*
   * Encryption enable support. Initially copied from IBM enterprise implementation.
   */
  /*
   * We are acquiring the encryption capability from the length of the SPIN index page.
   * If it has one page (only 0x00), then encryption is disabled from the libary.
   * If it has more pages, the encryption(0x00, 0x20 at the moment of writing), then it is enabled.
   */
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::encryption::spinCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::encryption::spinPageList_t<20> pl;

  cdb.securityProtocol = SCSI::encryption::spinSecurityProtocolPages::securityProtocolInformation;
  SCSI::Structures::setU16(cdb.securityProtocolSpecific, 0x0000);
  SCSI::Structures::setU32(cdb.allocationLength, sizeof(pl));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&pl);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveLTO::isEncryptionCapEnabled");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::isEncryptionCapEnabled");

  return SCSI::Structures::toU16(pl.supportedProtocolListLength) > 1;
}


bool drive::DriveT10000::isEncryptionCapEnabled() {
  /*
   * We are acquiring the encryption capability from the inquiry page on Oracle T10k drives
   * because the SPIN index page is not available when encryption is disabled from
   * the Oracle libary interface (invalid cdb error).
   */
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::inquiryCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  SCSI::Structures::inquiryDataT10k_t inqData;

  // EVPD and page code set to zero => standard inquiry
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(inqData));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&inqData);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveT10000::isEncryptionCapEnabled");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveT10000::clearEncryptionKey");

  unsigned char keyManagement = inqData.keyMgmt; // oracle metric for key management

  return keyManagement != 0x0;
}

void drive::DriveMHVTL::setEncryptionKey(const std::string &encryption_key) {
  throw cta::exception::Exception("In DriveMHVTL::setEncryptionKey: Encryption cannot be enabled.");
}

bool drive::DriveMHVTL::clearEncryptionKey() {
  return false;
}

bool drive::DriveMHVTL::isEncryptionCapEnabled() {
  return false;
}

SCSI::Structures::RAO::udsLimits drive::DriveGeneric::getLimitUDS() {
    SCSI::Structures::LinuxSGIO_t sgh;
    SCSI::Structures::RAO::recieveRAO_t cdb;
    SCSI::Structures::senseData_t<127> senseBuff;
    SCSI::Structures::RAO::udsLimitsPage_t limitsSCSI;
    SCSI::Structures::RAO::udsLimits lims;

    cdb.serviceAction = 0x1d;
    cdb.udsLimits = 1;
    SCSI::Structures::setU32(cdb.allocationLength, sizeof(SCSI::Structures::RAO::udsLimitsPage_t));

    sgh.setCDB(&cdb);
    sgh.setSenseBuffer(&senseBuff);
    sgh.setDataBuffer(&limitsSCSI);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
            "Failed SG_IO ioctl in DriveGeneric::getLimitUDS");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveGeneric::getLimitUDS");

    lims.maxSupported = SCSI::Structures::toU16(limitsSCSI.maxSupported);
    lims.maxSize = SCSI::Structures::toU16(limitsSCSI.maxSize);

    return lims;
}

void drive::DriveGeneric::generateRAO(std::list<SCSI::Structures::RAO::blockLims> &files,
                                      int maxSupported) {
    SCSI::Structures::LinuxSGIO_t sgh;
    SCSI::Structures::RAO::generateRAO_t cdb;
    SCSI::Structures::senseData_t<127> senseBuff;

    int udSize = std::min((int) files.size(), maxSupported);

    std::unique_ptr<SCSI::Structures::RAO::udsDescriptor_t[]>  ud (new SCSI::Structures::RAO::udsDescriptor_t[udSize]());

    auto it = files.begin();
    for (int i = 0; i < udSize; ++i) {
      strncpy((char*)ud.get()[i].udsName, (char*)it->fseq, 10);
      SCSI::Structures::setU64(ud.get()[i].beginLogicalObjID, it->begin);
      SCSI::Structures::setU64(ud.get()[i].endLogicalObjID, it->end);
      ++it;
    }

    SCSI::Structures::RAO::generateRAOParams_t params;
    int real_params_len = sizeof(params) + (udSize - 1) *
                          sizeof(SCSI::Structures::RAO::udsDescriptor_t);
    std::unique_ptr<unsigned char[]>  dataBuff (new unsigned char[real_params_len]());

    cdb.serviceAction = 0x1d;

    SCSI::Structures::setU32(cdb.paramsListLength, real_params_len);
    SCSI::Structures::setU32(params.udsListLength, udSize * sizeof(SCSI::Structures::RAO::udsDescriptor_t));
    memcpy(dataBuff.get(), &params, 8); // copy first 2 fields
    memcpy(dataBuff.get() + 8, ud.get(), udSize * sizeof(SCSI::Structures::RAO::udsDescriptor_t));

    sgh.setCDB(&cdb);
    sgh.setSenseBuffer(&senseBuff);
    sgh.setDataBuffer(dataBuff.get(), real_params_len);
    sgh.dxfer_direction = SG_DXFER_TO_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
            "Failed SG_IO ioctl in DriveGeneric::requestRAO");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveGeneric::requestRAO");
}

void drive::DriveGeneric::receiveRAO(std::list<SCSI::Structures::RAO::blockLims> &files) {
    SCSI::Structures::LinuxSGIO_t sgh;
    SCSI::Structures::RAO::recieveRAO_t cdb;
    SCSI::Structures::senseData_t<255> senseBuff;
    SCSI::Structures::RAO::raoList_t *params;

    int udSize = files.size();
    int real_params_len = sizeof(SCSI::Structures::RAO::raoList_t) + (udSize - 1) *
                          sizeof(SCSI::Structures::RAO::udsDescriptor_t);
    std::unique_ptr<unsigned char[]>  dataBuff (new unsigned char[real_params_len]());
    memset(dataBuff.get(), 0, real_params_len);

    cdb.udsLimits = 0;
    cdb.serviceAction = 0x1d;

    SCSI::Structures::setU32(cdb.allocationLength, real_params_len);

    sgh.setCDB(&cdb);
    sgh.setSenseBuffer(&senseBuff);
    sgh.setDataBuffer(dataBuff.get(), real_params_len);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
            "Failed SG_IO ioctl in DriveGeneric::getRAO");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveGeneric::getRAO");

    params = (SCSI::Structures::RAO::raoList_t *) dataBuff.get();

    files.clear();
    uint32_t desc_list_len = SCSI::Structures::toU32(params->raoDescriptorListLength);
    int files_no = desc_list_len / sizeof(SCSI::Structures::RAO::udsDescriptor_t);
    SCSI::Structures::RAO::udsDescriptor_t *ud = params->udsDescriptors;
    while (files_no > 0) {
        SCSI::Structures::RAO::blockLims bl;
        strcpy(reinterpret_cast<char*>(bl.fseq), reinterpret_cast<char const*>(ud->udsName));
        bl.begin = SCSI::Structures::toU64(ud->beginLogicalObjID);
        bl.end = SCSI::Structures::toU64(ud->endLogicalObjID);
        files.emplace_back(bl);
        ud++;
        files_no--;
    }
}

void drive::DriveGeneric::queryRAO(std::list<SCSI::Structures::RAO::blockLims> &files,
                                   int maxSupported) {
    generateRAO(files, maxSupported);
    receiveRAO(files);
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
    cta::exception::Errnum::throwOnMinusOne(
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
    cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(m_tapeFD, MTIOCTOP, &m_mtCmd),
      "Failed ST ioctl (MTWEOFI) in DriveGeneric::writeImmediateFileMarks");
}

/**
 * Write a data block to tape.
 * @param data pointer the the data block
 * @param count size of the data block
 */
void drive::DriveGeneric::writeBlock(const void * data, size_t count)  {
  switch (m_lbpToUse) {
    case lbpToUse::crc32cReadWrite:
      {
        uint8_t * dataWithCrc32c =(new (std::nothrow)
          uint8_t [count+SCSI::logicBlockProtectionMethod::CRC32CLength]);
        if(nullptr == dataWithCrc32c) {
          throw cta::exception::MemException("Failed to allocate memory "
            " for a new MemBlock in DriveGeneric::writeBlock!");
        }
        memcpy(dataWithCrc32c, data, count);
        const size_t countWithCrc32c =
          cta::addCrc32cToMemoryBlock(
            SCSI::logicBlockProtectionMethod::CRC32CSeed,
            count, dataWithCrc32c);
        if (countWithCrc32c !=
          count+SCSI::logicBlockProtectionMethod::CRC32CLength) {
          delete[] dataWithCrc32c;
          cta::exception::Errnum::throwOnMinusOne(-1,
            "Failed in DriveGeneric::writeBlock: incorrect length for block"
            " with crc32c");
        }
        if (-1 == m_sysWrapper.write(m_tapeFD, dataWithCrc32c,
          countWithCrc32c)) {
          delete[] dataWithCrc32c;
          cta::exception::Errnum::throwOnMinusOne(-1,
            "Failed ST write with crc32c in DriveGeneric::writeBlock");
        }
        delete[] dataWithCrc32c;
        break;
      }
    case lbpToUse::crc32cReadOnly:
      throw cta::exception::Exception("In DriveGeneric::writeBlock: "
          "trying to write a block in CRC-readonly mode");
    case lbpToUse::disabled:
      {
          cta::exception::Errnum::throwOnMinusOne(
            m_sysWrapper.write(m_tapeFD, data, count),
            "Failed ST write in DriveGeneric::writeBlock");
          break;
      }
    default:
      throw cta::exception::Exception("In DriveGeneric::writeBlock: "
          "unknown LBP mode");
  }
}

/**
 * Read a data block from tape.
 * @param data pointer the the data block
 * @param count size of the data block
 * @return the actual size of read data
 */
ssize_t drive::DriveGeneric::readBlock(void * data, size_t count)  {
  switch (m_lbpToUse) {
    case lbpToUse::crc32cReadWrite:
    case lbpToUse::crc32cReadOnly:
      {
        uint8_t * dataWithCrc32c =(new (std::nothrow)
          uint8_t [count+SCSI::logicBlockProtectionMethod::CRC32CLength]);
        if(nullptr == dataWithCrc32c) {
          throw cta::exception::MemException("In DriveGeneric::readBlock: Failed to allocate memory");
        }
        const ssize_t res = m_sysWrapper.read(m_tapeFD, dataWithCrc32c,
          count+SCSI::logicBlockProtectionMethod::CRC32CLength);
        if ( -1 == res ) {
          delete[] dataWithCrc32c;
          cta::exception::Errnum::throwOnMinusOne(res,
            "In DriveGeneric::readBlock: Failed ST read (with checksum)");
        }
        if ( 0 == res ) {
          delete[] dataWithCrc32c;
          return 0;
        }
        const size_t dataLenWithoutCrc32c = res -
          SCSI::logicBlockProtectionMethod::CRC32CLength;
        if ( 0>= dataLenWithoutCrc32c) {
          delete[] dataWithCrc32c;
          throw cta::exception::Exception("In DriveGeneric::readBlock: wrong data block size, checksum cannot fit");
        }
        if (cta::verifyCrc32cForMemoryBlockWithCrc32c(
          SCSI::logicBlockProtectionMethod::CRC32CSeed,
          res, dataWithCrc32c)) {
            // everything is fine here do mem copy
            memcpy(data, dataWithCrc32c, dataLenWithoutCrc32c);
            delete[] dataWithCrc32c;
            return dataLenWithoutCrc32c;
        } else {
          delete[] dataWithCrc32c;
          throw cta::exception::Exception(
              "In DriveGeneric::readBlock: Failed checksum verification");
        }
      }
    case lbpToUse::disabled:
      {
        ssize_t res = m_sysWrapper.read(m_tapeFD, data, count);
        cta::exception::Errnum::throwOnMinusOne(res,
          "In DriveGeneric::readBlock: Failed ST read");
        return res;
      }
    default:
      throw cta::exception::Exception("In DriveGeneric::readBlock: unknown LBP type");
  }
}

/**
 * Read a data block from tape. Throw an exception if the read block is not
 * the exact size of the buffer.
 * @param data pointer the the data block
 * @param count size of the data block
 */
void drive::DriveGeneric::readExactBlock(void * data, size_t count, const std::string& context)  {

  switch (m_lbpToUse) {
    case lbpToUse::crc32cReadWrite:
    case lbpToUse::crc32cReadOnly:
      {
        uint8_t * dataWithCrc32c =(new (std::nothrow)
          uint8_t [count+SCSI::logicBlockProtectionMethod::CRC32CLength]);
        if(nullptr == dataWithCrc32c) {
          throw cta::exception::MemException("Failed to allocate memory "
            " for a new MemBlock in DriveGeneric::readBlock!");
        }
        const ssize_t res = m_sysWrapper.read(m_tapeFD, dataWithCrc32c,
          count+SCSI::logicBlockProtectionMethod::CRC32CLength);

        // First handle block too big
        if (-1 == res && ENOSPC == errno) {
          delete[] dataWithCrc32c;
          throw UnexpectedSize(context);
        }
        // ENOMEM may be returned if the tape block size is larger than 'count'
        if (-1 == res && ENOMEM == errno) {
          delete[] dataWithCrc32c;
          throw cta::exception::Errnum(errno, context +
                                              ": Failed ST read in DriveGeneric::readExactBlock. Tape volume label size not be in the CTA/CASTOR format.");
        }
        // Generic handling of other errors
        if (-1 == res) {
          delete[] dataWithCrc32c;
          cta::exception::Errnum::throwOnMinusOne(res,
            context+": Failed ST read with crc32c in DriveGeneric::readExactBlock");
        }
        // Handle mismatch
        if ((size_t) (res - SCSI::logicBlockProtectionMethod::CRC32CLength)
          != count) {
          delete[] dataWithCrc32c;
          throw UnexpectedSize(context);
        }
        if (cta::verifyCrc32cForMemoryBlockWithCrc32c(
          SCSI::logicBlockProtectionMethod::CRC32CSeed,
          res, dataWithCrc32c)) {
            // everything is fine here do mem copy
            memcpy(data,dataWithCrc32c,count);
            delete[] dataWithCrc32c;
        } else {
          delete[] dataWithCrc32c;
          cta::exception::Exception(context+"Failed checksum verification for ST read"
            " in DriveGeneric::readBlock");
        }
        break;
      }
    case lbpToUse::disabled:
      {
        ssize_t res = m_sysWrapper.read(m_tapeFD, data, count);
        // First handle block too big
        if (-1 == res && ENOSPC == errno)
          throw UnexpectedSize(context);
        // ENOMEM may be returned if the tape block size is larger than 'count'
        if (-1 == res && ENOMEM == errno)
          throw cta::exception::Errnum(errno, context +
                                              ": Failed ST read in DriveGeneric::readExactBlock. Tape volume label size not be in the CTA/CASTOR format.");
        // Generic handling of other errors
        cta::exception::Errnum::throwOnMinusOne(res,
            context+": Failed ST read in DriveGeneric::readExactBlock");
        // Handle block too small
        if ((size_t) res != count)
          throw UnexpectedSize(context);
        break;
      }
    default:
      throw cta::exception::Exception("In DriveGeneric::readExactBlock: unknown LBP type");
  }
}

/**
 * Read over a file mark. Throw an exception we do not read one.
 * @return the actual size of read data
 */
void drive::DriveGeneric::readFileMark(const std::string& context)  {
  char buff[4]; // We need to try and read at least a small amount of data
                // due to a bug in mhvtl
  ssize_t res = m_sysWrapper.read(m_tapeFD, buff, 4);
  // First handle block too big: this is not a file mark (size 0)
  if (-1 == res && ENOSPC == errno)
    throw NotAFileMark(context);
  // Generic handling of other errors
  cta::exception::Errnum::throwOnMinusOne(res,
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
  cta::exception::Errnum::throwOnMinusOne(
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
  compressionStats driveCompressionStats = getCompressionStats();
  // Calculate current compression stats
  compressionStats toBeReturned;
  toBeReturned.toHost = driveCompressionStats.toHost - m_compressionStatsBase.toHost;
  toBeReturned.toTape = driveCompressionStats.toTape - m_compressionStatsBase.toTape;
  toBeReturned.fromTape = driveCompressionStats.fromTape - m_compressionStatsBase.fromTape;
  toBeReturned.fromHost = driveCompressionStats.fromHost - m_compressionStatsBase.fromHost;

  return toBeReturned;
}

drive::compressionStats drive::DriveT10000::getCompressionStats() {

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
  cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveT10000::getCompression");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveT10000::getCompression:");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
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

void drive::DriveMHVTL::disableLogicalBlockProtection() { }

void drive::DriveMHVTL::enableCRC32CLogicalBlockProtectionReadOnly() {
  throw cta::exception::Exception(
    "In DriveMHVTL::enableCRC32CLogicalBlockProtectionReadOnly(): not supported");
}

void drive::DriveMHVTL::enableCRC32CLogicalBlockProtectionReadWrite() {
  throw cta::exception::Exception(
    "In DriveMHVTL::enableCRC32CLogicalBlockProtectionReadWrite(): not supported");
}

drive::LBPInfo drive::DriveMHVTL::getLBPInfo() {
  drive::LBPInfo ret;
  ret.enableLBPforRead = false;
  ret.enableLBPforWrite = false;
  ret.method = 0;
  ret.methodLength = 0;
  return ret;
}

drive::lbpToUse drive::DriveMHVTL::getLbpToUse() {
  return lbpToUse::disabled;
}

void drive::DriveMHVTL::setLogicalBlockProtection(const unsigned char method,
  unsigned char methodLength, const bool enableLPBforRead,
  const bool enableLBBforWrite) {
  if (method != 0 || methodLength != 0 || enableLBBforWrite || enableLPBforRead)
    throw cta::exception::Exception("In DriveMHVTL::setLogicalBlockProtection:: LBP cannot be enabled");
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
  cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveLTO::getCompression");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getCompression:");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
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

std::vector<castor::tape::tapeserver::drive::endOfWrapPosition> drive::DriveLTO::getEndOfWrapPositions() {
  std::vector<castor::tape::tapeserver::drive::endOfWrapPosition> ret;
  SCSI::Structures::readEndOfWrapPositionCDB_t cdb;
  //WrapNumberValid = 0, ReportAll = 1 and WrapNumber = 0 as we want all the end of wraps positions
  cdb.WNV = 0;
  cdb.RA = 1;
  cdb.wrapNumber = 0;
  //Each wrap descriptor is 12 bytes
  SCSI::Structures::setU32(cdb.allocationLength,12 * castor::tape::SCSI::maxLTOTapeWraps);

  SCSI::Structures::readEndOfWrapPositionDataLongForm_t data;

  SCSI::Structures::LinuxSGIO_t sgh;
  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&data);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

   /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveLTO::getEndOfWrapPositions");
  SCSI::ExceptionLauncher(sgh, "SCSI error in getEndOfWrapPositions:");

  int nbWrapReturned = data.getNbWrapsReturned();
  //Loop over the list of wraps of the tape returned by the drive
  for(int i = 0; i < nbWrapReturned; ++i){
    castor::tape::tapeserver::drive::endOfWrapPosition position;
    auto wrapDescriptor = data.wrapDescriptor[i];
    position.wrapNumber = SCSI::Structures::toU16(wrapDescriptor.wrapNumber);
    position.partition = SCSI::Structures::toU16(wrapDescriptor.partition);
    //blockId returned is 6*8 = 48 bytes, so we need to store it into a uint64_t
    position.blockId = SCSI::Structures::toU64(wrapDescriptor.logicalObjectIdentifier);
    ret.push_back(position);
  }
  return ret;
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
  cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveIBM3592::getCompression");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getCompression:");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
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
// pre-emptive evaluation statistics (SCSI Statistics)
//------------------------------------------------------------------------------
std::map<std::string,uint64_t> drive::DriveGeneric::getTapeWriteErrors() {
  // No available data
  return std::map<std::string,uint64_t>();
}

std::map<std::string,uint64_t> drive::DriveGeneric::getTapeReadErrors() {
  // No available data
  return std::map<std::string,uint64_t>();
}

std::map<std::string,float> drive::DriveGeneric::getQualityStats() {
  // No available data
  return std::map<std::string,float>();
}

std::map<std::string,uint32_t> drive::DriveGeneric::getDriveStats() {
  // No available data
  return std::map<std::string,uint32_t>();
}

std::map<std::string,uint32_t> drive::DriveGeneric::getVolumeStats() {
  // No available data
  return std::map<std::string,uint32_t>();
}

std::map<std::string,uint64_t> drive::DriveIBM3592::getTapeWriteErrors() {
  // SCSI counters get reset after read in DriveIBM3592.
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint64_t> driveWriteErrorStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::writeErrors;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveIBM3592::getTapeWriteErrors");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getTapeWriteErrors");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);
  while (logParameter < endPage) { /* values in KiBs and we use shift <<10 to get bytes */
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::writeErrorsDevicePage::totalCorrectedErrors:
        driveWriteErrorStats["mountTotalCorrectedWriteErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::writeErrorsDevicePage::totalProcessed:
        driveWriteErrorStats["mountTotalWriteBytesProcessed"] = logPageParam.getU64Value() << 10;
        break;
      case SCSI::writeErrorsDevicePage::totalUncorrectedErrors:
        driveWriteErrorStats["mountTotalUncorrectedWriteErrors"] = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return driveWriteErrorStats;
}

std::map<std::string,uint64_t> drive::DriveT10000::getTapeWriteErrors() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint64_t> driveWriteErrorStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::writeErrors;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveT10000::getTapeWriteErrors");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveT10000::getTapeWriteErrors");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);
  while (logParameter < endPage) {
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::writeErrorsDevicePage::totalCorrectedErrors:
        driveWriteErrorStats["mountTotalCorrectedWriteErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::writeErrorsDevicePage::totalProcessed:
        driveWriteErrorStats["mountTotalWriteBytesProcessed"] = logPageParam.getU64Value(); // already in bytes
        break;
      case SCSI::writeErrorsDevicePage::totalUncorrectedErrors:
        driveWriteErrorStats["mountTotalUncorrectedWriteErrors"] = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return driveWriteErrorStats;
}

std::map<std::string,uint64_t> drive::DriveIBM3592::getTapeReadErrors() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint64_t> driveReadErrorStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::readErrors;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveIBM3592::getTapeReadErrors");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getTapeReadErrors");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

  while (logParameter < endPage) { /* values in KiBs and we use shift <<10 to get bytes */
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::readErrorsDevicePage::totalCorrectedErrors:
        driveReadErrorStats["mountTotalCorrectedReadErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::readErrorsDevicePage::totalProcessed:
        driveReadErrorStats["mountTotalReadBytesProcessed"] = logPageParam.getU64Value() << 10;
        break;
      case SCSI::readErrorsDevicePage::totalUncorrectedErrors:
        driveReadErrorStats["mountTotalUncorrectedReadErrors"] = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return driveReadErrorStats;
}

std::map<std::string,uint64_t> drive::DriveT10000::getTapeReadErrors() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint64_t> driveReadErrorStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::readErrors;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveT10000::getTapeReadErrors");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveT10000::getTapeReadErrors");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);
  while (logParameter < endPage) { /* values in KiBs and we use shift <<10 to get bytes */
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::readErrorsDevicePage::totalCorrectedErrors:
        driveReadErrorStats["mountTotalCorrectedReadErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::readErrorsDevicePage::totalProcessed:
        driveReadErrorStats["mountTotalReadBytesProcessed"] = logPageParam.getU64Value(); // already in bytes
        break;
      case SCSI::readErrorsDevicePage::totalUncorrectedErrors:
        driveReadErrorStats["mountTotalUncorrectedReadErrors"] = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return driveReadErrorStats;
}

std::map<std::string,uint32_t> drive::DriveGeneric::getTapeNonMediumErrors() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint32_t> driveNonMediumErrorsStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::nonMediumErrors;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveGeneric::getTapeNonMediumErrors");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveGeneric::getTapeNonMediumErrors");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

  while (logParameter < endPage) {
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::nonMediumErrorsDevicePage::totalCount:
        driveNonMediumErrorsStats["mountTotalNonMediumErrorCounts"] = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return driveNonMediumErrorsStats;
}

std::map<std::string,uint32_t> drive::DriveIBM3592::getVolumeStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint32_t> volumeStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::volumeStatistics;
  cdb.subPageCode = 0x00; // 0x01 // for IBM latest revision drives
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveIBM3592::getVolumeStats");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getVolumeStats");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

  while (logParameter < endPage) {
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::volumeStatisticsPage::validityFlag:
        volumeStats["validity"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeMounts:
        volumeStats["lifetimeVolumeMounts"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeRecoveredWriteDataErrors:
        volumeStats["lifetimeVolumeRecoveredWriteErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeUnrecoveredWriteDataErrors:
        volumeStats["lifetimeVolumeUnrecoveredWriteErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeRecoveredReadErrors:
        volumeStats["lifetimeVolumeRecoveredReadErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeUnrecoveredReadErrors:
        volumeStats["lifetimeVolumeUnrecoveredReadErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeManufacturingDate:
        char volumeManufacturingDate[9];
        for (int i = 0; i < 8; ++i) {
          volumeManufacturingDate[i] = logPageParam.parameterValue[i];
        }
        volumeManufacturingDate[8] = '\0';
        volumeStats["volumeManufacturingDate"] = std::atoi(volumeManufacturingDate);
        break;
      case SCSI::volumeStatisticsPage::BOTPasses:
        volumeStats["lifetimeBOTPasses"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::MOTPasses:
        volumeStats["lifetimeMOTPasses"] = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return volumeStats;
}

/*
 * This method looks very close to the IBM's implementation but Oracle has partly implemented
 * the SCSI page 0x17. This means that the missing values are defined, but have the value of 0.
 * This is the reason we provide a different method implementation for T10k drives.
 */
std::map<std::string,uint32_t> drive::DriveT10000::getVolumeStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint32_t> volumeStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::volumeStatistics;
  cdb.subPageCode = 0x00; // 0x01 // for IBM latest revision drives
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveT10000::getVolumeStats");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveT10000::getVolumeStats");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

  while (logParameter < endPage) {
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::volumeStatisticsPage::validityFlag:
        volumeStats["validity"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeMounts:
        volumeStats["lifetimeVolumeMounts"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeRecoveredWriteDataErrors:
        volumeStats["lifetimeVolumeRecoveredWriteErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeRecoveredReadErrors:
        volumeStats["lifetimeVolumeRecoveredReadErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeManufacturingDate:
        char volumeManufacturingDate[9];
        for (int i = 0; i < 8; ++i) {
          volumeManufacturingDate[i] = logPageParam.parameterValue[i];
        }
        volumeManufacturingDate[8] = '\0';
        volumeStats["volumeManufacturingDate"] = std::atoi(volumeManufacturingDate);
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return volumeStats;
}

std::map<std::string,uint32_t> drive::DriveLTO::getVolumeStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint32_t> volumeStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::volumeStatistics;
  cdb.subPageCode = 0x00; // 0x01 // for IBM latest revision drives
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveLTO::getVolumeStats");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getVolumeStats");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
                                 SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

  while (logParameter < endPage) {
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::volumeStatisticsPage::validityFlag:
        volumeStats["validity"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeMounts:
        volumeStats["lifetimeVolumeMounts"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeRecoveredWriteDataErrors:
        volumeStats["lifetimeVolumeRecoveredWriteErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeUnrecoveredWriteDataErrors:
        volumeStats["lifetimeVolumeUnrecoveredWriteErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeRecoveredReadErrors:
        volumeStats["lifetimeVolumeRecoveredReadErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeUnrecoveredReadErrors:
        volumeStats["lifetimeVolumeUnrecoveredReadErrors"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::volumeManufacturingDate:
        char volumeManufacturingDate[9];
        for (int i = 0; i < 8; ++i) {
          volumeManufacturingDate[i] = logPageParam.parameterValue[i];
        }
        volumeManufacturingDate[8] = '\0';
        volumeStats["volumeManufacturingDate"] = std::atoi(volumeManufacturingDate);
        break;
      case SCSI::volumeStatisticsPage::BOTPasses:
        volumeStats["lifetimeBOTPasses"] = logPageParam.getU64Value();
        break;
      case SCSI::volumeStatisticsPage::MOTPasses:
        volumeStats["lifetimeMOTPasses"] = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return volumeStats;
}

std::map<std::string,float> drive::DriveIBM3592::getQualityStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,float> qualityStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  // Obtain data from QualitySummarySubpage
  {
    // Lifetime values
    {
      memset(dataBuff, 0, sizeof(dataBuff));

      cdb.pageCode = SCSI::logSensePages::performanceCharacteristics;
      cdb.subPageCode = 0x80;
      cdb.PC = 0x01; // Current Cumulative Values
      SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;

      /* Manage both system error and SCSI errors. */
      cta::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveIBM3592::getQualityStats_qualitySummaryBlock");
      SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getQualityStats");

      SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
        *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

      const unsigned char *endPage = dataBuff +
        SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

      unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

      while (logParameter < endPage) {
        SCSI::Structures::logSenseParameter_t &logPageParam =
          *(SCSI::Structures::logSenseParameter_t *) logParameter;
        const int val = logPageParam.getU64Value();
        if (val != 0)
          switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
            case SCSI::performanceCharacteristicsQualitySummaryPage::driveEfficiency:
              qualityStats["lifetimeDriveEfficiencyPrct"] = 100-(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::mediaEfficiency:
              qualityStats["lifetimeMediumEfficiencyPrct"] = 100-(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::primaryInterfaceEfficiency0:
              qualityStats["lifetimeInterfaceEfficiency0Prct"] = 100-(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::primaryInterfaceEfficiency1:
              qualityStats["lifetimeInterfaceEfficiency1Prct"] = 100-(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::libraryInterfaceEfficiency:
              qualityStats["lifetimeLibraryEfficiencyPrct"] = 100-(val-1)*100/254.0;
              break;
          }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
      }
    }

    // Mount values
    {
      memset(dataBuff, 0, sizeof(dataBuff));

      cdb.pageCode = SCSI::logSensePages::performanceCharacteristics;
      cdb.subPageCode = 0x40;
      cdb.PC = 0x01; // Current Cumulative Values
      SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;

      /* Manage both system error and SCSI errors. */
      cta::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveIBM3592::getQualityStats_qualitySummaryBlock");
      SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getQualityStats");

      SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
        *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

      const unsigned char *endPage = dataBuff +
        SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

      unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

      while (logParameter < endPage) {
        SCSI::Structures::logSenseParameter_t &logPageParam =
          *(SCSI::Structures::logSenseParameter_t *) logParameter;
        const int val = logPageParam.getU64Value();
        if (val != 0)
          switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
            case SCSI::performanceCharacteristicsQualitySummaryPage::driveEfficiency:
              qualityStats["mountDriveEfficiencyPrct"] = 100-(float)(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::mediaEfficiency:
              qualityStats["mountMediumEfficiencyPrct"] = 100-(float)(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::primaryInterfaceEfficiency0:
              qualityStats["mountInterfaceEfficiency0Prct"] = 100-(float)(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::primaryInterfaceEfficiency1:
              qualityStats["mountInterfaceEfficiency1Prct"] = 100-(float)(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::libraryInterfaceEfficiency:
              qualityStats["mountLibraryEfficiencyPrct"] = 100-(float)(val-1)*100/254.0;
              break;
          }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
      }
    }
  }

  // Obtain data from HostCommandsSubpage
  {
    // lifetime values
    {
      memset(dataBuff, 0, sizeof(dataBuff));

      cdb.pageCode = SCSI::logSensePages::performanceCharacteristics;
      cdb.subPageCode = 0x91;
      cdb.PC = 0x01; // Current Cumulative Values
      SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;

      /* Manage both system error and SCSI errors. */
      cta::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveIBM3592::getQualityStats_hostCommandsBlock");
      SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getQualityStats");

      SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
        *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

      const unsigned char *endPage = dataBuff +
        SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

      unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

      while (logParameter < endPage) {
        SCSI::Structures::logSenseParameter_t &logPageParam =
          *(SCSI::Structures::logSenseParameter_t *) logParameter;
        switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
          case SCSI::performanceCharacteristicsHostCommandsPage::readPerformanceEfficiency:
            qualityStats["lifetimeReadEfficiencyPrct"] = (float)logPageParam.getU64Value()/65536;
            break;
          case SCSI::performanceCharacteristicsHostCommandsPage::writePerformanceEfficiency:
            qualityStats["lifetimeWriteEfficiencyPrct"] = (float)logPageParam.getU64Value()/65536;
            break;
        }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
      }
    }

    // mount values
    {
      memset(dataBuff, 0, sizeof(dataBuff));

      cdb.pageCode = SCSI::logSensePages::performanceCharacteristics;
      cdb.subPageCode = 0x51;
      cdb.PC = 0x01;  // Current Cumulative Values
      SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;

      /* Manage both system error and SCSI errors. */
      cta::exception::Errnum::throwOnMinusOne(m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
                                              "Failed SG_IO ioctl in DriveIBM3592::getQualityStats_hostCommandsBlock");
      SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getQualityStats");

      SCSI::Structures::logSenseLogPageHeader_t& logPageHeader = *(SCSI::Structures::logSenseLogPageHeader_t*) dataBuff;

      const unsigned char* endPage =
        dataBuff + SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

      unsigned char* logParameter = dataBuff + sizeof(logPageHeader);

      while (logParameter < endPage) {
        SCSI::Structures::logSenseParameter_t& logPageParam = *(SCSI::Structures::logSenseParameter_t*) logParameter;
        switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
          case SCSI::performanceCharacteristicsHostCommandsPage::readPerformanceEfficiency:
            qualityStats["mountReadEfficiencyPrct"] = (float) logPageParam.getU64Value() / 65536;
            break;
          case SCSI::performanceCharacteristicsHostCommandsPage::writePerformanceEfficiency:
            qualityStats["mountWriteEfficiencyPrct"] = (float) logPageParam.getU64Value() / 65536;
            break;
        }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
      }
    }
  }
  return qualityStats;
}

std::map<std::string, float> drive::DriveLTO::getQualityStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string, float> qualityStats;
  unsigned char dataBuff[1024];  //big enough to fit all the results

  // Obtain data from QualitySummarySubpage
  {
    // Lifetime values
    {
      memset(dataBuff, 0, sizeof(dataBuff));

      cdb.pageCode = SCSI::logSensePages::performanceCharacteristics;
      cdb.subPageCode = 0x80;
      cdb.PC = 0x01; // Current Cumulative Values
      SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;

      /* Manage both system error and SCSI errors. */
      cta::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveLTO::getQualityStats_qualitySummaryBlock");
      SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getQualityStats");

      SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
        *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

      const unsigned char *endPage = dataBuff +
                                     SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

      unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

      while (logParameter < endPage) {
        SCSI::Structures::logSenseParameter_t &logPageParam =
          *(SCSI::Structures::logSenseParameter_t *) logParameter;
        const int val = logPageParam.getU64Value();
        if (val != 0)
            switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
            case SCSI::performanceCharacteristicsQualitySummaryPage::driveEfficiency:
              qualityStats["lifetimeDriveEfficiencyPrct"] = 100-(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::mediaEfficiency:
              qualityStats["lifetimeMediumEfficiencyPrct"] = 100-(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::primaryInterfaceEfficiency0:
              qualityStats["lifetimeInterfaceEfficiency0Prct"] = 100-(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::primaryInterfaceEfficiency1:
              qualityStats["lifetimeInterfaceEfficiency1Prct"] = 100-(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::libraryInterfaceEfficiency:
              qualityStats["lifetimeLibraryEfficiencyPrct"] = 100-(val-1)*100/254.0;
              break;
          }
          logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
      }
    }

    // Mount values
    {
      memset(dataBuff, 0, sizeof(dataBuff));

      cdb.pageCode = SCSI::logSensePages::performanceCharacteristics;
      cdb.subPageCode = 0x40;
      cdb.PC = 0x01; // Current Cumulative Values
      SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;

      /* Manage both system error and SCSI errors. */
      cta::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveLTO::getQualityStats_qualitySummaryBlock");
      SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getQualityStats");

      SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
        *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

      const unsigned char *endPage = dataBuff +
                                     SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

      unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

      while (logParameter < endPage) {
        SCSI::Structures::logSenseParameter_t &logPageParam =
          *(SCSI::Structures::logSenseParameter_t *) logParameter;
        const int val = logPageParam.getU64Value();
        if (val != 0)
          switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
            case SCSI::performanceCharacteristicsQualitySummaryPage::driveEfficiency:
              qualityStats["mountDriveEfficiencyPrct"] = 100-(float)(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::mediaEfficiency:
              qualityStats["mountMediumEfficiencyPrct"] = 100-(float)(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::primaryInterfaceEfficiency0:
              qualityStats["mountInterfaceEfficiency0Prct"] = 100-(float)(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::primaryInterfaceEfficiency1:
              qualityStats["mountInterfaceEfficiency1Prct"] = 100-(float)(val-1)*100/254.0;
              break;
            case SCSI::performanceCharacteristicsQualitySummaryPage::libraryInterfaceEfficiency:
              qualityStats["mountLibraryEfficiencyPrct"] = 100-(float)(val-1)*100/254.0;
              break;
          }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
      }
    }
  }

  // Obtain data from HostCommandsSubpage
  {
    // lifetime values
    {
      memset(dataBuff, 0, sizeof(dataBuff));

      cdb.pageCode = SCSI::logSensePages::performanceCharacteristics;
      cdb.subPageCode = 0x91;
      cdb.PC = 0x01; // Current Cumulative Values
      SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;

      /* Manage both system error and SCSI errors. */
      cta::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveLTO::getQualityStats_hostCommandsBlock");
      SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getQualityStats");

      SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
        *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

      const unsigned char *endPage = dataBuff +
                                     SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

      unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

      while (logParameter < endPage) {
        SCSI::Structures::logSenseParameter_t &logPageParam =
          *(SCSI::Structures::logSenseParameter_t *) logParameter;
        switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
          case SCSI::performanceCharacteristicsHostCommandsPage::readPerformanceEfficiency:
            qualityStats["lifetimeReadEfficiencyPrct"] = (float)logPageParam.getU64Value()/65536;
            break;
          case SCSI::performanceCharacteristicsHostCommandsPage::writePerformanceEfficiency:
            qualityStats["lifetimeWriteEfficiencyPrct"] = (float)logPageParam.getU64Value()/65536;
            break;
        }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
      }
    }

    // mount values
    {
      memset(dataBuff, 0, sizeof(dataBuff));

      cdb.pageCode = SCSI::logSensePages::performanceCharacteristics;
      cdb.subPageCode = 0x51;
      cdb.PC = 0x01; // Current Cumulative Values
      SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

      sgh.setCDB(&cdb);
      sgh.setDataBuffer(&dataBuff);
      sgh.setSenseBuffer(&senseBuff);
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;

      /* Manage both system error and SCSI errors. */
      cta::exception::Errnum::throwOnMinusOne(
        m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
        "Failed SG_IO ioctl in DriveLTO::getQualityStats_hostCommandsBlock");
      SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getQualityStats");

      SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
        *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

      const unsigned char *endPage = dataBuff +
                                     SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

      unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

      while (logParameter < endPage) {
        SCSI::Structures::logSenseParameter_t &logPageParam =
          *(SCSI::Structures::logSenseParameter_t *) logParameter;
        switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
          case SCSI::performanceCharacteristicsHostCommandsPage::readPerformanceEfficiency:
            qualityStats["mountReadEfficiencyPrct"] = (float)logPageParam.getU64Value()/65536;
            break;
          case SCSI::performanceCharacteristicsHostCommandsPage::writePerformanceEfficiency:
            qualityStats["mountWriteEfficiencyPrct"] = (float)logPageParam.getU64Value()/65536;
            break;
        }
        logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
      }
    }
  }
  return qualityStats;
}

std::map<std::string,float> drive::DriveT10000::getQualityStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,float> qualityStats;
  unsigned char dataBuff[4096]; //big enough to fit all the results
  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::vendorUniqueDriveStatistics;
  cdb.subPageCode = 0x00;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveT10000::getQualityStats");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveT10000::getQualityStats");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

  while (logParameter < endPage) {
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::vendorUniqueDriveStatistics::readQualityIndex:
        qualityStats["mountReadEfficiencyPrct"] = logPageParam.getU64Value()/160.0;
        break;
      case SCSI::vendorUniqueDriveStatistics::writeEfficiency:
        qualityStats["mountWriteEfficiencyPrct"] = logPageParam.getU64Value()/10.0;
        break;
      case SCSI::vendorUniqueDriveStatistics::tapeEfficiency:
        qualityStats["lifetimeMediumEfficiencyPrct"] = logPageParam.getU64Value()/10.0;
        break;
      case SCSI::vendorUniqueDriveStatistics::readBackCheckQualityIndex:
        qualityStats["mountReadBackCheckQualityIndexPrct"] = logPageParam.getU64Value()/160.0;
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return qualityStats;
}

std::map<std::string,uint32_t> drive::DriveIBM3592::getDriveStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint32_t> driveStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  // write errors (0x34)
  {
    memset(dataBuff, 0, sizeof(dataBuff));

    cdb.pageCode = SCSI::logSensePages::driveWriteErrors;
    cdb.PC = 0x01; // Current Cumulative Values
    SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&dataBuff);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveIBM3592::getDriveStats_writeErrors");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getDriveStats");

    SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

    const unsigned char *endPage = dataBuff +
      SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

    unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

    while (logParameter < endPage) {
      SCSI::Structures::logSenseParameter_t &logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;
      switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
        case SCSI::driveWriteErrorsPage::dataTemps:
          driveStats["mountTemps"] = logPageParam.getU64Value();
          break;
        case SCSI::driveWriteErrorsPage::servoTemps:
          driveStats["mountServoTemps"] = logPageParam.getU64Value();
          break;
        case SCSI::driveWriteErrorsPage::servoTransients:
          driveStats["mountServoTransients"] = logPageParam.getU64Value();
          break;
        case SCSI::driveWriteErrorsPage::dataTransients:
          driveStats["mountWriteTransients"] = logPageParam.getU64Value();
          break;
        case SCSI::driveWriteErrorsPage::totalRetries:
          driveStats["mountTotalWriteRetries"] = logPageParam.getU64Value();
          break;
      }
      logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
    }
  }

  // read FW errors
  {
    memset(dataBuff, 0, sizeof(dataBuff));

    cdb.pageCode = SCSI::logSensePages::driveReadForwardErrors;
    cdb.PC = 0x01; // Current Cumulative Values
    SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&dataBuff);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveIBM3592::getDriveStats_readFWErrors");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getDriveStats");

    SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

    const unsigned char *endPage = dataBuff +
      SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

    unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

    while (logParameter < endPage) {
      SCSI::Structures::logSenseParameter_t &logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;
      switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
        case SCSI::driveReadErrorsPage::dataTemps:
          driveStats["mountTemps"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::servoTemps:
          driveStats["mountServoTemps"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::servoTransients:
          driveStats["mountServoTransients"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::dataTransients:
          driveStats["mountReadTransients"] = logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::totalRetries:
          driveStats["mountTotalReadRetries"] = logPageParam.getU64Value();
          break;
      }
      logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
    }
  }

  // read BW errors
  {
    memset(dataBuff, 0, sizeof(dataBuff));

    cdb.pageCode = SCSI::logSensePages::driveReadBackwardErrors;
    cdb.PC = 0x01; // Current Cumulative Values
    SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&dataBuff);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveIBM3592::getDriveStats_readBWErrors");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveIBM3592::getDriveStats");

    SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

    const unsigned char *endPage = dataBuff +
      SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

    unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

    while (logParameter < endPage) {
      SCSI::Structures::logSenseParameter_t &logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;
      switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
        case SCSI::driveReadErrorsPage::dataTemps:
          driveStats["mountTemps"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::servoTemps:
          driveStats["mountServoTemps"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::servoTransients:
          driveStats["mountServoTransients"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::dataTransients:
          driveStats["mountReadTransients"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::totalRetries:
          driveStats["mountTotalReadRetries"] += logPageParam.getU64Value();
          break;
      }
      logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
    }
  }
  return driveStats;
}

std::map<std::string,uint32_t> drive::DriveLTO::getDriveStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint32_t> driveStats;
  unsigned char dataBuff[1024]; //big enough to fit all the results

  // write errors (0x34)
  {
    memset(dataBuff, 0, sizeof(dataBuff));

    cdb.pageCode = SCSI::logSensePages::driveWriteErrors;
    cdb.PC = 0x01; // Current Cumulative Values
    SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&dataBuff);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveLTO::getDriveStats_writeErrors");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getDriveStats");

    SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

    const unsigned char *endPage = dataBuff +
                                   SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

    unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

    while (logParameter < endPage) {
      SCSI::Structures::logSenseParameter_t &logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;
      switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
        case SCSI::driveWriteErrorsPage::dataTemps:
          driveStats["mountTemps"] = logPageParam.getU64Value();
          break;
        case SCSI::driveWriteErrorsPage::servoTemps:
          driveStats["mountServoTemps"] = logPageParam.getU64Value();
          break;
        case SCSI::driveWriteErrorsPage::servoTransients:
          driveStats["mountServoTransients"] = logPageParam.getU64Value();
          break;
        case SCSI::driveWriteErrorsPage::dataTransients:
          driveStats["mountWriteTransients"] = logPageParam.getU64Value();
          break;
        case SCSI::driveWriteErrorsPage::totalRetries:
          driveStats["mountTotalWriteRetries"] = logPageParam.getU64Value();
          break;
      }
      logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
    }
  }

  // read FW errors
  {
    memset(dataBuff, 0, sizeof(dataBuff));

    cdb.pageCode = SCSI::logSensePages::driveReadForwardErrors;
    cdb.PC = 0x01; // Current Cumulative Values
    SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&dataBuff);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveLTO::getDriveStats_readFWErrors");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getDriveStats");

    SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

    const unsigned char *endPage = dataBuff +
                                   SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

    unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

    while (logParameter < endPage) {
      SCSI::Structures::logSenseParameter_t &logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;
      switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
        case SCSI::driveReadErrorsPage::dataTemps:
          driveStats["mountTemps"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::servoTemps:
          driveStats["mountServoTemps"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::servoTransients:
          driveStats["mountServoTransients"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::dataTransients:
          driveStats["mountReadTransients"] = logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::totalRetries:
          driveStats["mountTotalReadRetries"] = logPageParam.getU64Value();
          break;
      }
      logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
    }
  }

  // read BW errors
  {
    memset(dataBuff, 0, sizeof(dataBuff));

    cdb.pageCode = SCSI::logSensePages::driveReadBackwardErrors;
    cdb.PC = 0x01; // Current Cumulative Values
    SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

    sgh.setCDB(&cdb);
    sgh.setDataBuffer(&dataBuff);
    sgh.setSenseBuffer(&senseBuff);
    sgh.dxfer_direction = SG_DXFER_FROM_DEV;

    /* Manage both system error and SCSI errors. */
    cta::exception::Errnum::throwOnMinusOne(
      m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
      "Failed SG_IO ioctl in DriveLTO::getDriveStats_readBWErrors");
    SCSI::ExceptionLauncher(sgh, "SCSI error in DriveLTO::getDriveStats");

    SCSI::Structures::logSenseLogPageHeader_t &logPageHeader =
      *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

    const unsigned char *endPage = dataBuff +
                                   SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

    unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

    while (logParameter < endPage) {
      SCSI::Structures::logSenseParameter_t &logPageParam =
        *(SCSI::Structures::logSenseParameter_t *) logParameter;
      switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
        case SCSI::driveReadErrorsPage::dataTemps:
          driveStats["mountTemps"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::servoTemps:
          driveStats["mountServoTemps"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::servoTransients:
          driveStats["mountServoTransients"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::dataTransients:
          driveStats["mountReadTransients"] += logPageParam.getU64Value();
          break;
        case SCSI::driveReadErrorsPage::totalRetries:
          driveStats["mountTotalReadRetries"] += logPageParam.getU64Value();
          break;
      }
      logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
    }
  }
  return driveStats;
}

std::map<std::string,uint32_t> drive::DriveT10000::getDriveStats() {
  SCSI::Structures::LinuxSGIO_t sgh;
  SCSI::Structures::logSenseCDB_t cdb;
  SCSI::Structures::senseData_t<255> senseBuff;
  std::map<std::string,uint32_t> driveStats;
  unsigned char dataBuff[4096]; //big enough to fit all the results
  memset(dataBuff, 0, sizeof (dataBuff));

  cdb.pageCode = SCSI::logSensePages::vendorUniqueDriveStatistics;
  cdb.PC = 0x01; // Current Cumulative Values
  SCSI::Structures::setU16(cdb.allocationLength, sizeof(dataBuff));

  sgh.setCDB(&cdb);
  sgh.setDataBuffer(&dataBuff);
  sgh.setSenseBuffer(&senseBuff);
  sgh.dxfer_direction = SG_DXFER_FROM_DEV;

  /* Manage both system error and SCSI errors. */
  cta::exception::Errnum::throwOnMinusOne(
    m_sysWrapper.ioctl(this->m_tapeFD, SG_IO, &sgh),
    "Failed SG_IO ioctl in DriveT10000::getDriveStats");
  SCSI::ExceptionLauncher(sgh, "SCSI error in DriveT10000::getDriveStats");

  SCSI::Structures::logSenseLogPageHeader_t & logPageHeader =
    *(SCSI::Structures::logSenseLogPageHeader_t *) dataBuff;

  const unsigned char *endPage = dataBuff +
    SCSI::Structures::toU16(logPageHeader.pageLength) + sizeof(logPageHeader);

  unsigned char *logParameter = dataBuff + sizeof(logPageHeader);

  while (logParameter < endPage) {
    SCSI::Structures::logSenseParameter_t & logPageParam =
      *(SCSI::Structures::logSenseParameter_t *) logParameter;
    switch (SCSI::Structures::toU16(logPageParam.header.parameterCode)) {
      case SCSI::vendorUniqueDriveStatistics::readRecoveryRetries:
        driveStats["mountTotalReadRetries"] = logPageParam.getU64Value();
        break;
      case SCSI::vendorUniqueDriveStatistics::readTransientConditions:
        driveStats["mountReadTransients"] = logPageParam.getU64Value();
        break;
      case SCSI::vendorUniqueDriveStatistics::writeTransientConditions:
        driveStats["mountWriteTransients"] = logPageParam.getU64Value();
        break;
      case SCSI::vendorUniqueDriveStatistics::servoTepomporaries:
        driveStats["mountServoTemps"] = logPageParam.getU64Value();
        break;
      case SCSI::vendorUniqueDriveStatistics::servoTransientConditions:
        driveStats["mountServoTransients"] = logPageParam.getU64Value();
        break;
      case SCSI::vendorUniqueDriveStatistics::writeRecoveryRetries:
        driveStats["mountTotalWriteRetries"] = logPageParam.getU64Value();
        break;
      case SCSI::vendorUniqueDriveStatistics::temporaryDriveErrors:
        driveStats["mountTemps"] = logPageParam.getU64Value();
        break;
    }
    logParameter += logPageParam.header.parameterLength + sizeof(logPageParam.header);
  }
  return driveStats;
}

std::string drive::DriveGeneric::getDriveFirmwareVersion() {
  return this->getDeviceInfo().productRevisionLevel;
}

/*
 * Override as not implemented of all SCSI metrics functions for MHVTL virtual drives as SCSI log sense pages
 * are not implemented as vendor(Oracle) specific, but inherits from DriveT10000.
 */
std::map<std::string,uint64_t> drive::DriveMHVTL::getTapeWriteErrors() {
  // No available data
  return std::map<std::string,uint64_t>();
}

std::map<std::string,uint64_t> drive::DriveMHVTL::getTapeReadErrors() {
  // No available data
  return std::map<std::string,uint64_t>();
}

std::map<std::string,uint32_t> drive::DriveMHVTL::getTapeNonMediumErrors() {
  // No available data
  return std::map<std::string,uint32_t>();
}

std::map<std::string,float> drive::DriveMHVTL::getQualityStats(){
  // No available data
  return std::map<std::string,float>();
}

std::map<std::string,uint32_t> drive::DriveMHVTL::getDriveStats() {
  // No available data
  return std::map<std::string,uint32_t>();
}

std::map<std::string,uint32_t> castor::tape::tapeserver::drive::DriveMHVTL::getVolumeStats() {
  // No available data
  return std::map<std::string,uint32_t>();
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
  cta::exception::Errnum::throwOnMinusOne(
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
  cta::exception::Errnum::throwOnMinusOne(m_sysWrapper.close(m_tapeFD),
    std::string("Could not close device file: ") + m_SCSIInfo.nst_dev);
  cta::exception::Errnum::throwOnMinusOne(
    m_tapeFD = m_sysWrapper.open(m_SCSIInfo.nst_dev.c_str(), O_RDWR | O_NONBLOCK),
          std::string("Could not open device file: ") + m_SCSIInfo.nst_dev);

  struct mtget mtInfo;

  /* Read drive status */
  {
    const int ioctl_rc = m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &mtInfo);
    const int ioctl_errno = errno;
    if(-1 == ioctl_rc) {
      std::ostringstream errMsg;
      errMsg << "Could not read drive status in waitUntilReady: " << m_SCSIInfo.nst_dev;
      if(EBADF == ioctl_errno) {
        errMsg << " tapeFD=" << m_tapeFD;
      }
      throw cta::exception::Errnum(ioctl_errno, errMsg.str());
    }
  }

  if(GMT_ONLINE(mtInfo.mt_gstat)==0) {
    cta::exception::TimeOut ex;
    ex.getMessage() << "Tape drive empty after waiting " <<
      timeoutSecond << " seconds.";
    throw ex;
  }
}

void drive::DriveGeneric::waitTestUnitReady(const uint32_t timeoutSecond)  {
  std::string lastTestUnitReadyExceptionMsg("");
  cta::utils::Timer t;
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
  cta::exception::TimeOut ex;
  ex.getMessage() << "Failed to test unit ready after waiting " <<
    timeoutSecond << " seconds: " << lastTestUnitReadyExceptionMsg;
  throw ex;
}

}}}
