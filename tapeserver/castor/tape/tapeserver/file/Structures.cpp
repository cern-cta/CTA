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

#include "castor/tape/tapeserver/file/Structures.hpp"
#include "common/exception/Exception.hpp"
#include <time.h>
#include <stdio.h>

using namespace castor::tape;

#define BASEVERSION "2.1.15"

void tapeFile::VOL1::fill(std::string VSN, 
  unsigned char LBPMethod) {
  setString(m_label, "VOL1");
  setString(m_VSN, VSN);
  setString(m_lblStandard, "3");
  setString(m_ownerID, "CASTOR");
  std::stringstream hexLBP; 
  hexLBP << std::setfill('0') << std::setw(2) << std::hex
      << std::noshowbase << static_cast<int>(LBPMethod);
  setString(m_LBPMethod, hexLBP.str());
}

void tapeFile::VOL1::verify() {
  if (cmpString(m_label, "VOL1"))
    throw cta::exception::Exception(std::string("Failed verify for the VOL1: ") +
          tapeFile::toString(m_label));
  if (!cmpString(m_VSN, ""))
    throw cta::exception::Exception(std::string("Failed verify for the VSN: ") +
          tapeFile::toString(m_VSN));
  const char *const expectedLblStandard = "3";
  if (cmpString(m_lblStandard, expectedLblStandard))
    throw cta::exception::Exception(
          std::string("Failed verify for the label standard: expected=") +
          expectedLblStandard + " actual=" +
          tapeFile::toString(m_lblStandard));

  /* now we verify all other fields which must be spaces */
  if (cmpString(m_accessibility, ""))
    throw cta::exception::Exception("accessibility is not empty");
  if (cmpString(m_reserved1, ""))
    throw cta::exception::Exception("reserved1 is not empty");
  if (cmpString(m_implID, ""))
    throw cta::exception::Exception("implID is not empty");
  if (cmpString(m_reserved2, ""))
    throw cta::exception::Exception("reserved2 is not empty");
}

void tapeFile::HDR1EOF1::fillCommon(
  std::string fileId, std::string VSN, int fSeq) {

  setString(m_fileId, fileId);
  setString(m_VSN, VSN);
  setInt(m_fSeq, fSeq);

  // fill the predefined values
  setString(m_fSec, "0001");
  setString(m_genNum, "0001");
  setString(m_verNumOfGen, "00");
  setDate(m_creationDate);
  /**
   * TODO:   current_time += (retentd * 86400); to check do we really need
   * retend retention period in days which means a file may be overwritten only
   * if it is  expired.   Default  is  0,  which means that the file may be
   * overwritten immediately.
   */
  setDate(m_expirationDate);
  setString(m_sysCode, std::string("CASTOR ") + BASEVERSION); /* TODO: CASTOR BASEVERSION */
}

void tapeFile::HDR1EOF1::verifyCommon()
  const  {

  if (!cmpString(m_fileId, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the fileId: ") +
          tapeFile::toString(m_fileId));
  if (!cmpString(m_VSN, ""))
    throw cta::exception::Exception(std::string("Failed verify for the VSN: ") +
          tapeFile::toString(m_VSN));
  if (cmpString(m_fSec, "0001"))
    throw cta::exception::Exception(
          std::string("Failed verify for the fSec: ") +
          tapeFile::toString(m_fSec));
  if (!cmpString(m_fSeq, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the fSeq: ") +
          tapeFile::toString(m_fSeq));
  if (cmpString(m_genNum, "0001"))
    throw cta::exception::Exception(
          std::string("Failed verify for the genNum: ") +
          tapeFile::toString(m_genNum));
  if (cmpString(m_verNumOfGen, "00"))
    throw cta::exception::Exception(
          std::string("Failed verify for the verNumOfGen: ") +
          tapeFile::toString(m_verNumOfGen));
  if (!cmpString(m_creationDate, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the creationDate: ") +
          tapeFile::toString(m_creationDate));
  if (!cmpString(m_expirationDate, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the expirationDate: ") +
          tapeFile::toString(m_expirationDate));
  if (cmpString(m_accessibility, ""))
    throw cta::exception::Exception("accessibility is not empty");
  if (!cmpString(m_sysCode, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the sysCode: ") +
          tapeFile::toString(m_sysCode));
  if (cmpString(m_reserved, ""))
    throw cta::exception::Exception("reserved is not empty");
}

void tapeFile::HDR1::fill(
  std::string fileId,
  std::string VSN,
  int fSeq) {

  setString(m_label, "HDR1");
  setString(m_blockCount, "000000");

  fillCommon(fileId, VSN, fSeq);
}

void tapeFile::HDR1::verify() const  {
  if (cmpString(m_label, "HDR1"))
    throw cta::exception::Exception(std::string("Failed verify for the HDR1: ") +
          tapeFile::toString(m_label));
  if (cmpString(m_blockCount, "000000"))
    throw cta::exception::Exception(std::string("Failed verify for the blockCount: ") +
          tapeFile::toString(m_blockCount));

  verifyCommon();
}

void tapeFile::HDR1PRELABEL::fill(std::string VSN) {
  setString(m_label, "HDR1");
  setString(m_blockCount, "000000");

  fillCommon(std::string("PRELABEL"), VSN, 1);
}

void tapeFile::HDR1PRELABEL::verify()
  const  {

  if (cmpString(m_label, "HDR1"))
    throw cta::exception::Exception(std::string("Failed verify for the HDR1: ") +
          tapeFile::toString(m_label));
  if (cmpString(m_blockCount, "000000"))
    throw cta::exception::Exception(
          std::string("Failed verify for the blockCount: ") +
          tapeFile::toString(m_blockCount));
  if (cmpString(m_fileId, "PRELABEL"))
    throw cta::exception::Exception(
          std::string("Failed verify for the PRELABEL: ") +
          tapeFile::toString(m_fileId));

  verifyCommon();
}

void tapeFile::EOF1::fill(
  std::string fileId, std::string VSN, int fSeq, int blockCount) {

  setString(m_label, "EOF1");
  setInt(m_blockCount, blockCount);

  fillCommon(fileId, VSN, fSeq);
}

void tapeFile::EOF1::verify() const  {
  if (cmpString(m_label, "EOF1"))
    throw cta::exception::Exception(std::string("Failed verify for the EOF1: ") +
          tapeFile::toString(m_label));
  if (!cmpString(m_blockCount, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the blockCount: ") +
          tapeFile::toString(m_blockCount));

  verifyCommon();
}

void tapeFile::HDR2EOF2::fillCommon(int blockLength, bool driveHasCompression) {
  setString(m_recordFormat, "F");
  if (blockLength < 100000) {
    setInt(m_blockLength, blockLength);
    setInt(m_recordLength, blockLength);
  } else {
    setInt(m_blockLength, 0);
    setInt(m_recordLength, 0);
  }
  if (driveHasCompression) setString(m_recTechnique,"P ");
  setString(m_aulId, "00");
}

void tapeFile::HDR2EOF2::verifyCommon() 
  const  {
  if (cmpString(m_recordFormat, "F"))
    throw cta::exception::Exception(
          std::string("Failed verify for the recordFormat: ") +
          tapeFile::toString(m_recordFormat));
  if (!cmpString(m_blockLength, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the blockLength: ") +
          tapeFile::toString(m_blockLength));
  if (!cmpString(m_recordLength, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the recordLength: ") +
          tapeFile::toString(m_recordLength));
  if (cmpString(m_aulId, "00"))
    throw cta::exception::Exception(
          std::string("Failed verify for the aulId: ") +
          tapeFile::toString(m_aulId));
  if (cmpString(m_reserved1, ""))
    throw cta::exception::Exception("reserved1 is not empty");
  if (cmpString(m_reserved2, ""))
    throw cta::exception::Exception("reserved2 is not empty");
  if (cmpString(m_reserved3, ""))
    throw cta::exception::Exception("reserved3 is not empty");
}

void tapeFile::HDR2::fill(int blockLength, bool driveHasCompression) {
  setString(m_label, "HDR2");
  
  fillCommon(blockLength, driveHasCompression);
}
void tapeFile::HDR2::verify() const  {
  if (cmpString(m_label, "HDR2"))
    throw cta::exception::Exception(std::string("Failed verify for the HDR2: ") +
          tapeFile::toString(m_label));

  verifyCommon();
}

void tapeFile::EOF2::fill(int blockLength, bool driveHasCompression) {
  setString(m_label, "EOF2");

  fillCommon(blockLength, driveHasCompression);
}

void tapeFile::EOF2::verify() const  {
  if (cmpString(m_label, "EOF2"))
    throw cta::exception::Exception(std::string("Failed verify for the EOF2: ") +
          tapeFile::toString(m_label));

  verifyCommon();
}

void tapeFile::UHL1UTL1::fillCommon(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  tapeserver::drive::deviceInfo deviceInfo) {

  setInt(m_actualfSeq, fSeq);
  setInt(m_actualBlockSize, blockSize);
  setInt(m_actualRecordLength, blockSize);
  setString(m_site, siteName);
  setString(m_moverHost, hostName);
  setString(m_driveVendor, deviceInfo.vendor);
  setString(m_driveModel, deviceInfo.product);
  setString(m_serialNumber, deviceInfo.serialNumber);
}

void tapeFile::UHL1UTL1::verifyCommon() 
  const {  
  if (!cmpString(m_actualfSeq, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the actualfSeq: ") +
          tapeFile::toString(m_actualfSeq));
  if (!cmpString(m_actualBlockSize, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the actualBlockSize: ") +
          tapeFile::toString(m_actualBlockSize));
  if (!cmpString(m_actualRecordLength, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for the actualRecordLength: ") +
          tapeFile::toString(m_actualRecordLength));
  if (!cmpString(m_site, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for site: ") +
          tapeFile::toString(m_site));
  if (!cmpString(m_moverHost, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for moverHost: ") +
          tapeFile::toString(m_moverHost));
  if (!cmpString(m_driveVendor, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for driveVendor: ") +
          tapeFile::toString(m_driveVendor));
  if (!cmpString(m_driveModel, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for driveModel: ") +
          tapeFile::toString(m_driveModel));
  if (!cmpString(m_serialNumber, ""))
    throw cta::exception::Exception(
          std::string("Failed verify for serialNumber: ") +
          tapeFile::toString(m_serialNumber));
}

void tapeFile::UHL1::fill(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  tapeserver::drive::deviceInfo deviceInfo) {
    
  setString(m_label, "UHL1");

  fillCommon(fSeq, blockSize, siteName, hostName, deviceInfo);
}

void tapeFile::UHL1::verify() const  {
  if (cmpString(m_label, "UHL1"))
    throw cta::exception::Exception(std::string("Failed verify for the UHL1: ") +
          tapeFile::toString(m_label));

  verifyCommon();
}

void tapeFile::UTL1::fill(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  tapeserver::drive::deviceInfo deviceInfo) {
    
  setString(m_label, "UTL1");

  fillCommon(fSeq, blockSize, siteName, hostName, deviceInfo);
}

void tapeFile::UTL1::verify() const  {
  if (cmpString(m_label, "UTL1"))
    throw cta::exception::Exception(std::string("Failed verify for the UTL1: ") +
          tapeFile::toString(m_label));

  verifyCommon();
}

