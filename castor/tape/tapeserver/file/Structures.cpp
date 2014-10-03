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

#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/exception/Exception.hpp"
#include "h/patchlevel.h"
#include <time.h>
#include <stdio.h>

using namespace castor::tape;

void tapeFile::VOL1::fill(std::string VSN) {
  setString(m_label, "VOL1");
  setString(m_VSN, VSN);
  setString(m_lblStandard, "3");
  setString(m_ownerID, "CASTOR"); /* TODO: check do we need CASTOR's STAGERSUPERUSER */
}

void tapeFile::VOL1::verify() {
  if (cmpString(m_label, "VOL1"))
    throw castor::exception::Exception(std::string("Failed verify for the VOL1: ") +
          tapeFile::toString(m_label));
  if (!cmpString(m_VSN, ""))
    throw castor::exception::Exception(std::string("Failed verify for the VSN: ") +
          tapeFile::toString(m_VSN));
  if (cmpString(m_lblStandard, "3"))
    throw castor::exception::Exception(
          std::string("Failed verify for the label standard: ") +
          tapeFile::toString(m_lblStandard));
  if (cmpString(m_ownerID, "CASTOR") && cmpString(m_ownerID, "stage"))
    throw castor::exception::Exception(
          std::string("Failed verify for the ownerID: ") +
          tapeFile::toString(m_ownerID));

  /* now we verify all other fields which must be spaces */
  if (cmpString(m_accessibility, ""))
    throw castor::exception::Exception("accessibility is not empty");
  if (cmpString(m_reserved1, ""))
    throw castor::exception::Exception("reserved1 is not empty");
  if (cmpString(m_implID, ""))
    throw castor::exception::Exception("implID is not empty");
  if (cmpString(m_reserved2, ""))
    throw castor::exception::Exception("reserved2 is not empty");
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
    throw castor::exception::Exception(
          std::string("Failed verify for the fileId: ") +
          tapeFile::toString(m_fileId));
  if (!cmpString(m_VSN, ""))
    throw castor::exception::Exception(std::string("Failed verify for the VSN: ") +
          tapeFile::toString(m_VSN));
  if (cmpString(m_fSec, "0001"))
    throw castor::exception::Exception(
          std::string("Failed verify for the fSec: ") +
          tapeFile::toString(m_fSec));
  if (!cmpString(m_fSeq, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for the fSeq: ") +
          tapeFile::toString(m_fSeq));
  if (cmpString(m_genNum, "0001"))
    throw castor::exception::Exception(
          std::string("Failed verify for the genNum: ") +
          tapeFile::toString(m_genNum));
  if (cmpString(m_verNumOfGen, "00"))
    throw castor::exception::Exception(
          std::string("Failed verify for the verNumOfGen: ") +
          tapeFile::toString(m_verNumOfGen));
  if (!cmpString(m_creationDate, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for the creationDate: ") +
          tapeFile::toString(m_creationDate));
  if (!cmpString(m_expirationDate, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for the expirationDate: ") +
          tapeFile::toString(m_expirationDate));
  if (cmpString(m_accessibility, ""))
    throw castor::exception::Exception("accessibility is not empty");
  if (!cmpString(m_sysCode, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for the sysCode: ") +
          tapeFile::toString(m_sysCode));
  if (cmpString(m_reserved, ""))
    throw castor::exception::Exception("reserved is not empty");
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
    throw castor::exception::Exception(std::string("Failed verify for the HDR1: ") +
          tapeFile::toString(m_label));
  if (cmpString(m_blockCount, "000000"))
    throw castor::exception::Exception(std::string("Failed verify for the blockCount: ") +
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
    throw castor::exception::Exception(std::string("Failed verify for the HDR1: ") +
          tapeFile::toString(m_label));
  if (cmpString(m_blockCount, "000000"))
    throw castor::exception::Exception(
          std::string("Failed verify for the blockCount: ") +
          tapeFile::toString(m_blockCount));
  if (cmpString(m_fileId, "PRELABEL"))
    throw castor::exception::Exception(
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
    throw castor::exception::Exception(std::string("Failed verify for the EOF1: ") +
          tapeFile::toString(m_label));
  if (!cmpString(m_blockCount, ""))
    throw castor::exception::Exception(
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
    throw castor::exception::Exception(
          std::string("Failed verify for the recordFormat: ") +
          tapeFile::toString(m_recordFormat));
  if (!cmpString(m_blockLength, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for the blockLength: ") +
          tapeFile::toString(m_blockLength));
  if (!cmpString(m_recordLength, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for the recordLength: ") +
          tapeFile::toString(m_recordLength));
  if (cmpString(m_aulId, "00"))
    throw castor::exception::Exception(
          std::string("Failed verify for the aulId: ") +
          tapeFile::toString(m_aulId));
  if (cmpString(m_reserved1, ""))
    throw castor::exception::Exception("reserved1 is not empty");
  if (cmpString(m_reserved2, ""))
    throw castor::exception::Exception("reserved2 is not empty");
  if (cmpString(m_reserved3, ""))
    throw castor::exception::Exception("reserved3 is not empty");
}

void tapeFile::HDR2::fill(int blockLength, bool driveHasCompression) {
  setString(m_label, "HDR2");
  
  fillCommon(blockLength, driveHasCompression);
}
void tapeFile::HDR2::verify() const  {
  if (cmpString(m_label, "HDR2"))
    throw castor::exception::Exception(std::string("Failed verify for the HDR2: ") +
          tapeFile::toString(m_label));

  verifyCommon();
}

void tapeFile::EOF2::fill(int blockLength, bool driveHasCompression) {
  setString(m_label, "EOF2");

  fillCommon(blockLength, driveHasCompression);
}

void tapeFile::EOF2::verify() const  {
  if (cmpString(m_label, "EOF2"))
    throw castor::exception::Exception(std::string("Failed verify for the EOF2: ") +
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
    throw castor::exception::Exception(
          std::string("Failed verify for the actualfSeq: ") +
          tapeFile::toString(m_actualfSeq));
  if (!cmpString(m_actualBlockSize, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for the actualBlockSize: ") +
          tapeFile::toString(m_actualBlockSize));
  if (!cmpString(m_actualRecordLength, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for the actualRecordLength: ") +
          tapeFile::toString(m_actualRecordLength));
  if (!cmpString(m_site, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for site: ") +
          tapeFile::toString(m_site));
  if (!cmpString(m_moverHost, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for moverHost: ") +
          tapeFile::toString(m_moverHost));
  if (!cmpString(m_driveVendor, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for driveVendor: ") +
          tapeFile::toString(m_driveVendor));
  if (!cmpString(m_driveModel, ""))
    throw castor::exception::Exception(
          std::string("Failed verify for driveModel: ") +
          tapeFile::toString(m_driveModel));
  if (!cmpString(m_serialNumber, ""))
    throw castor::exception::Exception(
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
    throw castor::exception::Exception(std::string("Failed verify for the UHL1: ") +
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
    throw castor::exception::Exception(std::string("Failed verify for the UTL1: ") +
          tapeFile::toString(m_label));

  verifyCommon();
}

