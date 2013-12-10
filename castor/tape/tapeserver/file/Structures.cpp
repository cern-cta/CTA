/******************************************************************************
 *                      Structures.cpp
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

#include "Structures.hpp"
#include "../exception/Exception.hpp"
#include <time.h>
#include <stdio.h>

using namespace castor::tape;

void AULFile::VOL1::fill(std::string vsn) {
  setString(label, "VOL1");
  setString(VSN, vsn);
  setString(lblStandard, "3");
  setString(ownerID, "CASTOR"); /* TODO: check do we need CASTOR's STAGERSUPERUSER */
}

void AULFile::VOL1::verify() {
  if (cmpString(label, "VOL1"))
    throw exceptions::Errnum(std::string("Failed verify for the VOL1: ") +
          AULFile::toString(label));
  if (!cmpString(VSN, ""))
    throw exceptions::Errnum(std::string("Failed verify for the VSN: ") +
          AULFile::toString(VSN));
  if (cmpString(lblStandard, "3"))
    throw exceptions::Errnum(
          std::string("Failed verify for the label standard: ") +
          AULFile::toString(lblStandard));
  if (cmpString(ownerID, "CASTOR"))
    throw exceptions::Errnum(
          std::string("Failed verify for the ownerID: ") +
          AULFile::toString(ownerID));

  /* now we verify all other fields which must be spaces */
  if (cmpString(accessibility, ""))
    throw exceptions::Errnum("accessibility is not empty");
  if (cmpString(reserved1, ""))
    throw exceptions::Errnum("reserved1 is not empty");
  if (cmpString(implID, ""))
    throw exceptions::Errnum("implID is not empty");
  if (cmpString(reserved2, ""))
    throw exceptions::Errnum("reserved2 is not empty");
}

void AULFile::HDR1EOF1::fillCommon(
  std::string _fileId, std::string _VSN, int _fSeq) {

  setString(fileId, _fileId);
  setString(VSN, _VSN);
  setInt(fSeq, _fSeq);

  // fill the predefined values
  setString(fSec, "0001");
  setString(genNum, "0001");
  setString(verNumOfGen, "00");
  setDate(creationDate);
  /**
   * TODO:   current_time += (retentd * 86400); to check do we really need
   * retend retention period in days which means a file may be overwritten only
   * if it is  expired.   Default  is  0,  which means that the file may be
   * overwritten immediately.
   */
  setDate(expirationDate);
  setString(sysCode, "CASTOR 7.7.77"); /* TODO: CASTOR BASEVERSION */
}

void AULFile::HDR1EOF1::verifyCommon()
  const throw (exceptions::Errnum) {

  if (!cmpString(fileId, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the fileId: ") +
          AULFile::toString(fileId));
  if (!cmpString(VSN, ""))
    throw exceptions::Errnum(std::string("Failed verify for the VSN: ") +
          AULFile::toString(VSN));
  if (cmpString(fSec, "0001"))
    throw exceptions::Errnum(
          std::string("Failed verify for the fSec: ") +
          AULFile::toString(fSec));
  if (!cmpString(fSeq, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the fSeq: ") +
          AULFile::toString(fSeq));
  if (cmpString(genNum, "0001"))
    throw exceptions::Errnum(
          std::string("Failed verify for the genNum: ") +
          AULFile::toString(genNum));
  if (cmpString(verNumOfGen, "00"))
    throw exceptions::Errnum(
          std::string("Failed verify for the verNumOfGen: ") +
          AULFile::toString(verNumOfGen));
  if (!cmpString(creationDate, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the creationDate: ") +
          AULFile::toString(creationDate));
  if (!cmpString(expirationDate, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the expirationDate: ") +
          AULFile::toString(expirationDate));
  if (cmpString(accessibility, ""))
    throw exceptions::Errnum("accessibility is not empty");
  if (!cmpString(sysCode, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the sysCode: ") +
          AULFile::toString(sysCode));
  if (cmpString(reserved, ""))
    throw exceptions::Errnum("reserved is not empty");
}

void AULFile::HDR1::fill(
  std::string _fileId,
  std::string _VSN,
  int _fSeq) {

  setString(label, "HDR1");
  setString(blockCount, "000000");

  fillCommon(_fileId, _VSN, _fSeq);
}

void AULFile::HDR1::verify() const throw (exceptions::Errnum) {
  if (cmpString(label, "HDR1"))
    throw exceptions::Errnum(std::string("Failed verify for the HDR1: ") +
          AULFile::toString(label));
  if (cmpString(blockCount, "000000"))
    throw exceptions::Errnum(
          std::string("Failed verify for the blockCount: ") +
          AULFile::toString(blockCount));

  verifyCommon();
}

void AULFile::HDR1PRELABEL::fill(std::string _VSN) {
  setString(label, "HDR1");
  setString(blockCount, "000000");

  fillCommon(std::string("PRELABEL"), _VSN, 1);
}

void AULFile::HDR1PRELABEL::verify()
  const throw (exceptions::Errnum) {

  if (cmpString(label, "HDR1"))
    throw exceptions::Errnum(std::string("Failed verify for the HDR1: ") +
          AULFile::toString(label));
  if (cmpString(blockCount, "000000"))
    throw exceptions::Errnum(
          std::string("Failed verify for the blockCount: ") +
          AULFile::toString(blockCount));
  if (cmpString(fileId, "PRELABEL"))
    throw exceptions::Errnum(
          std::string("Failed verify for the PRELABEL: ") +
          AULFile::toString(fileId));

  verifyCommon();
}

void AULFile::EOF1::fill(
  std::string _fileId, std::string _VSN, int _fSeq, int _blockCount) {

  setString(label, "EOF1");
  setInt(blockCount, _blockCount);

  fillCommon(_fileId, _VSN, _fSeq);
}

void AULFile::EOF1::verify() const throw (exceptions::Errnum) {
  if (cmpString(label, "EOF1"))
    throw exceptions::Errnum(std::string("Failed verify for the EOF1: ") +
          AULFile::toString(label));
  if (!cmpString(blockCount, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the blockCount: ") +
          AULFile::toString(blockCount));

  verifyCommon();
}

void AULFile::HDR2EOF2::fillCommon(int _blockLength, bool driveHasCompression) {
  setString(recordFormat, "F");
  if (_blockLength < 100000) {
    setInt(blockLength, _blockLength);
    setInt(recordLength, _blockLength);
  } else {
    setInt(blockLength, 0);
    setInt(recordLength, 0);
  }
  if (driveHasCompression) setString(recTechnique,"P ");
  setString(aulId, "00");
}

void AULFile::HDR2EOF2::verifyCommon() 
  const throw (exceptions::Errnum) {

  if (cmpString(recordFormat, "F"))
    throw exceptions::Errnum(
          std::string("Failed verify for the recordFormat: ") +
          AULFile::toString(recordFormat));
  if (!cmpString(blockLength, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the blockLength: ") +
          AULFile::toString(blockLength));
  if (!cmpString(recordLength, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the recordLength: ") +
          AULFile::toString(recordLength));
  if (cmpString(aulId, "00"))
    throw exceptions::Errnum(
          std::string("Failed verify for the aulId: ") +
          AULFile::toString(aulId));
  if (cmpString(reserved1, ""))
    throw exceptions::Errnum("reserved1 is not empty");
  if (cmpString(reserved2, ""))
    throw exceptions::Errnum("reserved2 is not empty");
  if (cmpString(reserved3, ""))
    throw exceptions::Errnum("reserved2 is not empty");
}

void AULFile::HDR2::fill(int _blockLength, bool driveHasCompression) {
  setString(label, "HDR2");
  
  fillCommon(_blockLength, driveHasCompression);
}
void AULFile::HDR2::verify() const throw (exceptions::Errnum) {
  if (cmpString(label, "HDR2"))
    throw exceptions::Errnum(std::string("Failed verify for the HDR2: ") +
          AULFile::toString(label));

  verifyCommon();
}

void AULFile::EOF2::fill(int _blockLength, bool driveHasCompression) {
  setString(label, "EOF2");

  fillCommon(_blockLength, driveHasCompression);
}

void AULFile::EOF2::verify() const throw (exceptions::Errnum) {
  if (cmpString(label, "EOF2"))
    throw exceptions::Errnum(std::string("Failed verify for the EOF2: ") +
          AULFile::toString(label));

  verifyCommon();
}

void AULFile::UHL1UTL1::fillCommon(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  drives::deviceInfo deviceInfo) {

  setInt(actualfSeq, fSeq);
  setInt(actualBlockSize, blockSize);
  setInt(actualRecordLength, blockSize);
  setString(site, siteName);
  setString(moverHost, hostName);
  setString(driveVendor, deviceInfo.vendor);
  setString(driveModel, deviceInfo.product);
  setString(serialNumber, deviceInfo.serialNumber);
}

void AULFile::UHL1UTL1::verifyCommon() 
  const throw (exceptions::Errnum){  

  if (!cmpString(actualfSeq, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the actualfSeq: ") +
          AULFile::toString(actualfSeq));
  if (!cmpString(actualBlockSize, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the actualBlockSize: ") +
          AULFile::toString(actualBlockSize));
  if (!cmpString(actualRecordLength, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for the actualRecordLength: ") +
          AULFile::toString(actualRecordLength));
  if (!cmpString(site, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for site: ") +
          AULFile::toString(site));
  if (!cmpString(moverHost, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for moverHost: ") +
          AULFile::toString(moverHost));
  if (!cmpString(driveVendor, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for driveVendor: ") +
          AULFile::toString(driveVendor));
  if (!cmpString(driveModel, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for driveModel: ") +
          AULFile::toString(driveModel));
  if (!cmpString(serialNumber, ""))
    throw exceptions::Errnum(
          std::string("Failed verify for serialNumber: ") +
          AULFile::toString(serialNumber));
}

void AULFile::UHL1::fill(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  drives::deviceInfo deviceInfo) {

  setString(label, "UHL1");

  fillCommon(fSeq, blockSize, siteName, hostName, deviceInfo);
}

void AULFile::UHL1::verify() const throw (exceptions::Errnum) {
  if (cmpString(label, "UHL1"))
    throw exceptions::Errnum(std::string("Failed verify for the UHL1: ") +
          AULFile::toString(label));

  verifyCommon();
}

void AULFile::UTL1::fill(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  drives::deviceInfo deviceInfo) {

  setString(label, "UTL1");

  fillCommon(fSeq, blockSize, siteName, hostName, deviceInfo);
}

void AULFile::UTL1::verify() const throw (exceptions::Errnum) {
  if (cmpString(label, "UTL1"))
    throw exceptions::Errnum(std::string("Failed verify for the UTL1: ") +
          AULFile::toString(label));

  verifyCommon();
}

