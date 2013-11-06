// ----------------------------------------------------------------------
// File: File/Structures.cc
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

#include "Structures.hh"
#include "../Exception/Exception.hh"
#include <time.h>
#include <stdio.h>

void Tape::AULFile::VOL1::fill(std::string vsn) {
  setString(label, "VOL1");
  setString(VSN, vsn);
  setString(lblStandard, "3");
  setString(ownerID, "CASTOR"); /* TODO: check do we need CASTOR's STAGERSUPERUSER */
}

void Tape::AULFile::VOL1::verify() {
  if (cmpString(label, "VOL1"))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the VOL1: ") +
          Tape::AULFile::toString(label));
  if (!cmpString(VSN, ""))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the VSN: ") +
          Tape::AULFile::toString(VSN));
  if (cmpString(lblStandard, "3"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the label standard: ") +
          Tape::AULFile::toString(lblStandard));
  if (cmpString(ownerID, "CASTOR"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the ownerID: ") +
          Tape::AULFile::toString(ownerID));

  /* now we verify all other fields which must be spaces */
  if (cmpString(accessibility, ""))
    throw Tape::Exceptions::Errnum("accessibility is not empty");
  if (cmpString(reserved1, ""))
    throw Tape::Exceptions::Errnum("reserved1 is not empty");
  if (cmpString(implID, ""))
    throw Tape::Exceptions::Errnum("implID is not empty");
  if (cmpString(reserved2, ""))
    throw Tape::Exceptions::Errnum("reserved2 is not empty");
}

void Tape::AULFile::HDR1EOF1::fillCommon(
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

void Tape::AULFile::HDR1EOF1::verifyCommon()
  const throw (Tape::Exceptions::Errnum) {

  if (!cmpString(fileId, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the fileId: ") +
          Tape::AULFile::toString(fileId));
  if (!cmpString(VSN, ""))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the VSN: ") +
          Tape::AULFile::toString(VSN));
  if (cmpString(fSec, "0001"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the fSec: ") +
          Tape::AULFile::toString(fSec));
  if (!cmpString(fSeq, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the fSeq: ") +
          Tape::AULFile::toString(fSeq));
  if (cmpString(genNum, "0001"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the genNum: ") +
          Tape::AULFile::toString(genNum));
  if (cmpString(verNumOfGen, "00"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the verNumOfGen: ") +
          Tape::AULFile::toString(verNumOfGen));
  if (!cmpString(creationDate, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the creationDate: ") +
          Tape::AULFile::toString(creationDate));
  if (!cmpString(expirationDate, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the expirationDate: ") +
          Tape::AULFile::toString(expirationDate));
  if (cmpString(accessibility, ""))
    throw Tape::Exceptions::Errnum("accessibility is not empty");
  if (!cmpString(sysCode, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the sysCode: ") +
          Tape::AULFile::toString(sysCode));
  if (cmpString(reserved, ""))
    throw Tape::Exceptions::Errnum("reserved is not empty");
}

void Tape::AULFile::HDR1::fill(
  std::string _fileId,
  std::string _VSN,
  int _fSeq) {

  setString(label, "HDR1");
  setString(blockCount, "000000");

  fillCommon(_fileId, _VSN, _fSeq);
}

void Tape::AULFile::HDR1::verify() const throw (Tape::Exceptions::Errnum) {
  if (cmpString(label, "HDR1"))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the HDR1: ") +
          Tape::AULFile::toString(label));
  if (cmpString(blockCount, "000000"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the blockCount: ") +
          Tape::AULFile::toString(blockCount));

  verifyCommon();
}

void Tape::AULFile::HDR1PRELABEL::fill(std::string _VSN) {
  setString(label, "HDR1");
  setString(blockCount, "000000");

  fillCommon(std::string("PRELABEL"), _VSN, 1);
}

void Tape::AULFile::HDR1PRELABEL::verify()
  const throw (Tape::Exceptions::Errnum) {

  if (cmpString(label, "HDR1"))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the HDR1: ") +
          Tape::AULFile::toString(label));
  if (cmpString(blockCount, "000000"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the blockCount: ") +
          Tape::AULFile::toString(blockCount));
  if (cmpString(fileId, "PRELABEL"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the PRELABEL: ") +
          Tape::AULFile::toString(fileId));

  verifyCommon();
}

void Tape::AULFile::EOF1::fill(
  std::string _fileId, std::string _VSN, int _fSeq, int _blockCount) {

  setString(label, "EOF1");
  setInt(blockCount, _blockCount);

  fillCommon(_fileId, _VSN, _fSeq);
}

void Tape::AULFile::EOF1::verify() const throw (Tape::Exceptions::Errnum) {
  if (cmpString(label, "EOF1"))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the EOF1: ") +
          Tape::AULFile::toString(label));
  if (!cmpString(blockCount, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the blockCount: ") +
          Tape::AULFile::toString(blockCount));

  verifyCommon();
}

void Tape::AULFile::HDR2EOF2::fillCommon(int _blockLength, bool driveHasCompression) {
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

void Tape::AULFile::HDR2EOF2::verifyCommon() 
  const throw (Tape::Exceptions::Errnum) {

  if (cmpString(recordFormat, "F"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the recordFormat: ") +
          Tape::AULFile::toString(recordFormat));
  if (!cmpString(blockLength, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the blockLength: ") +
          Tape::AULFile::toString(blockLength));
  if (!cmpString(recordLength, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the recordLength: ") +
          Tape::AULFile::toString(recordLength));
  if (cmpString(aulId, "00"))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the aulId: ") +
          Tape::AULFile::toString(aulId));
  if (cmpString(reserved1, ""))
    throw Tape::Exceptions::Errnum("reserved1 is not empty");
  if (cmpString(reserved2, ""))
    throw Tape::Exceptions::Errnum("reserved2 is not empty");
  if (cmpString(reserved3, ""))
    throw Tape::Exceptions::Errnum("reserved2 is not empty");
}

void Tape::AULFile::HDR2::fill(int _blockLength, bool driveHasCompression) {
  setString(label, "HDR2");
  
  fillCommon(_blockLength, driveHasCompression);
}
void Tape::AULFile::HDR2::verify() const throw (Tape::Exceptions::Errnum) {
  if (cmpString(label, "HDR2"))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the HDR2: ") +
          Tape::AULFile::toString(label));

  verifyCommon();
}

void Tape::AULFile::EOF2::fill(int _blockLength, bool driveHasCompression) {
  setString(label, "EOF2");

  fillCommon(_blockLength, driveHasCompression);
}

void Tape::AULFile::EOF2::verify() const throw (Tape::Exceptions::Errnum) {
  if (cmpString(label, "EOF2"))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the EOF2: ") +
          Tape::AULFile::toString(label));

  verifyCommon();
}

void Tape::AULFile::UHL1UTL1::fillCommon(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  Tape::deviceInfo deviceInfo) {

  setInt(actualfSeq, fSeq);
  setInt(actualBlockSize, blockSize);
  setInt(actualRecordLength, blockSize);
  setString(site, siteName);
  setString(moverHost, hostName);
  setString(driveVendor, deviceInfo.vendor);
  setString(driveModel, deviceInfo.product);
  setString(serialNumber, deviceInfo.serialNumber);
}

void Tape::AULFile::UHL1UTL1::verifyCommon() 
  const throw (Tape::Exceptions::Errnum){  

  if (!cmpString(actualfSeq, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the actualfSeq: ") +
          Tape::AULFile::toString(actualfSeq));
  if (!cmpString(actualBlockSize, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the actualBlockSize: ") +
          Tape::AULFile::toString(actualBlockSize));
  if (!cmpString(actualRecordLength, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for the actualRecordLength: ") +
          Tape::AULFile::toString(actualRecordLength));
  if (!cmpString(site, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for site: ") +
          Tape::AULFile::toString(site));
  if (!cmpString(moverHost, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for moverHost: ") +
          Tape::AULFile::toString(moverHost));
  if (!cmpString(driveVendor, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for driveVendor: ") +
          Tape::AULFile::toString(driveVendor));
  if (!cmpString(driveModel, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for driveModel: ") +
          Tape::AULFile::toString(driveModel));
  if (!cmpString(serialNumber, ""))
    throw Tape::Exceptions::Errnum(
          std::string("Failed verify for serialNumber: ") +
          Tape::AULFile::toString(serialNumber));
}

void Tape::AULFile::UHL1::fill(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  Tape::deviceInfo deviceInfo) {

  setString(label, "UHL1");

  fillCommon(fSeq, blockSize, siteName, hostName, deviceInfo);
}

void Tape::AULFile::UHL1::verify() const throw (Tape::Exceptions::Errnum) {
  if (cmpString(label, "UHL1"))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the UHL1: ") +
          Tape::AULFile::toString(label));

  verifyCommon();
}

void Tape::AULFile::UTL1::fill(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  Tape::deviceInfo deviceInfo) {

  setString(label, "UTL1");

  fillCommon(fSeq, blockSize, siteName, hostName, deviceInfo);
}

void Tape::AULFile::UTL1::verify() const throw (Tape::Exceptions::Errnum) {
  if (cmpString(label, "UTL1"))
    throw Tape::Exceptions::Errnum(std::string("Failed verify for the UTL1: ") +
          Tape::AULFile::toString(label));

  verifyCommon();
}

