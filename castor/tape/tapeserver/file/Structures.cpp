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

#include "castor/tape/tapeserver/file/Structures.hpp"
#include "castor/exception/Exception.hpp"
#include "h/patchlevel.h"
#include <time.h>
#include <stdio.h>

using namespace castor::tape;

void tapeFile::VOL1::fill(std::string vsn) {
  setString(label, "VOL1");
  setString(VSN, vsn);
  setString(lblStandard, "3");
  setString(ownerID, "CASTOR"); /* TODO: check do we need CASTOR's STAGERSUPERUSER */
}

void tapeFile::VOL1::verify() {
  if (cmpString(label, "VOL1"))
    throw Exception(std::string("Failed verify for the VOL1: ") +
          tapeFile::toString(label));
  if (!cmpString(VSN, ""))
    throw Exception(std::string("Failed verify for the VSN: ") +
          tapeFile::toString(VSN));
  if (cmpString(lblStandard, "3"))
    throw Exception(
          std::string("Failed verify for the label standard: ") +
          tapeFile::toString(lblStandard));
  if (cmpString(ownerID, "CASTOR"))
    throw Exception(
          std::string("Failed verify for the ownerID: ") +
          tapeFile::toString(ownerID));

  /* now we verify all other fields which must be spaces */
  if (cmpString(accessibility, ""))
    throw Exception("accessibility is not empty");
  if (cmpString(reserved1, ""))
    throw Exception("reserved1 is not empty");
  if (cmpString(implID, ""))
    throw Exception("implID is not empty");
  if (cmpString(reserved2, ""))
    throw Exception("reserved2 is not empty");
}

void tapeFile::HDR1EOF1::fillCommon(
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
  setString(sysCode, std::string("CASTOR ") + BASEVERSION); /* TODO: CASTOR BASEVERSION */
}

void tapeFile::HDR1EOF1::verifyCommon()
  const throw (exceptions::Errnum) {

  if (!cmpString(fileId, ""))
    throw Exception(
          std::string("Failed verify for the fileId: ") +
          tapeFile::toString(fileId));
  if (!cmpString(VSN, ""))
    throw Exception(std::string("Failed verify for the VSN: ") +
          tapeFile::toString(VSN));
  if (cmpString(fSec, "0001"))
    throw Exception(
          std::string("Failed verify for the fSec: ") +
          tapeFile::toString(fSec));
  if (!cmpString(fSeq, ""))
    throw Exception(
          std::string("Failed verify for the fSeq: ") +
          tapeFile::toString(fSeq));
  if (cmpString(genNum, "0001"))
    throw Exception(
          std::string("Failed verify for the genNum: ") +
          tapeFile::toString(genNum));
  if (cmpString(verNumOfGen, "00"))
    throw Exception(
          std::string("Failed verify for the verNumOfGen: ") +
          tapeFile::toString(verNumOfGen));
  if (!cmpString(creationDate, ""))
    throw Exception(
          std::string("Failed verify for the creationDate: ") +
          tapeFile::toString(creationDate));
  if (!cmpString(expirationDate, ""))
    throw Exception(
          std::string("Failed verify for the expirationDate: ") +
          tapeFile::toString(expirationDate));
  if (cmpString(accessibility, ""))
    throw Exception("accessibility is not empty");
  if (!cmpString(sysCode, ""))
    throw Exception(
          std::string("Failed verify for the sysCode: ") +
          tapeFile::toString(sysCode));
  if (cmpString(reserved, ""))
    throw Exception("reserved is not empty");
}

void tapeFile::HDR1::fill(
  std::string _fileId,
  std::string _VSN,
  int _fSeq) {

  setString(label, "HDR1");
  setString(blockCount, "000000");

  fillCommon(_fileId, _VSN, _fSeq);
}

void tapeFile::HDR1::verify() const throw (Exception) {
  if (cmpString(label, "HDR1"))
    throw Exception(std::string("Failed verify for the HDR1: ") +
          tapeFile::toString(label));
  if (cmpString(blockCount, "000000"))
    throw Exception(std::string("Failed verify for the blockCount: ") +
          tapeFile::toString(blockCount));

  verifyCommon();
}

void tapeFile::HDR1PRELABEL::fill(std::string _VSN) {
  setString(label, "HDR1");
  setString(blockCount, "000000");

  fillCommon(std::string("PRELABEL"), _VSN, 1);
}

void tapeFile::HDR1PRELABEL::verify()
  const throw (Exception) {

  if (cmpString(label, "HDR1"))
    throw Exception(std::string("Failed verify for the HDR1: ") +
          tapeFile::toString(label));
  if (cmpString(blockCount, "000000"))
    throw Exception(
          std::string("Failed verify for the blockCount: ") +
          tapeFile::toString(blockCount));
  if (cmpString(fileId, "PRELABEL"))
    throw Exception(
          std::string("Failed verify for the PRELABEL: ") +
          tapeFile::toString(fileId));

  verifyCommon();
}

void tapeFile::EOF1::fill(
  std::string _fileId, std::string _VSN, int _fSeq, int _blockCount) {

  setString(label, "EOF1");
  setInt(blockCount, _blockCount);

  fillCommon(_fileId, _VSN, _fSeq);
}

void tapeFile::EOF1::verify() const throw (Exception) {
  if (cmpString(label, "EOF1"))
    throw Exception(std::string("Failed verify for the EOF1: ") +
          tapeFile::toString(label));
  if (!cmpString(blockCount, ""))
    throw Exception(
          std::string("Failed verify for the blockCount: ") +
          tapeFile::toString(blockCount));

  verifyCommon();
}

void tapeFile::HDR2EOF2::fillCommon(int _blockLength, bool driveHasCompression) {
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

void tapeFile::HDR2EOF2::verifyCommon() 
  const throw (Exception) {

  if (cmpString(recordFormat, "F"))
    throw Exception(
          std::string("Failed verify for the recordFormat: ") +
          tapeFile::toString(recordFormat));
  if (!cmpString(blockLength, ""))
    throw Exception(
          std::string("Failed verify for the blockLength: ") +
          tapeFile::toString(blockLength));
  if (!cmpString(recordLength, ""))
    throw Exception(
          std::string("Failed verify for the recordLength: ") +
          tapeFile::toString(recordLength));
  if (cmpString(aulId, "00"))
    throw Exception(
          std::string("Failed verify for the aulId: ") +
          tapeFile::toString(aulId));
  if (cmpString(reserved1, ""))
    throw Exception("reserved1 is not empty");
  if (cmpString(reserved2, ""))
    throw castor::tape::Exception("reserved2 is not empty");
  if (cmpString(reserved3, ""))
    throw castor::tape::Exception("reserved2 is not empty");
}

void tapeFile::HDR2::fill(int _blockLength, bool driveHasCompression) {
  setString(label, "HDR2");
  
  fillCommon(_blockLength, driveHasCompression);
}
void tapeFile::HDR2::verify() const throw (Exception) {
  if (cmpString(label, "HDR2"))
    throw Exception(std::string("Failed verify for the HDR2: ") +
          tapeFile::toString(label));

  verifyCommon();
}

void tapeFile::EOF2::fill(int _blockLength, bool driveHasCompression) {
  setString(label, "EOF2");

  fillCommon(_blockLength, driveHasCompression);
}

void tapeFile::EOF2::verify() const throw (Exception) {
  if (cmpString(label, "EOF2"))
    throw Exception(std::string("Failed verify for the EOF2: ") +
          tapeFile::toString(label));

  verifyCommon();
}

void tapeFile::UHL1UTL1::fillCommon(int fSeq,
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

void tapeFile::UHL1UTL1::verifyCommon() 
  const throw (Exception){  

  if (!cmpString(actualfSeq, ""))
    throw Exception(
          std::string("Failed verify for the actualfSeq: ") +
          tapeFile::toString(actualfSeq));
  if (!cmpString(actualBlockSize, ""))
    throw Exception(
          std::string("Failed verify for the actualBlockSize: ") +
          tapeFile::toString(actualBlockSize));
  if (!cmpString(actualRecordLength, ""))
    throw Exception(
          std::string("Failed verify for the actualRecordLength: ") +
          tapeFile::toString(actualRecordLength));
  if (!cmpString(site, ""))
    throw Exception(
          std::string("Failed verify for site: ") +
          tapeFile::toString(site));
  if (!cmpString(moverHost, ""))
    throw Exception(
          std::string("Failed verify for moverHost: ") +
          tapeFile::toString(moverHost));
  if (!cmpString(driveVendor, ""))
    throw Exception(
          std::string("Failed verify for driveVendor: ") +
          tapeFile::toString(driveVendor));
  if (!cmpString(driveModel, ""))
    throw Exception(
          std::string("Failed verify for driveModel: ") +
          tapeFile::toString(driveModel));
  if (!cmpString(serialNumber, ""))
    throw Exception(
          std::string("Failed verify for serialNumber: ") +
          tapeFile::toString(serialNumber));
}

void tapeFile::UHL1::fill(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  drives::deviceInfo deviceInfo) {

  setString(label, "UHL1");

  fillCommon(fSeq, blockSize, siteName, hostName, deviceInfo);
}

void tapeFile::UHL1::verify() const throw (Exception) {
  if (cmpString(label, "UHL1"))
    throw Exception(std::string("Failed verify for the UHL1: ") +
          tapeFile::toString(label));

  verifyCommon();
}

void tapeFile::UTL1::fill(int fSeq,
  int blockSize,
  std::string siteName,
  std::string hostName,
  drives::deviceInfo deviceInfo) {

  setString(label, "UTL1");

  fillCommon(fSeq, blockSize, siteName, hostName, deviceInfo);
}

void tapeFile::UTL1::verify() const throw (Exception) {
  if (cmpString(label, "UTL1"))
    throw Exception(std::string("Failed verify for the UTL1: ") +
          tapeFile::toString(label));

  verifyCommon();
}

