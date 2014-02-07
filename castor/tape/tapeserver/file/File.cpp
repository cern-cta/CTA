/******************************************************************************
 *                      File.cpp
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

#include "File.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/exception/Mismatch.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include <sstream>
#include <iomanip>

using namespace castor::tape::AULFile;
/**
 * Constructor of the AULReadSession. It will rewind the tape, and check the 
 * VSN value. Throws an exception in case of mismatch.
 * @param drive
 * @param VSN
 */
ReadSession::ReadSession(drives::DriveGeneric & drive, std::string volId) throw (Exception) : dg(drive), VSN(volId), current_block_size(0) { 
  checkVOL1(); //after which we are at the end of VOL1 header (i.e. beginning of HDR1 of the first file) on success, or at BOT in case of exception
}

/**
 * checks the volume label to make sure the label is valid and that we
 * have the correct tape (checks VSN). Leaves the tape at the end of the
 * first header block (i.e. right before the first data block) in case
 * of success, or rewinds the tape in case of volume label problems.
 * Might also leave the tape in unknown state in case any of the st
 * operations fail.
 */
void ReadSession::checkVOL1() throw (Exception) {
  dg.rewind();
  VOL1 vol1;
  dg.readExactBlock((void * )&vol1, sizeof(vol1), "[ReadSession::checkVOL1()] - Reading VOL1");
  try {
    vol1.verify();
  } catch (std::exception & e) {
    throw TapeFormatError(e.what());
  }  
  if(vol1.getVSN().compare(VSN)) {
    dg.rewind();
    std::stringstream ex_str;
    ex_str << "[ReadSession::checkVOL1()] - VSN of tape (" << vol1.getVSN() << ") is not the one requested (" << VSN << ")";
    throw TapeFormatError(ex_str.str());
  }
}

/**
 * Checks the field of a header comparing it with the numerical value provided
 * @param headerField: the string of the header that we need to check
 * @param value: the unsigned numerical value against which we check
 * @param is_field_hex: set to true if the value in the header is in hexadecimal and to false otherwise
 * @param is_field_oct: set to true if the value in the header is in octal and to false otherwise
 * @return true if the header field  matches the numerical value, false otherwise
 */
bool ReadSession::checkHeaderNumericalField(const std::string &headerField, const uint64_t &value, bool is_field_hex, bool is_field_oct) const throw (Exception) {
  uint64_t res = 0;
  std::stringstream field_converter;
  field_converter << headerField;
  if(is_field_hex && !is_field_oct) field_converter >> std::hex >> res;
  else if(!is_field_hex && is_field_oct) field_converter >> std::oct >> res;
  else if(!is_field_hex && !is_field_oct) field_converter >> res;
  else throw castor::exception::InvalidArgument();
  return value==res;
}

/**
 * Checks the hdr1
 * @param hdr1: the hdr1 header of the current file
 * @param fileInfo: the Information structure of the current file
 */
void ReadSession::checkHDR1(const HDR1 &hdr1, const Information &fileInfo) const throw (Exception) {
  if(!checkHeaderNumericalField(hdr1.getFileId(), (uint64_t)fileInfo.nsFileId, true, false)) { // the nsfileid stored in HDR1 is in hexadecimal while the one supplied in the Information structure is in decimal
    std::stringstream ex_str;
    ex_str << "[ReadSession::checkHDR1] - Invalid fileid detected: " << hdr1.getFileId() << ". Wanted: " << fileInfo.nsFileId << std::endl;
    throw TapeFormatError(ex_str.str());
  }
  
  //the following should never ever happen... but never say never...
  if(hdr1.getVSN().compare(VSN)) {
    std::stringstream ex_str;
    ex_str << "[ReadSession::checkHDR1] - Wrong volume ID info found in hdr1: " << hdr1.getVSN() << ". Wanted: " << VSN;
    throw TapeFormatError(ex_str.str());
  }
}

/**
 * Checks the uhl1
 * @param uhl1: the uhl1 header of the current file
 * @param fileInfo: the Information structure of the current file
 */
void ReadSession::checkUHL1(const UHL1 &uhl1, const Information &fileInfo) const throw (Exception) {
  if(!checkHeaderNumericalField(uhl1.getfSeq(), (uint64_t)fileInfo.fseq, true, false)) {
    std::stringstream ex_str;
    ex_str << "[ReadSession::checkUHL1] - Invalid fseq detected in uhl1: " << atol(uhl1.getfSeq().c_str()) << ". Wanted: " << fileInfo.fseq;
    throw TapeFormatError(ex_str.str());
  }
}

/**
 * Set the block size member using the info contained within the uhl1
 * @param uhl1: the uhl1 header of the current file
 * @param fileInfo: the Information structure of the current file
 */
void ReadSession::setBlockSize(const UHL1 &uhl1) throw (Exception) {
  current_block_size = (size_t)atol(uhl1.getBlockSize().c_str());
  if(current_block_size<1) {
    std::stringstream ex_str;
    ex_str << "[ReadSession::setBlockSize] - Invalid block size in uhl1 detected: " << current_block_size;
    throw TapeFormatError(ex_str.str());
  }
}

/**
 * Positions the tape for reading the file. Depending on the previous activity,
 * it is the duty of this function to determine how to best move to the next
 * file. The positioning will then be verified (header will be read). 
 * As usual, exception is thrown if anything goes wrong.
 * @param fileInfo: all relevant information passed by the stager about
 * the file.
 */
void ReadSession::position(const Information &fileInfo) throw (Exception) {
  
  if(fileInfo.checksum==0 or fileInfo.nsFileId==0 or fileInfo.size==0 or fileInfo.fseq<1) {
    throw castor::exception::InvalidArgument();
  }  
  
  // if we want the first file on tape (fileInfo.blockId==0) we need to skip the VOL1 header
  uint32_t destination_block = fileInfo.blockId ? fileInfo.blockId : 1;
  
  // we position using the sg locate because it is supposed to do the right thing possibly in a more optimized way (better than st's spaceBlocksForward/Backwards)
  dg.positionToLogicalObject(destination_block);// at this point we should be at the beginning of the headers of the desired file, so now let's check the headers...
  
  HDR1 hdr1;
  HDR2 hdr2;
  UHL1 uhl1;
  dg.readExactBlock((void *)&hdr1, sizeof(hdr1), "[ReadSession::position] - Reading HDR1");  
  dg.readExactBlock((void *)&hdr2, sizeof(hdr2), "[ReadSession::position] - Reading HDR2");
  dg.readExactBlock((void *)&uhl1, sizeof(uhl1), "[ReadSession::position] - Reading UHL1");
  dg.readFileMark("[ReadSession::position] - Reading file mark at the end of file header"); // after this we should be where we want, i.e. at the beginning of the file
  
  //the size of the headers is fine, now let's check each header  
  try {
    hdr1.verify();
    hdr2.verify();
    uhl1.verify();
  } catch (std::exception & e) {
    throw TapeFormatError(e.what());
  }
  
  //headers are valid here, let's see if they contain the right info, i.e. are we in the correct place?
  checkHDR1(hdr1, fileInfo);
  //we disregard hdr2 on purpose as it contains no useful information, we now check the fseq in uhl1 (hdr1 also contains fseq info but it is modulo 10000, therefore useless)
  checkUHL1(uhl1, fileInfo);
  //now that we are all happy with the information contained within the headers, we finally get the block size for our file (provided it has a reasonable value)
  setBlockSize(uhl1);
}

/**
 * After positioning at the beginning of a file for readings, this function
 * allows the reader to know which block sizes to provide.
 * @return the block size in bytes.
 */
size_t ReadSession::getBlockSize() throw (Exception) {
  if(current_block_size<1) {
    std::stringstream ex_str;
    ex_str << "[ReadSession::getBlockSize] - Invalid block size: " << current_block_size;
    throw TapeFormatError(ex_str.str());
  }
  return current_block_size;
}

/**
 * Read data from the file. The buffer should equal to or bigger than the 
 * block size. Will try to actually fill up the provided buffer (this
 * function can trigger several read on the tape side).
 * This function will throw exceptions when problems arise (especially
 * at end of file in case of size or checksum mismatch.
 * After end of file, a new call to read without a call to position
 * will throw NotReadingAFile.
 * @param buff pointer to the data buffer
 * @param len size of the buffer
 * @return The amount of data actually copied. Zero at end of file.
 */
size_t ReadSession::read(void * buff, size_t len) throw (Exception) {  
  return dg.readBlock(buff, len);
}
