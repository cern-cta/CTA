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
#include <unistd.h>
#include <algorithm>
#include <fstream>

const unsigned short max_unix_hostname_length = 256; //255 + 1 terminating character

using namespace castor::tape::AULFile;

/**
 * Constructor of the ReadSession. It will rewind the tape, and check the 
 * VSN value. Throws an exception in case of mismatch.
 * @param drive: drive object to which we bind the session
 * @param volId: volume name of the tape we would like to read from
 */
ReadSession::ReadSession(drives::DriveGeneric & drive, std::string volId) throw (Exception) : dg(drive), VSN(volId), lock(false), corrupted(false) { 
  dg.rewind();
  VOL1 vol1;
  dg.readExactBlock((void * )&vol1, sizeof(vol1), "[ReadSession::ReadSession()] - Reading VOL1");
  try {
    vol1.verify();
  } catch (std::exception & e) {
    throw TapeFormatError(e.what());
  }
  HeaderChecker::checkVOL1(vol1, volId); //after which we are at the end of VOL1 header (i.e. beginning of HDR1 of the first file) on success, or at BOT in case of exception
}

/**
 * checks the volume label to make sure the label is valid and that we
 * have the correct tape (checks VSN). Leaves the tape at the end of the
 * first header block (i.e. right before the first data block) in case
 * of success, or rewinds the tape in case of volume label problems.
 * Might also leave the tape in unknown state in case any of the st
 * operations fail.
 */
void HeaderChecker::checkVOL1(const VOL1 &vol1, const std::string &volId) throw (Exception) {
  if(vol1.getVSN().compare(volId)) {
    std::stringstream ex_str;
    ex_str << "[HeaderChecker::checkVOL1()] - VSN of tape (" << vol1.getVSN() << ") is not the one requested (" << volId << ")";
    throw TapeFormatError(ex_str.str());
  }
}

/**
 * Constructor of the AULReadSession. It will rewind the tape, and check the 
 * VSN value. Throws an exception in case of mismatch.
 * @param drive
 * @param VSN
 */
ReadFile::ReadFile(ReadSession *rs, const Information &fileInfo) throw (Exception) : current_block_size(0), session(rs) {
  if(session->isCorrupted()) {
    throw SessionCorrupted();
  }
  if(session->lock) {
    throw SessionAlreadyInUse();
  }
  else {
    session->lock=true;
  }
  position(fileInfo);
}

/**
 * Destructor of the ReadFile. It will release the lock on the read session.
 */
ReadFile::~ReadFile() throw (Exception) {
  if(!(session->lock)) {
    session->setCorrupted();
  }
  else {
    session->lock=false;
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
bool HeaderChecker::checkHeaderNumericalField(const std::string &headerField, const uint64_t &value, bool is_field_hex, bool is_field_oct) throw (Exception) {
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
void HeaderChecker::checkHDR1(const HDR1 &hdr1, const Information &fileInfo, const std::string &volId) throw (Exception) {
  if(!checkHeaderNumericalField(hdr1.getFileId(), (uint64_t)fileInfo.nsFileId, true, false)) { // the nsfileid stored in HDR1 is in hexadecimal while the one supplied in the Information structure is in decimal
    std::stringstream ex_str;
    ex_str << "[HeaderChecker::checkHDR1] - Invalid fileid detected: " << hdr1.getFileId() << ". Wanted: " << fileInfo.nsFileId << std::endl;
    throw TapeFormatError(ex_str.str());
  }
  
  //the following should never ever happen... but never say never...
  if(hdr1.getVSN().compare(volId)) {
    std::stringstream ex_str;
    ex_str << "[HeaderChecker::checkHDR1] - Wrong volume ID info found in hdr1: " << hdr1.getVSN() << ". Wanted: " << volId;
    throw TapeFormatError(ex_str.str());
  }
}

/**
 * Checks the uhl1
 * @param uhl1: the uhl1 header of the current file
 * @param fileInfo: the Information structure of the current file
 */
void HeaderChecker::checkUHL1(const UHL1 &uhl1, const Information &fileInfo) throw (Exception) {
  if(!checkHeaderNumericalField(uhl1.getfSeq(), (uint64_t)fileInfo.fseq, true, false)) {
    std::stringstream ex_str;
    ex_str << "[HeaderChecker::checkUHL1] - Invalid fseq detected in uhl1: " << atol(uhl1.getfSeq().c_str()) << ". Wanted: " << fileInfo.fseq;
    throw TapeFormatError(ex_str.str());
  }
}

/**
 * Checks the utl1
 * @param utl1: the utl1 trailer of the current file
 * @param fseq: the file sequence number of the current file
 */
void HeaderChecker::checkUTL1(const UTL1 &utl1, const uint32_t fseq) throw (Exception) {
  if(!checkHeaderNumericalField(utl1.getfSeq(), (uint64_t)fseq, true, false)) {
    std::stringstream ex_str;
    ex_str << "[HeaderChecker::checkUTL1] - Invalid fseq detected in uhl1: " << atol(utl1.getfSeq().c_str()) << ". Wanted: " << fseq;
    throw TapeFormatError(ex_str.str());
  }
}

/**
 * Set the block size member using the info contained within the uhl1
 * @param uhl1: the uhl1 header of the current file
 * @param fileInfo: the Information structure of the current file
 */
void ReadFile::setBlockSize(const UHL1 &uhl1) throw (Exception) {
  current_block_size = (size_t)atol(uhl1.getBlockSize().c_str());
  if(current_block_size<1) {
    std::stringstream ex_str;
    ex_str << "[ReadFile::setBlockSize] - Invalid block size in uhl1 detected: " << current_block_size;
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
void ReadFile::position(const Information &fileInfo) throw (Exception) {
  
  if(fileInfo.checksum==0 or fileInfo.nsFileId==0 or fileInfo.size==0 or fileInfo.fseq<1) {
    throw castor::exception::InvalidArgument();
  }  
  
  // if we want the first file on tape (fileInfo.blockId==0) we need to skip the VOL1 header
  uint32_t destination_block = fileInfo.blockId ? fileInfo.blockId : 1;
  
  // we position using the sg locate because it is supposed to do the right thing possibly in a more optimized way (better than st's spaceBlocksForward/Backwards)
  session->dg.positionToLogicalObject(destination_block);// at this point we should be at the beginning of the headers of the desired file, so now let's check the headers...
  
  HDR1 hdr1;
  HDR2 hdr2;
  UHL1 uhl1;
  session->dg.readExactBlock((void *)&hdr1, sizeof(hdr1), "[ReadFile::position] - Reading HDR1");  
  session->dg.readExactBlock((void *)&hdr2, sizeof(hdr2), "[ReadFile::position] - Reading HDR2");
  session->dg.readExactBlock((void *)&uhl1, sizeof(uhl1), "[ReadFile::position] - Reading UHL1");
  session->dg.readFileMark("[ReadFile::position] - Reading file mark at the end of file header"); // after this we should be where we want, i.e. at the beginning of the file
  
  //the size of the headers is fine, now let's check each header  
  try {
    hdr1.verify();
    hdr2.verify();
    uhl1.verify();
  } catch (std::exception & e) {
    throw TapeFormatError(e.what());
  }
  
  //headers are valid here, let's see if they contain the right info, i.e. are we in the correct place?
  HeaderChecker::checkHDR1(hdr1, fileInfo, session->VSN);
  //we disregard hdr2 on purpose as it contains no useful information, we now check the fseq in uhl1 (hdr1 also contains fseq info but it is modulo 10000, therefore useless)
  HeaderChecker::checkUHL1(uhl1, fileInfo);
  //now that we are all happy with the information contained within the headers, we finally get the block size for our file (provided it has a reasonable value)
  setBlockSize(uhl1);
}

/**
 * After positioning at the beginning of a file for readings, this function
 * allows the reader to know which block sizes to provide.
 * @return the block size in bytes.
 */
size_t ReadFile::getBlockSize() throw (Exception) {
  if(current_block_size<1) {
    std::stringstream ex_str;
    ex_str << "[ReadFile::getBlockSize] - Invalid block size: " << current_block_size;
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
size_t ReadFile::read(void * buff, size_t len) throw (Exception) {
  if(len!=current_block_size) {
    throw castor::exception::InvalidArgument();
  }
  size_t bytes_read = session->dg.readBlock(buff, len);
  if(!bytes_read) {
    throw EndOfFile();
  }
  return bytes_read;
}

/**
 * Constructor of the WriteSession. It will rewind the tape, and check the 
 * VSN value. Throws an exception in case of mismatch. Then positions the tape at the
 * end of the last file (actually at the end of the last trailer) after having checked
 * the fseq in the trailer of the last file.
 * @param drive: drive object to which we bind the session
 * @param volId: volume name of the tape we would like to write to
 * @param last_fseq: fseq of the last active (undeleted) file on tape
 * @param compression: set this to true in case the drive has compression enabled (x000GC)
 */
WriteSession::WriteSession(drives::DriveGeneric & drive, std::string volId, uint32_t last_fseq, bool compression) throw (Exception) : dg(drive), VSN(volId), lock(false), compressionEnabled(compression), corrupted(false) {

  if(!volId.compare("")) {
    throw castor::exception::InvalidArgument();
  }
  
  dg.rewind();
  VOL1 vol1;
  dg.readExactBlock((void * )&vol1, sizeof(vol1), "[WriteSession::checkVOL1()] - Reading VOL1");
  try {
    vol1.verify();
  } catch (std::exception & e) {
    throw TapeFormatError(e.what());
  }  
  HeaderChecker::checkVOL1(vol1, volId); // now we know that we are going to write on the correct tape
  //if the tape is not empty let's move to the last trailer
  if(last_fseq>0) {
    uint32_t dst_filemark = last_fseq*3-1; // 3 file marks per file but we want to read the last trailer (hence the -1)
    dg.spaceFileMarksForward(dst_filemark);

    EOF1 eof1;
    EOF2 eof2;
    UTL1 utl1;
    dg.readExactBlock((void *)&eof1, sizeof(eof1), "[WriteSession::WriteSession] - Reading EOF1");  
    dg.readExactBlock((void *)&eof2, sizeof(eof2), "[WriteSession::WriteSession] - Reading EOF2");
    dg.readExactBlock((void *)&utl1, sizeof(utl1), "[WriteSession::WriteSession] - Reading UTL1");
    dg.readFileMark("[WriteSession::WriteSession] - Reading file mark at the end of file trailer"); // after this we should be where we want, i.e. at the end of the last trailer of the last file on tape

    //the size of the trailers is fine, now let's check each trailer  
    try {
      eof1.verify();
      eof2.verify();
      utl1.verify();
    } catch (std::exception & e) {
      throw TapeFormatError(e.what());
    }

    //trailers are valid here, let's see if they contain the right info, i.e. are we in the correct place?
    //we disregard eof1 and eof2 on purpose as they contain no useful information for us now, we now check the fseq in utl1 (hdr1 also contains fseq info but it is modulo 10000, therefore useless)
    HeaderChecker::checkUTL1(utl1, last_fseq);
  } 
  else {
    //else we are already where we want to be: at the end of the 80 bytes of the VOL1, all ready to write the headers of the first file
  }
  //now we need to get two pieces of information that will end up in the headers and trailers that we will write (siteName, hostName)
  setSiteName();
  setHostName();
}

/**
 * uses gethostname to get the upper-cased hostname without the domain name
 */
void WriteSession::setHostName() throw (Exception) {
  char hostname_cstr[max_unix_hostname_length];
  castor::exception::Errnum::throwOnMinusOne(gethostname(hostname_cstr, max_unix_hostname_length), "Failed gethostname() in WriteFile::setHostName");
  hostName = hostname_cstr;
  std::transform(hostName.begin(), hostName.end(), hostName.begin(), ::toupper);
  hostName = hostName.substr(0, hostName.find("."));
}

/**
 * looks for the site name in /etc/resolv.conf in the search line and saves the upper-cased value in siteName
 */
void WriteSession::setSiteName() throw (Exception) {
  std::ifstream resolv;
  resolv.exceptions(std::ifstream::failbit|std::ifstream::badbit);
  try {
    resolv.open("/etc/resolv.conf", std::ifstream::in);
    std::string buf;
    while(std::getline(resolv, buf)) {
      if(std::string::npos != buf.find("search ")) {
        siteName = buf.substr(7);
        siteName = siteName.substr(0, siteName.find("."));
        std::transform(siteName.begin(), siteName.end(), siteName.begin(), ::toupper);
        break;
      }
    }
    resolv.close();
  }
  catch (std::ifstream::failure e) {
    throw castor::exception::Exception("Error opening, reading or closing /etc/resolv.conf");
  }
}

/**
 * Constructor of the WriteFile. It will bind itself to an existing write session
 * and write the headers of the file (positioning already occurred in the WriteSession).
 * @param ws: session to be bound to
 * @param fileInfo: information about the file we want to read
 * @param blockSize: size of blocks we want to use in writing
 */
WriteFile::WriteFile(WriteSession *ws, Information info, size_t blockSize) throw (Exception) : current_block_size(blockSize), session(ws), fileinfo(info), open(false), nonzero_file_written(false), number_of_blocks(0) {
  if(session->isCorrupted()) {
    throw SessionCorrupted();
  }
  if(session->lock) {
    throw SessionAlreadyInUse();
  }
  else {
    session->lock=true;
  }
  HDR1 hdr1;
  HDR2 hdr2;
  UHL1 uhl1;
  std::stringstream s;
  s << std::hex << fileinfo.nsFileId;
  std::string fileId;
  s >> fileId;
  std::transform(fileId.begin(), fileId.end(), fileId.begin(), ::toupper);
  hdr1.fill(fileId, session->VSN, fileinfo.fseq);
  hdr2.fill(current_block_size, session->compressionEnabled);
  uhl1.fill(fileinfo.fseq, current_block_size, session->getSiteName(), session->getHostName(), session->dg.getDeviceInfo());
  session->dg.writeBlock(&hdr1, sizeof(hdr1));
  session->dg.writeBlock(&hdr2, sizeof(hdr2));
  session->dg.writeBlock(&uhl1, sizeof(uhl1));
  session->dg.writeImmediateFileMarks(1);
  open=true;
}

/**
 * Returns the block id of the current position
 * @return blockId of current position
 */
uint32_t WriteFile::getPosition() throw (Exception) {  
  return session->dg.getPositionInfo().currentPosition;
}

/**
 * Writes a block of data on tape
 * @param data: buffer to copy the data from
 * @param size: size of the buffer
 * @return amount of data actually written 
 */
void WriteFile::write(const void *data, size_t size) throw (Exception) {
  session->dg.writeBlock(data, size);
  if(size>0) {
    nonzero_file_written = true;
    number_of_blocks++;
  }
}

/**
 * Closes the file by writing the corresponding trailer on tape
 */
void WriteFile::close() throw (Exception) {
  if(!open) {
    session->setCorrupted();
    throw FileClosedTwice();
  }
  if(!nonzero_file_written) {
    session->setCorrupted();
    throw ZeroFileWritten();
  }
  session->dg.writeImmediateFileMarks(1); // filemark at the end the of data file
  EOF1 eof1;
  EOF2 eof2;
  UTL1 utl1;
  std::stringstream s;
  s << std::hex << fileinfo.nsFileId;
  std::string fileId;
  s >> fileId;
  std::transform(fileId.begin(), fileId.end(), fileId.begin(), ::toupper);
  eof1.fill(fileId, session->VSN, fileinfo.fseq, number_of_blocks);
  eof2.fill(current_block_size, session->compressionEnabled);
  utl1.fill(fileinfo.fseq, current_block_size, session->getSiteName(), session->getHostName(), session->dg.getDeviceInfo());
  session->dg.writeBlock(&eof1, sizeof(eof1));
  session->dg.writeBlock(&eof2, sizeof(eof2));
  session->dg.writeBlock(&utl1, sizeof(utl1));
  session->dg.writeImmediateFileMarks(1); // filemark at the end the of trailers
  open=false;
}

/**
 * Destructor of the WriteFile object. Releases the WriteSession
 */
WriteFile::~WriteFile() throw (Exception) {
  if(open) {
    session->setCorrupted();
  }
  if(!(session->lock)) {
    session->setCorrupted();
  }
  else {
    session->lock=false;
  }  
}
