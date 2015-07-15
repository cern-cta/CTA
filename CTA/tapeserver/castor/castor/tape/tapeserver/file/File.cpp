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

#include "castor/tape/tapeserver/file/File.hpp"
#include "castor/exception/Errnum.hpp"
#include "castor/exception/SErrnum.hpp"
#include "castor/exception/Mismatch.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include <sstream>
#include <iomanip>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <rfio_api.h>

const unsigned short max_unix_hostname_length = 256; //255 + 1 terminating character

namespace castor {
  namespace tape {
    namespace tapeFile {

      LabelSession::LabelSession(tapeserver::drive::DriveInterface & drive, const std::string &vid)  {
        VOL1 vol1;
        vol1.fill(vid);
        drive.writeBlock(&vol1, sizeof(vol1));
        HDR1PRELABEL prelabel;
        prelabel.fill(vid);
        drive.writeBlock(&prelabel, sizeof(prelabel));
        drive.writeSyncFileMarks(1);
      }

      ReadSession::ReadSession(tapeserver::drive::DriveInterface & drive,
              tapeserver::client::ClientInterface::VolumeInfo volInfo)  : 
      m_drive(drive), m_vid(volInfo.vid), m_corrupted(false), m_locked(false), 
      m_fseq(1), m_currentFilePart(Header),m_volInfo(volInfo) { 

        if(!m_vid.compare("")) {
          throw castor::exception::InvalidArgument();
        }
        
        if(m_drive.isTapeBlank()) {
          castor::exception::Exception ex;
          ex.getMessage() << "[ReadSession::ReadSession()] - Tape is blank, cannot proceed with constructing the ReadSession";
          throw ex;
        }
        
        m_drive.rewind();
        VOL1 vol1;
        m_drive.readExactBlock((void * )&vol1, sizeof(vol1), "[ReadSession::ReadSession()] - Reading VOL1");
        try {
          vol1.verify();
        } catch (std::exception & e) {
          throw TapeFormatError(e.what());
        }
        HeaderChecker::checkVOL1(vol1, volInfo.vid); //after which we are at the end of VOL1 header (i.e. beginning of HDR1 of the first file) on success, or at BOT in case of exception
      }

      void HeaderChecker::checkVOL1(const VOL1 &vol1, const std::string &volId)  {
        if(vol1.getVSN().compare(volId)) {
          std::stringstream ex_str;
          ex_str << "[HeaderChecker::checkVOL1()] - VSN of tape (" << vol1.getVSN() << ") is not the one requested (" << volId << ")";
          throw TapeFormatError(ex_str.str());
        }
      }
      bool HeaderChecker::checkHeaderNumericalField(const std::string &headerField, 
        const uint64_t value, const headerBase base)  {
        uint64_t res = 0;
        std::stringstream field_converter;
        field_converter << headerField;
        switch(base) {
          case octal:
            field_converter >> std::oct >> res;
            break;
          case decimal:
            field_converter >> std::dec >> res;
            break;
          case hexadecimal:
            field_converter >> std::hex >> res;
            break;
          default:
            throw castor::exception::InvalidArgument("Unrecognised base in HeaderChecker::checkHeaderNumericalField");
        }
        return value==res;
      }

      void HeaderChecker::checkHDR1(const HDR1 &hdr1,
        const castor::tape::tapegateway::FileToRecallStruct &filetoRecall,
        const tape::tapeserver::client::ClientInterface::VolumeInfo &volInfo)  {
        const std::string &volId = volInfo.vid;
        if( volInfo.clientType != tape::tapegateway::READ_TP 
       && (!checkHeaderNumericalField(hdr1.getFileId(), 
            filetoRecall.fileid(), hexadecimal))) { 
          // the nsfileid stored in HDR1 as an hexadecimal string . The one in 
          // filetoRecall is numeric
          std::stringstream ex_str;
          ex_str << "[HeaderChecker::checkHDR1] - Invalid fileid detected: (0x)\"" 
              << hdr1.getFileId() << "\". Wanted: 0x" << std::hex 
              << filetoRecall.fileid() << std::endl;
          throw TapeFormatError(ex_str.str());
        }

        //the following should never ever happen... but never say never...
        if(hdr1.getVSN().compare(volId)) {
          std::stringstream ex_str;
          ex_str << "[HeaderChecker::checkHDR1] - Wrong volume ID info found in hdr1: " 
              << hdr1.getVSN() << ". Wanted: " << volId;
          throw TapeFormatError(ex_str.str());
        }
      }

      void HeaderChecker::checkUHL1(const UHL1 &uhl1,
        const castor::tape::tapegateway::FileToRecallStruct &fileToRecall)  {
        if(!checkHeaderNumericalField(uhl1.getfSeq(), fileToRecall.fseq(), decimal)) {
          std::stringstream ex_str;
          ex_str << "[HeaderChecker::checkUHL1] - Invalid fseq detected in uhl1: \"" 
              << uhl1.getfSeq() << "\". Wanted: " << fileToRecall.fseq();
          throw TapeFormatError(ex_str.str());
        }
      }

      void HeaderChecker::checkUTL1(const UTL1 &utl1, const uint32_t fseq)  {
        if(!checkHeaderNumericalField(utl1.getfSeq(), (uint64_t)fseq, decimal)) {
          std::stringstream ex_str;
          ex_str << "[HeaderChecker::checkUTL1] - Invalid fseq detected in utl1: \"" 
                 << utl1.getfSeq() << "\". Wanted: " << fseq;
          throw TapeFormatError(ex_str.str());
        }
      }
      
      // TODO: merge with same function in DiskWriteTask.hpp and move to tape/utils
      uint32_t BlockId::extract(const castor::tape::tapegateway::FileToRecallStruct& ftr) {
        return (ftr.blockId0() << 24) | (ftr.blockId1() << 16) |  (ftr.blockId2() << 8) | ftr.blockId3();
      }
      
      void BlockId::set(castor::tape::tapegateway::FileToRecallStruct& ftr, uint32_t blockId) {
        ftr.setBlockId0(blockId >> 24);
        ftr.setBlockId1((blockId >> 16) & 0xFF);
        ftr.setBlockId2((blockId >> 8) & 0xFF);
        ftr.setBlockId3((blockId) & 0xFF);
      }
      
      ReadFile::ReadFile(ReadSession *rs,
        const castor::tape::tapegateway::FileToRecallStruct &fileToRecall)
         : m_currentBlockSize(0), m_session(rs), m_positionCommandCode(fileToRecall.positionCommandCode())
      {
        if(m_session->isCorrupted()) {
          throw SessionCorrupted();
        }
        m_session->lock();
        try{
          position(fileToRecall);
        } catch(...){
          if(castor::tape::tapegateway::TPPOSIT_FSEQ==m_positionCommandCode && 
                  m_session->getCurrentFilePart() != Header){
            m_session->setCorrupted();
          }
          m_session->release();
          throw;
        }
      }

      ReadFile::~ReadFile() throw() {
        if(castor::tape::tapegateway::TPPOSIT_FSEQ==m_positionCommandCode && 
                m_session->getCurrentFilePart() != Header){
          m_session->setCorrupted();
        }
        m_session->release();
      }
      void ReadFile::positionByFseq(const castor::tape::tapegateway::FileToRecallStruct &fileToRecall) {
        if(fileToRecall.fseq()<1) {
          std::stringstream err;
          err << "Unexpected fileId in ReadFile::position with fSeq expected >=1, got: "
                  << fileToRecall.fseq() << ")";
          throw castor::exception::InvalidArgument(err.str());
        }
        
        int64_t fseq_delta = fileToRecall.fseq() - m_session->getCurrentFseq();
        if(fileToRecall.fseq() == 1) { 
          // special case: we can rewind the tape to be faster 
          //(TODO: in the future we could also think of a threshold above 
          //which we rewind the tape anyway and then space forward)       
          m_session->m_drive.rewind();
          VOL1 vol1;
          m_session->m_drive.readExactBlock((void * )&vol1, sizeof(vol1), "[ReadFile::position] - Reading VOL1");
          try {
            vol1.verify();
          } catch (std::exception & e) {
            throw TapeFormatError(e.what());
          }
        }
        else if(fseq_delta == 0) {
          // do nothing we are in the correct place
        }
        else if(fseq_delta > 0) {
          //we need to skip three file marks per file (header, payload, trailer)
          m_session->m_drive.spaceFileMarksForward((uint32_t)fseq_delta*3); 
        }
        else { //fseq_delta < 0
          //we need to skip three file marks per file 
          //(trailer, payload, header) + 1 to go on the BOT (beginning of tape) side 
          //of the file mark before the header of the file we want to read
          m_session->m_drive.spaceFileMarksBackwards((uint32_t)abs(fseq_delta)*3+1); 
          m_session->m_drive.readFileMark("[ReadFile::position] Reading file mark right before the header of the file we want to read");
        }
      }
        
        void ReadFile::positionByBlockID(const castor::tape::tapegateway::FileToRecallStruct &fileToRecall) {
         // if we want the first file on tape (fileInfo.blockId==0) we need to skip the VOL1 header
          const uint32_t destination_block = BlockId::extract(fileToRecall) ? 
            BlockId::extract(fileToRecall) : 1;
          /* 
          we position using the sg locate because it is supposed to do the 
          right thing possibly in a more optimized way (better than st's 
          spaceBlocksForward/Backwards) 
           */
          
          // at this point we should be at the beginning of
          //the headers of the desired file, so now let's check the headers...
          m_session->m_drive.positionToLogicalObject(destination_block);
        }

      void ReadFile::setBlockSize(const UHL1 &uhl1)  {
        m_currentBlockSize = (size_t)atol(uhl1.getBlockSize().c_str());
        if(m_currentBlockSize<1) {
          std::stringstream ex_str;
          ex_str << "[ReadFile::setBlockSize] - Invalid block size in uhl1 detected";
          throw TapeFormatError(ex_str.str());
        }
      }

      void ReadFile::position(
        const castor::tape::tapegateway::FileToRecallStruct &fileToRecall)  {  
        
        if(m_session->getCurrentFilePart() != Header && fileToRecall.positionCommandCode()!=castor::tape::tapegateway::TPPOSIT_BLKID) {
          m_session->setCorrupted();
          throw SessionCorrupted();
        }
        
        // Make sure the session state is advanced to cover our failures
        // and allow next call to position to discover we failed half way
        m_session->setCurrentFilePart(HeaderProcessing);
                
        if(castor::tape::tapegateway::TPPOSIT_BLKID==m_positionCommandCode) {
          positionByBlockID(fileToRecall);
        }
        else if(castor::tape::tapegateway::TPPOSIT_FSEQ==m_positionCommandCode) {    
          positionByFseq(fileToRecall);
        }
        else {
          throw UnsupportedPositioningMode();
        }

        //save the current fseq into the read session
        m_session->setCurrentFseq(fileToRecall.fseq());

        HDR1 hdr1;
        HDR2 hdr2;
        UHL1 uhl1;
        m_session->m_drive.readExactBlock((void *)&hdr1, sizeof(hdr1), "[ReadFile::position] - Reading HDR1");  
        m_session->m_drive.readExactBlock((void *)&hdr2, sizeof(hdr2), "[ReadFile::position] - Reading HDR2");
        m_session->m_drive.readExactBlock((void *)&uhl1, sizeof(uhl1), "[ReadFile::position] - Reading UHL1");
        m_session->m_drive.readFileMark("[ReadFile::position] - Reading file mark at the end of file header"); 
        // after this we should be where we want, i.e. at the beginning of the file
        m_session->setCurrentFilePart(Payload);

        //the size of the headers is fine, now let's check each header  
        try {
          hdr1.verify();
          hdr2.verify();
          uhl1.verify();
        }
        catch (std::exception & e) {
          throw TapeFormatError(e.what());
        }

        //headers are valid here, let's see if they contain the right info, i.e. are we in the correct place?
        HeaderChecker::checkHDR1(hdr1, fileToRecall, m_session->getVolumeInfo());
        //we disregard hdr2 on purpose as it contains no useful information, we now check the fseq in uhl1 
        //(hdr1 also contains fseq info but it is modulo 10000, therefore useless)
        HeaderChecker::checkUHL1(uhl1, fileToRecall);
        //now that we are all happy with the information contained within the 
        //headers, we finally get the block size for our file (provided it has a reasonable value)
        setBlockSize(uhl1);
      }

      size_t ReadFile::getBlockSize()  {
        if(m_currentBlockSize<1) {
          std::stringstream ex_str;
          ex_str << "[ReadFile::getBlockSize] - Invalid block size: " << m_currentBlockSize;
          throw TapeFormatError(ex_str.str());
        }
        return m_currentBlockSize;
      }

      size_t ReadFile::read(void *data, const size_t size)  {
        if(size!=m_currentBlockSize) {
          throw WrongBlockSize();
        }
        size_t bytes_read = m_session->m_drive.readBlock(data, size);
        if(!bytes_read) {    // end of file reached! we will keep on reading until we have read the file mark at the end of the trailers
          m_session->setCurrentFilePart(Trailer);

          //let's read and check the trailers    
          EOF1 eof1;
          EOF2 eof2;
          UTL1 utl1;
          m_session->m_drive.readExactBlock((void *)&eof1, sizeof(eof1), "[ReadFile::read] - Reading HDR1");  
          m_session->m_drive.readExactBlock((void *)&eof2, sizeof(eof2), "[ReadFile::read] - Reading HDR2");
          m_session->m_drive.readExactBlock((void *)&utl1, sizeof(utl1), "[ReadFile::read] - Reading UTL1");
          m_session->m_drive.readFileMark("[ReadFile::read] - Reading file mark at the end of file trailer");

          m_session->setCurrentFseq(m_session->getCurrentFseq() + 1); // moving on to the header of the next file 
          m_session->setCurrentFilePart(Header);

          //the size of the headers is fine, now let's check each header  
          try {
            eof1.verify();
            eof2.verify();
            utl1.verify();
          }
          catch (std::exception & e) {
            throw TapeFormatError(e.what());
          }
          // the following is a normal day exception: end of files exceptions are thrown at the end of each file being read    
          throw EndOfFile();
        }
        return bytes_read;
      }

      WriteSession::WriteSession(tapeserver::drive::DriveInterface & drive, 
              const tapeserver::client::ClientInterface::VolumeInfo& volInfo, 
              const uint32_t last_fseq, const bool compression)  
      : m_drive(drive), m_vid(volInfo.vid), m_compressionEnabled(compression),
              m_corrupted(false), m_locked(false),m_volInfo(volInfo) {

        if(!m_vid.compare("")) {
          throw castor::exception::InvalidArgument();
        }
        
        if(m_drive.isTapeBlank()) {
          castor::exception::Exception ex;
          ex.getMessage() << "[WriteSession::WriteSession()] - Tape is blank, cannot proceed with constructing the WriteSession";
          throw ex;
        }

        m_drive.rewind();
        VOL1 vol1;
        m_drive.readExactBlock((void * )&vol1, sizeof(vol1), "[WriteSession::WriteSession()] - Reading VOL1");
        try {
          vol1.verify();
        } catch (std::exception & e) {
          throw TapeFormatError(e.what());
        }  
        HeaderChecker::checkVOL1(vol1, m_vid); // now we know that we are going to write on the correct tape
        //if the tape is not empty let's move to the last trailer
        if(last_fseq>0) {
          uint32_t dst_filemark = last_fseq*3-1; // 3 file marks per file but we want to read the last trailer (hence the -1)
          m_drive.spaceFileMarksForward(dst_filemark);

          EOF1 eof1;
          EOF2 eof2;
          UTL1 utl1;
          m_drive.readExactBlock((void *)&eof1, sizeof(eof1), "[WriteSession::WriteSession] - Reading EOF1");  
          m_drive.readExactBlock((void *)&eof2, sizeof(eof2), "[WriteSession::WriteSession] - Reading EOF2");
          m_drive.readExactBlock((void *)&utl1, sizeof(utl1), "[WriteSession::WriteSession] - Reading UTL1");
          m_drive.readFileMark("[WriteSession::WriteSession] - Reading file mark at the end of file trailer"); // after this we should be where we want, i.e. at the end of the last trailer of the last file on tape

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
          m_lastWrittenFSeq = last_fseq;
        } 
        else {
          //else we are already where we want to be: at the end of the 80 bytes of the VOL1, all ready to write the headers of the first file
          m_lastWrittenFSeq = 0;
        }
        //now we need to get two pieces of information that will end up in the headers and trailers that we will write (siteName, hostName)
        setSiteName();
        setHostName();
      }

      void WriteSession::setHostName()  {
        char hostname_cstr[max_unix_hostname_length];
        castor::exception::Errnum::throwOnMinusOne(gethostname(hostname_cstr, max_unix_hostname_length), "Failed gethostname() in WriteFile::setHostName");
        m_hostName = hostname_cstr;
        std::transform(m_hostName.begin(), m_hostName.end(), m_hostName.begin(), ::toupper);
        m_hostName = m_hostName.substr(0, m_hostName.find("."));
      }

      void WriteSession::setSiteName()  {
        std::ifstream resolv;
        resolv.exceptions(std::ifstream::failbit|std::ifstream::badbit);
        try {
          resolv.open("/etc/resolv.conf", std::ifstream::in);
          std::string buf;
          const char * const toFind="search ";
          while(std::getline(resolv, buf)) {
            if(buf.substr(0,7)==toFind) {
              m_siteName = buf.substr(7);
              m_siteName = m_siteName.substr(0, m_siteName.find("."));
              std::transform(m_siteName.begin(), m_siteName.end(), m_siteName.begin(), ::toupper);
              break;
            }
          }
          resolv.close();
        }
        catch (const std::ifstream::failure& e) {
          throw castor::exception::Exception(std::string("In /etc/resolv.conf : error opening/closing or can't find search domain [")+e.what()+"]");
        }
      }

      WriteFile::WriteFile(WriteSession *ws, 
        const castor::tape::tapegateway::FileToMigrateStruct & ftm,
        const size_t blockSize)  : 
        m_currentBlockSize(blockSize), m_session(ws), m_fileToMigrate(ftm),
        m_open(false), m_nonzeroFileWritten(false), m_numberOfBlocks(0)
      {
        // Check the sanity of the parameters. fSeq should be >= 1
        if ( ws->getVolumeInfo().clientType != castor::tape::tapegateway::WRITE_TP &&
                (0 == ftm.fileid() || ftm.fseq()<1)) {
          std::stringstream err;
          err << "Unexpected fileId in WriteFile::WriteFile (expected != 0, got: "
              << ftm.fileid() << ") or fSeq (expected >=1, got: "
              << ftm.fseq() << ")";
          throw castor::exception::InvalidArgument(err.str());
        }
        if(m_session->isCorrupted()) {
          throw SessionCorrupted();
        }
        m_session->lock();
        HDR1 hdr1;
        HDR2 hdr2;
        UHL1 uhl1;
        std::stringstream s;
        s << std::hex << m_fileToMigrate.fileid();
        std::string fileId;
        s >> fileId;
        std::transform(fileId.begin(), fileId.end(), fileId.begin(), ::toupper);
        hdr1.fill(fileId, m_session->m_vid, m_fileToMigrate.fseq());
        hdr2.fill(m_currentBlockSize, m_session->m_compressionEnabled);
        uhl1.fill(m_fileToMigrate.fseq(), m_currentBlockSize, m_session->getSiteName(), 
            m_session->getHostName(), m_session->m_drive.getDeviceInfo());
        /* Before writing anything, we record the blockId of the file */
        if (1 == ftm.fseq()) {
          m_blockId = 0;
        } else {
          m_blockId = getPosition();
        }
        m_session->m_drive.writeBlock(&hdr1, sizeof(hdr1));
        m_session->m_drive.writeBlock(&hdr2, sizeof(hdr2));
        m_session->m_drive.writeBlock(&uhl1, sizeof(uhl1));
        m_session->m_drive.writeImmediateFileMarks(1);
        m_open=true;
      }

      uint32_t WriteFile::getPosition()  {  
        return m_session->m_drive.getPositionInfo().currentPosition;
      }
      
      uint32_t WriteFile::getBlockId()  {  
        return m_blockId;
      }
      
      size_t WriteFile::getBlockSize() {
        return m_currentBlockSize;
      }


      void WriteFile::write(const void *data, const size_t size)  {
        m_session->m_drive.writeBlock(data, size);
        if(size>0) {
          m_nonzeroFileWritten = true;
          m_numberOfBlocks++;
        }
      }

      void WriteFile::close()  {
        if(!m_open) {
          m_session->setCorrupted();
          throw FileClosedTwice();
        }
        if(!m_nonzeroFileWritten) {
          m_session->setCorrupted();
          throw ZeroFileWritten();
        }
        m_session->m_drive.writeImmediateFileMarks(1); // filemark at the end the of data file
        EOF1 eof1;
        EOF2 eof2;
        UTL1 utl1;
        std::stringstream s;
        s << std::hex << m_fileToMigrate.fileid();
        std::string fileId;
        s >> fileId;
        std::transform(fileId.begin(), fileId.end(), fileId.begin(), ::toupper);
        eof1.fill(fileId, m_session->m_vid, m_fileToMigrate.fseq(), m_numberOfBlocks);
        eof2.fill(m_currentBlockSize, m_session->m_compressionEnabled);
        utl1.fill(m_fileToMigrate.fseq(), m_currentBlockSize, m_session->getSiteName(),
            m_session->getHostName(), m_session->m_drive.getDeviceInfo());
        m_session->m_drive.writeBlock(&eof1, sizeof(eof1));
        m_session->m_drive.writeBlock(&eof2, sizeof(eof2));
        m_session->m_drive.writeBlock(&utl1, sizeof(utl1));
        m_session->m_drive.writeImmediateFileMarks(1); // filemark at the end the of trailers
        m_open=false;
      }

      WriteFile::~WriteFile() throw() {
        if(m_open) {
          m_session->setCorrupted();
        }
        m_session->release();
      }

    } //end of namespace tapeFile

  } //end of namespace tape
} //end of namespace castor
