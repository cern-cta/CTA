/******************************************************************************
 *                      TapeCopyTest.cpp
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

/**
 * Test main program. For development use.
 */

#include "../system/Wrapper.hpp"
#include "../SCSI/Device.hpp"
#include "File.hpp"
#include "../drive/Drive.hpp"
#include <assert.h>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <memory>

//- 1   1 V92002       1 00000000                  748 100         adler32 108deddb           5001074305 /castor/cern.ch/de

void fill_info(castor::tape::AULFile::FileInfo *fileInfo, const std::string volId, const uint32_t fseq) {
  std::string disabled, copy_no, seg_no, volId_str, fseq_str, blockId_str, size_str, compr_fact, checksum_name, checksum_str, nsFileId_str, path;
  std::ifstream resolv;
  std::string filename("nslisttape_" + volId + ".txt");
  resolv.exceptions(std::ifstream::failbit|std::ifstream::badbit);
  try {
    resolv.open(filename.c_str(), std::ifstream::in);
    std::string buf;
    for(unsigned int i=0; i<fseq; i++) {
      std::getline(resolv, buf);
    }
    resolv >> disabled >> copy_no >> seg_no >> volId_str >> fseq_str >> blockId_str >> size_str >> compr_fact >> checksum_name >> checksum_str >> nsFileId_str >> path;
    
    std::stringstream fseq_conv; fseq_conv << std::dec << fseq_str;
    std::stringstream blockId_conv; blockId_conv << std::hex << blockId_str;
    std::stringstream size_conv; size_conv << std::dec << size_str;
    std::stringstream checksum_conv; checksum_conv << std::hex << checksum_str;
    std::stringstream nsFileId_conv; nsFileId_conv << std::dec << nsFileId_str;
    
    fseq_conv >> fileInfo->fseq;
    blockId_conv >> fileInfo->blockId;
    size_conv >> fileInfo->size;
    checksum_conv >> fileInfo->checksum;
    nsFileId_conv >> fileInfo->nsFileId;
    
    resolv.close();
  }
  catch (std::ifstream::failure e) {
    throw castor::exception::Exception("Error opening, reading or closing nslisttape_*");
  }
}

int main(int argc, char* argv[])
{
  if (argc != 6) {
    std::cerr << "Usage: " << argv[0] << " <src device(ex. /dev/nst0)> <src VSN(ex. V92003)> <dst device(ex. /dev/nst1)> <dst VSN(ex. V92001)> <number of files to copy>" << std::endl;
    return 1;
  }
  
  std::string src_device(argv[1]);
  std::string src_tape(argv[2]);
  std::string dst_device(argv[3]);
  std::string dst_tape(argv[4]);
  const unsigned int no_of_files = (unsigned int)atoi(argv[5]);
  
  int fail = 0;
  
  castor::tape::System::realWrapper sWrapper;
  castor::tape::SCSI::DeviceVector dl(sWrapper);
  
  castor::tape::AULFile::ReadSession *read_sess = NULL;
  castor::tape::AULFile::WriteSession *write_sess = NULL;
  castor::tape::AULFile::LabelSession *label_sess = NULL;
  
  castor::tape::SCSI::DeviceInfo read_dev;
  castor::tape::SCSI::DeviceInfo write_dev;
  
  for(castor::tape::SCSI::DeviceVector::iterator i = dl.begin(); i != dl.end(); i++) {
    if((*i).type == castor::tape::SCSI::Types::tape and !strcmp((*i).nst_dev.c_str(), src_device.c_str())) {
      read_dev = (*i);
    }
    else if ((*i).type == castor::tape::SCSI::Types::tape and !strcmp((*i).nst_dev.c_str(), dst_device.c_str())) {
      write_dev = (*i);
    }
  }
  
  castor::tape::drives::Drive read_dContainer(read_dev, sWrapper);
  castor::tape::drives::DriveGeneric & read_drive = read_dContainer;
  castor::tape::drives::Drive write_dContainer(write_dev, sWrapper);
  castor::tape::drives::DriveGeneric & write_drive = write_dContainer;
  
  try {
    label_sess = new castor::tape::AULFile::LabelSession(write_drive, dst_tape, true);
    std::cout << "Label session on " << dst_tape << " (" << dst_device << ") established." << std::endl;
  } 
  catch (std::exception & e) {
    fail = 1;
    std::cout << "-- EXCEPTION ---------------------------------" << std::endl
              << e.what() << std::endl
              << "----------------------------------------------" << std::endl;
  }
  
  try {
    read_sess = new castor::tape::AULFile::ReadSession(read_drive, src_tape);
    std::cout << "Read session on " << src_tape << " (" << src_device << ") established." << std::endl;
    write_sess = new castor::tape::AULFile::WriteSession(write_drive, dst_tape, 0, false);
    std::cout << "Write session on " << dst_tape << " (" << dst_device << ") established." << std::endl;
  } 
  catch (std::exception & e) {
    fail = 1;
    std::cout << "-- EXCEPTION ---------------------------------" << std::endl
              << e.what() << std::endl
              << "----------------------------------------------" << std::endl;
  }
  
  /**
   * TapeCopyTest 
   */
  if(read_sess!=NULL and write_sess!=NULL) {
    for(unsigned int i=0; i<no_of_files; i++) {
      try {
        castor::tape::AULFile::FileInfo info;
        fill_info(&info, src_tape, i);
        castor::tape::AULFile::ReadFile input_file(read_sess, info, castor::tape::AULFile::ByBlockId);
        size_t blockSize = input_file.getBlockSize();
        castor::tape::AULFile::WriteFile output_file(write_sess, info, blockSize);
        std::auto_ptr<char> buf(new char[blockSize]);
        size_t bytes_read = 0;
        try {
          while(true) {
            bytes_read = input_file.read(buf.get(), blockSize);
            output_file.write(buf.get(), bytes_read);
          }
        }
        catch (castor::tape::AULFile::EndOfFile &e) {
        }
        output_file.close();
      }
      catch (std::exception & e) {
        fail = 1;
        std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                  << e.what() << std::endl
                  << "----------------------------------------------" << std::endl;
      }
    }
    delete read_sess;
    delete write_sess;
  }
  return fail;
}
