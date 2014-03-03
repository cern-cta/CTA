/******************************************************************************
 *                      FileFormatTest.cpp
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
#include <iostream>
#include <assert.h>
#include <fstream>
#include <sstream>
#include <iomanip>

int main(int argc, char* argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <device(ex. /dev/nst0)> <VSN(ex. V92003)>" << std::endl;
    return 1;
  }
    
  int fail = 0;
  castor::tape::System::realWrapper sWrapper;
  castor::tape::SCSI::DeviceVector dl(sWrapper);
  for(castor::tape::SCSI::DeviceVector::iterator i = dl.begin(); i != dl.end(); i++) {
    castor::tape::SCSI::DeviceInfo & dev = (*i);
    std::cout << std::endl << "-- SCSI device: " << dev.sg_dev << " (" << dev.nst_dev << ")" << std::endl;
    if (dev.type == castor::tape::SCSI::Types::tape) {
      castor::tape::drives::Drive dContainer(dev, sWrapper);
      castor::tape::drives::DriveGeneric & drive = dContainer;
      castor::tape::drives::deviceInfo devInfo;
      try {
        devInfo = drive.getDeviceInfo();
        std::cout << "-- INFO --------------------------------------" << std::endl             
                  << "  devInfo.vendor               : '"  << devInfo.vendor << "'" << std::endl
                  << "  devInfo.product              : '" << devInfo.product << "'" << std::endl
                  << "  devInfo.productRevisionLevel : '" << devInfo.productRevisionLevel << "'" << std::endl
                  << "  devInfo.serialNumber         : '" << devInfo.serialNumber << "'" << std::endl
                  << "----------------------------------------------" << std::endl;
      } catch (std::exception & e) {
        fail = 1;
        std::string temp = e.what();
        std::cout << "----------------------------------------------" << std::endl
                  << temp 
                  << "----------------------------------------------" << std::endl;
      }      
      /**
       * File Format Check Test
       */
      if(!strcmp(dev.nst_dev.c_str(),argv[1])) {
        try {
          castor::tape::tapeFile::ReadSession my_sess(drive, argv[2]);
          std::cout << "Read session on " << argv[2] << " (" << argv[1] << ") established." << std::endl;
          int f=0;
          while(!f){
            std::cout << "*******    Menu     ********\n\n";
            std::cout << "1) Read file and dump output\n";
            std::cout << "2) Exit\n";
            std::cout << "Enter your choice (1 or 2): ";

            int choice;
            std::cin >> choice;
            std::cin.ignore();
            
            if(choice==1) {
              castor::tape::tapeFile::FileInfo info;
              std::cout << "Please enter the blockId: ";
              std::string blockId;
              std::cin >> blockId;
              std::cin.ignore();
              std::stringstream blockId_converter(blockId.c_str());
              blockId_converter >> std::hex >> info.blockId;
              std::cout << "Please enter the fseq number of the file: ";
              std::cin >> info.fseq;
              std::cin.ignore();
              std::cout << "Please enter the checksum of the file: ";
              std::string checksum;
              std::cin >> checksum;
              std::cin.ignore();
              std::stringstream checksum_converter(checksum.c_str());
              checksum_converter >> std::hex >> info.checksum;
              std::cout << "Please enter the nsFileId of the file: ";
              std::cin >> info.nsFileId;
              std::cin.ignore();
              std::cout << "Please enter the size of the file: ";
              std::cin >> info.size;
              std::cin.ignore();
              castor::tape::tapeFile::ReadFile file(&my_sess, info, castor::tape::tapeFile::ByBlockId);
              std::cout << "Tape positioned at the beginning of the file\n";
              size_t bs = file.getBlockSize();
              std::cout << "Block size: " << bs << std::endl;
              char *buf = new char[bs];
              std::ofstream out;
              out.open("/tmp/hello", std::ofstream::trunc);
              size_t bytes_read = 0;
              try {
                while(true) {
                  bytes_read = file.read(buf, bs);
                  std::cout << "Reading... ";
                  out.write(buf, bytes_read);
                  std::cout << "DONE\n";
                }
              }
              catch (castor::tape::tapeFile::EndOfFile &e) {
                std::cout << e.what() << std::endl;
              }
              out.close();
              delete[] buf;
            }
            else {
              f=1;
            }
          }
          if(my_sess.isCorrupted()) {
            throw castor::tape::tapeFile::SessionCorrupted();
          }
        } 
        catch (std::exception & e) {
          fail = 1;
          std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                    << e.what() << std::endl
                    << "----------------------------------------------" << std::endl;
        }
      }
    }  
  }
  return fail;
}
