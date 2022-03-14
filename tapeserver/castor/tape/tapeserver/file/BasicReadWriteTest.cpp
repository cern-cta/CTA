/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Test main program. For development use.
 */

#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "File.hpp"
#include "../daemon/VolumeInfo.hpp"
#include "scheduler/ArchiveJob.hpp"
#include <iostream>
#include <assert.h>
#include <memory>
#include <string>
#include <list>

char gen_random() {
    static const char alphanum[] =
        "123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    return alphanum[rand() % (sizeof(alphanum) - 1)];
}

enum {
    BLOCK_TEST,
    FILE_TEST,
    RAO_TEST
};

int test = RAO_TEST;

class BasicRetrieveJob: public cta::RetrieveJob {
  public:
    BasicRetrieveJob() : cta::RetrieveJob(nullptr,
    cta::common::dataStructures::RetrieveRequest(), 
    cta::common::dataStructures::ArchiveFile(), 1,
    cta::PositioningMethod::ByBlock) {}
  };

class BasicArchiveJob: public cta::ArchiveJob {
public:
  BasicArchiveJob(): cta::ArchiveJob(nullptr, 
      *((cta::catalogue::Catalogue *)nullptr), cta::common::dataStructures::ArchiveFile(),
      "", cta::common::dataStructures::TapeFile()) {
  }
};

std::vector<std::string> split(std::string to_split, std::string delimiter) {
  std::vector<std::string> toBeReturned;
  int pos = 0;
  while ((pos = to_split.find(delimiter)) != -1) {
    std::string token = to_split.substr(0, pos);
    toBeReturned.push_back(token);
    to_split.erase(0, pos + delimiter.length());
  }
  toBeReturned.push_back(to_split);
  return toBeReturned;
}

int main (int argc, char *argv[])
{
  int fail = 0;
  castor::tape::System::realWrapper sWrapper;
  castor::tape::SCSI::DeviceVector dl(sWrapper);
  for(castor::tape::SCSI::DeviceVector::iterator i = dl.begin();
          i != dl.end(); ++i) {
    castor::tape::SCSI::DeviceInfo & dev = (*i);
    std::cout << std::endl << "-- SCSI device: " 
              << dev.sg_dev << " (" << dev.nst_dev << ")" << std::endl;
    if (dev.type == castor::tape::SCSI::Types::tape) {
      try {
        // Create drive object and open tape device
        std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> drive(
          castor::tape::tapeserver::drive::createDrive(dev, sWrapper));
     
        /**
         * From now we could use generic SCSI request for the drive object.
         * We should be aware that there might be a problem with tape in the 
         * drive for example incompatible media installed.
         */
        
        try {
          /**
           * Gets generic device info for the drive object.
           */
          castor::tape::tapeserver::drive::deviceInfo devInfo;  
          devInfo = drive->getDeviceInfo();
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
                    << "-- INFO --------------------------------------" << std::endl;
          continue;
        }  
        
        try {
          /**
           * Checks if the drive ready to use the tape installed loaded into it.
           */
          drive->waitUntilReady(5); 
        } catch(cta::exception::Exception &ne) {
          std::string temp=ne.getMessage().str();
          fail = 1;
          std::cout << "----------------------------------------------" << std::endl
                    << temp  << std::endl
                    << "----------------------------------------------" << std::endl;
          continue;  
        }

        drive->enableCRC32CLogicalBlockProtectionReadWrite();

        try {

            if (test == BLOCK_TEST) {
                const size_t count = 10;
                unsigned char data[count];          
                memset(data, 0, count);

                std::cout << "Rewinding..." << std::endl;
                drive->rewind(); // go back to the beginning of tape after Victor's positioning

                memset(data, 'a', count-1);
                std::cout << "Writing 1st block (9 a's)..." << std::endl;
                drive->writeBlock((void *)data, count); // write 9 a's + string term

                std::cout << "Writing EOD (2 filemarks)..." << std::endl;
                drive->writeSyncFileMarks(2); // EOD and flush  

                std::cout << "Rewinding..." << std::endl;
                drive->rewind(); // go back to the beginning of tape

                std::cout << "Reading back 1st block 9 a's)..." << std::endl;                    
                memset(data, 0, count);
                drive->readBlock((void *)data, count); // read 9 a's + string term

                std::cout << "Rewinding..." << std::endl;
                drive->rewind(); // go back to the beginning of tape

            }
            else if (test == FILE_TEST) {
                drive->rewind();

                castor::tape::tapeFile::LabelSession *ls;
                std::string label = "TW8510";
                ls = new castor::tape::tapeFile::LabelSession(*drive, label, true);
                delete ls;

                castor::tape::tapeserver::daemon::VolumeInfo m_volInfo;
                m_volInfo.vid = label;
                m_volInfo.nbFiles = 0;
                m_volInfo.mountType = cta::common::dataStructures::MountType::ArchiveForUser;

                castor::tape::tapeFile::WriteSession *ws;
                ws = new castor::tape::tapeFile::WriteSession(*drive, m_volInfo, 0, true, true);

                uint32_t block_size = 262144;
                uint32_t no_blocks = 100;

                // Write test files ( ! add stop condition)
                int j = 1;
                while (true) {
                  BasicArchiveJob fileToMigrate;
                  fileToMigrate.archiveFile.fileSize = block_size * 100;
                  fileToMigrate.archiveFile.archiveFileID = j;
                  fileToMigrate.tapeFile.fSeq = j;
                  std::unique_ptr<castor::tape::tapeFile::WriteFile> wf;
                  wf.reset(new castor::tape::tapeFile::WriteFile(ws, fileToMigrate, block_size));

                  std::string testString = "";
                  for (uint32_t i = 0; i < block_size - 5; i++)
                      testString += gen_random();
                  for (uint32_t k = 0; k < no_blocks; k++) {
                      wf->write(testString.c_str(),testString.size());  
                  }

                  wf->close();
                  j++;
                }

                drive->rewind();

                // Now read a random file
                castor::tape::tapeFile::ReadSession *rs;
                rs = new castor::tape::tapeFile::ReadSession(*drive, m_volInfo, true);

                BasicRetrieveJob fileToRecall;
                fileToRecall.selectedCopyNb=1;
                fileToRecall.archiveFile.tapeFiles.push_back(cta::common::dataStructures::TapeFile());
                fileToRecall.selectedTapeFile().blockId = 110; // here should be the block ID of HDR1
                fileToRecall.selectedTapeFile().fSeq = 2;
                fileToRecall.retrieveRequest.archiveFileID = 2;
                fileToRecall.positioningMethod = cta::PositioningMethod::ByBlock;

                castor::tape::tapeFile::ReadFile rf(rs, fileToRecall);
                size_t bs = rf.getBlockSize();
                char *data = new char[bs+1];
                j = 0;
                while(j < 100) {
                    rf.read(data, bs);
                    j++;
                    std::cout << data << std::endl;
                }
            }
            else if (test == RAO_TEST) {
                if (argc != 2) {
                    std::cout << "For RAO testing the first parameter should be "
                            "the file containing the sequential order" << std::endl;
                }
                else {
                    drive->rewind();
                    
                    std::list<castor::tape::SCSI::Structures::RAO::blockLims> files;
                    std::ifstream ns_file_pick(argv[1]);
                    if (ns_file_pick.is_open()) {
                      std::string line;
                      while (getline(ns_file_pick, line)) {
                        std::vector<std::string> tokens = split(line, ":");
                        castor::tape::SCSI::Structures::RAO::blockLims lims;
                        std::cout << tokens[0].c_str() << std::endl;
                        tokens[0] += '\0';
                        strcpy((char*)lims.fseq, tokens[0].c_str());
                        lims.begin = std::stoi(tokens[1]);
                        lims.end = std::stoi(tokens[2]);                        
                        files.push_back(lims);
                      }
                    }
                    else {
                      throw -1;
                    }
                    castor::tape::SCSI::Structures::RAO::udsLimits limits = drive->getLimitUDS();
                    drive->queryRAO(files, limits.maxSupported);
                }
            }

        } catch (std::exception & e) {
          fail = 1;
          std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                    << e.what() << std::endl
                    << "----------------------------------------------" << std::endl;
        }
      } catch(cta::exception::Exception &ne) {
        std::string temp=ne.getMessage().str();
        fail = 1;
        std::cout << "----------------------------------------------" << std::endl
                  << temp  << std::endl
                  << "-- object ------------------------------------" << std::endl;
        break;  
      } 
    }  
  }
  return fail;
}
