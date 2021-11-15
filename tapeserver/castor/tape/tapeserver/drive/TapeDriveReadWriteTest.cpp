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
#include <iostream>
#include <assert.h>
#include <memory>

namespace {
/*
 * Prints and compares the current position with the expected one. Returns on
 * success (expected value is the same as the actual value), or else fails the 
 * assertion and exits.
 * @param expected_position expected position
 */
void print_and_assert_position(castor::tape::tapeserver::drive::DriveInterface &drive, int expected_position)
{
  int curPos = (int)drive.getPositionInfo().currentPosition;
  std::cout << "CurrentPosition: "  << curPos << " (Expected: " << expected_position << ")" << std::endl;
  assert(expected_position == curPos);
  return;
}

/*
 * Prints and compares the current data read with the expected one. Returns on
 * success (expected value is the same as the actual value), or else fails the 
 * assertion and exits.
 * @param expected_data expected data
 */
void print_and_assert_data(const char * expected_data, const char * actual_data)
{
  std::cout << "Data Read: "  << actual_data << " (Expected: " << expected_data << ")" << std::endl;
  assert(0 == strcmp(expected_data, actual_data));
  return;
}

}

int main ()
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
          /** 
           * We will write on the tape, so prepare 2 blocks
           */
          std::cout << "-- INFO --------------------------------------" << std::endl             
                    << " Rewinding, writing 2 blocks and repositioning on block 2" << std::endl  
                    << "----------------------------------------------" << std::endl;
          drive->rewind();
          /* For some unexplained (TODO) reason, mhvtl does not accept blocks smaller than 4 bytes */
          drive->writeBlock((void *)"X123", 4);
          drive->writeBlock((void *)"Y123", 4);
          /**
           * trying to do position to the block 2.
           */
          drive->positionToLogicalObject(2);
        } catch (std::exception & e) {
          fail = 1;
          std::string temp = e.what();
          std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                    << temp 
                    << "----------------------------------------------" << std::endl;
        }

        try {
          castor::tape::tapeserver::drive::positionInfo posInfo = drive->getPositionInfo();
          std::cout << "-- INFO --------------------------------------" << std::endl 
                    << "  posInfo.currentPosition   : "  << posInfo.currentPosition <<std::endl
                    << "  posInfo.oldestDirtyObject : "<< posInfo.oldestDirtyObject <<std::endl
                    << "  posInfo.dirtyObjectsCount : "<< posInfo.dirtyObjectsCount <<std::endl
                    << "  posInfo.dirtyBytesCount   : "  << posInfo.dirtyBytesCount <<std::endl
                    << "----------------------------------------------" << std::endl;
        } catch (std::exception & e) {
          fail = 1;
          std::string temp = e.what();
          std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                    << temp 
                    << "----------------------------------------------" << std::endl;
        }

        try {  // switch off compression on the drive
          std::cout << "** set density and compression" << std::endl;
          drive->setDensityAndCompression(false);
        } catch (std::exception & e) {
          fail = 1;
          std::string temp = e.what();
          std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                    << temp 
                    << "----------------------------------------------" << std::endl;
        }

        try {
          /**
           * Trying to get compression from the drive-> Read or write should be
           * done before to have something in the data fields.
           */
          castor::tape::tapeserver::drive::compressionStats comp = drive->getCompression();
          std::cout << "-- INFO --------------------------------------" << std::endl
                  << "  fromHost : " << comp.fromHost << std::endl
                  << "  toHost   : " << comp.toHost << std::endl
                  << "  fromTape : " << comp.fromTape << std::endl
                  << "  toTape   : " << comp.toTape << std::endl
                  << "----------------------------------------------" << std::endl;

          std::cout << "** clear compression stats" << std::endl;
          drive->clearCompressionStats();

          comp = drive->getCompression();
          std::cout << "-- INFO --------------------------------------" << std::endl
                  << "  fromHost : " << comp.fromHost << std::endl
                  << "  toHost   : " << comp.toHost << std::endl
                  << "  fromTape : " << comp.fromTape << std::endl
                  << "  toTape   : " << comp.toTape << std::endl
                  << "----------------------------------------------" << std::endl;
        } catch (std::exception & e) {
          fail = 1;
          std::string temp = e.what();
          std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                  << temp
                  << "----------------------------------------------" << std::endl;
        }

        std::vector<std::string> Alerts(drive->getTapeAlerts(drive->getTapeAlertCodes()));
        while (Alerts.size()) {
          std::cout << "Tape alert: " << Alerts.back() << std::endl;
          Alerts.pop_back();
        }

        /**
         * Rewind/Read/Write/Skip Test
         */
        try {

          const size_t count = 10;
          unsigned char data[count];          
          memset(data, 0, count);

          std::cout << "************** BEGIN: Rewind/Read/Write/Skip Test *************" << std::endl;

          std::cout << "Rewinding..." << std::endl;
          drive->rewind(); // go back to the beginning of tape after Victor's positioning
          print_and_assert_position(*drive, 0);

          memset(data, 'a', count-1);
          std::cout << "Writing 1st block (9 a's)..." << std::endl;
          drive->writeBlock((void *)data, count); // write 9 a's + string term
          print_and_assert_position(*drive, 1);

          std::cout << "Writing 1st Synchronous filemark..." << std::endl;
          drive->writeSyncFileMarks(1); // filemark and flush
          print_and_assert_position(*drive, 2);

          memset(data, 'b', count-1);
          std::cout << "Writing 2nd block (9 b's)..." << std::endl;
          drive->writeBlock((void *)data, count); // write 9 b's + string term
          print_and_assert_position(*drive, 3);

          std::cout << "Writing 2nd Synchronous filemark..." << std::endl;
          drive->writeSyncFileMarks(1); // filemark and flush
          print_and_assert_position(*drive, 4);

          memset(data, 'c', count-1);
          std::cout << "Writing 3rd block (9 c's)..." << std::endl;
          drive->writeBlock((void *)data, count); // write 9 c's + string term
          print_and_assert_position(*drive, 5);

          std::cout << "Writing EOD (2 filemarks)..." << std::endl;
          drive->writeSyncFileMarks(2); // EOD and flush  
          print_and_assert_position(*drive, 7);

          std::cout << "Rewinding..." << std::endl;
          drive->rewind(); // go back to the beginning of tape
          print_and_assert_position(*drive, 0);

          std::cout << "Reading back 1st block 9 a's)..." << std::endl;                    
          memset(data, 0, count);
          drive->readBlock((void *)data, count); // read 9 a's + string term
          print_and_assert_position(*drive, 1);
          print_and_assert_data("aaaaaaaaa", (const char *)data);

          std::cout << "Skipping first file mark..." << std::endl;                    
          memset(data, 0, count);
          drive->readBlock((void *)data, count);
          print_and_assert_position(*drive, 2);

          std::cout << "Reading back 2nd block (9 b's)..." << std::endl;                    
          memset(data, 0, count);
          drive->readBlock((void *)data, count); // read 9 b's + string term
          print_and_assert_position(*drive, 3);
          print_and_assert_data("bbbbbbbbb", (const char *)data);

          std::cout << "Skipping first file mark..." << std::endl;                    
          memset(data, 0, count);
          drive->readBlock((void *)data, count);
          print_and_assert_position(*drive, 4);

          std::cout << "Reading back 3rd block (9 c's)..." << std::endl;          
          memset(data, 0, count);
          drive->readBlock((void *)data, count); // read 9 c's + string term
          print_and_assert_position(*drive, 5);
          print_and_assert_data("ccccccccc", (const char *)data);

          std::cout << "Skipping the last two file marks..." << std::endl;          
          memset(data, 0, count);
          drive->readBlock((void *)data, count);
          memset(data, 0, count);
          drive->readBlock((void *)data, count);
          print_and_assert_position(*drive, 7);

          std::cout << "Rewinding..." << std::endl;
          drive->rewind(); // go back to the beginning of tape
          print_and_assert_position(*drive, 0);

          std::cout << "Spacing to the end of media..." << std::endl;
          drive->spaceToEOM();
          print_and_assert_position(*drive, 7);

          std::cout << "Rewinding..." << std::endl;
          drive->rewind(); // go back to the beginning of tape
          print_and_assert_position(*drive, 0);

          std::cout << "Fast spacing to the end of media..." << std::endl;
          drive->fastSpaceToEOM();
          print_and_assert_position(*drive, 7);

          std::cout << "Rewinding..." << std::endl;
          drive->rewind(); // go back to the beginning of tape
          print_and_assert_position(*drive, 0);

          std::cout << "Spacing 2 file marks forward..." << std::endl;
          drive->spaceFileMarksForward(2);
          print_and_assert_position(*drive, 4);

          std::cout << "Spacing 1 file mark backwards..." << std::endl;
          drive->spaceFileMarksBackwards(1);
          print_and_assert_position(*drive, 3);

          std::cout << "Rewinding..." << std::endl;
          drive->rewind(); // go back to the beginning of tape
          print_and_assert_position(*drive, 0);

          std::cout << "Spacing 3 file marks forward..." << std::endl;
          drive->spaceFileMarksForward(3);
          print_and_assert_position(*drive, 6);

          memset(data, 'd', count-1);
          std::cout << "Writing 9 d's..." << std::endl;
          drive->writeBlock((void *)data, count); // write 9 d's + string term
          print_and_assert_position(*drive, 7);

          std::cout << "Writing Asynchronous filemark..." << std::endl;
          drive->writeImmediateFileMarks(1); // buffered filemark
          print_and_assert_position(*drive, 8);

          memset(data, 'e', count-1);
          std::cout << "Writing 9 e's..." << std::endl;
          drive->writeBlock((void *)data, count); // write 9 e's + string term
          print_and_assert_position(*drive, 9);

          std::cout << "Writing Asynchronous EOD..." << std::endl;
          drive->writeImmediateFileMarks(2); // buffered filemarks
          print_and_assert_position(*drive, 11);

          std::cout << "Synch-ing..." << std::endl;
          drive->flush(); // flush buffer with no-op
          print_and_assert_position(*drive, 11);

          std::cout << "Rewinding..." << std::endl;
          drive->rewind(); // go back to the beginning of tape
          print_and_assert_position(*drive, 0);

          for(int i=0; i<9; i++) {
            memset(data, '0'+i, count-1);
            std::cout << "Writing 9 " << i << "'s..." << std::endl;
            drive->writeBlock((void *)data, count);
            print_and_assert_position(*drive, i+1);
          }

          std::cout << "Rewinding..." << std::endl;
          drive->rewind(); // go back to the beginning of tape
          print_and_assert_position(*drive, 0);

          std::cout << "TEST PASSED!" << std::endl;

          std::cout << "************** END: Rewind/Read/Write/Skip Test *************" << std::endl;

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
