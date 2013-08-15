// ----------------------------------------------------------------------
// File: SCSI/TapeDriveInfo.cc
// Author: Eric Cano - CERN
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

/**
 * Test main program. For development use.
 */

#include "../System/Wrapper.hh"
#include "../SCSI/Device.hh"
#include "Drive.hh"
#include <iostream>

int main ()
{
  Tape::System::realWrapper sWrapper;
  SCSI::DeviceVector dl(sWrapper);
  for(SCSI::DeviceVector::iterator i = dl.begin();
          i != dl.end(); i++) {
    SCSI::DeviceInfo & dev = (*i);
    std::cout << std::endl << "-- SCSI device: " 
              << dev.sg_dev << " (" << dev.nst_dev << ")" << std::endl;
    if (dev.type == SCSI::Types::tape) {
      Tape::Drive *drive = new Tape::Drive(dev, sWrapper);
      Tape::deviceInfo devInfo;

      try {
        devInfo = drive->getDeviceInfo();
        std::cout << "-- INFO --------------------------------------" << std::endl             
                  << "  devInfo.vendor               : '"  << devInfo.vendor << "'" << std::endl
                  << "  devInfo.product              : '" << devInfo.product << "'" << std::endl
                  << "  devInfo.productRevisionLevel : '" << devInfo.productRevisionLevel << "'" << std::endl
                  << "  devInfo.serialNumber         : '" << devInfo.serialNumber << "'" << std::endl
                  << "----------------------------------------------" << std::endl;
      } catch (std::exception & e) {
        std::string temp = e.what();
        std::cout << "----------------------------------------------" << std::endl
                  << temp 
                  << "----------------------------------------------" << std::endl;
      }
      /* here we detect drive type and switch to appropriate class */
      if (std::string::npos != devInfo.product.find("T10000")) {
        delete drive;
        drive = new Tape::DriveT10000(dev, sWrapper);
        std::cout << "** Switch to DriveT10000" << std::endl;
      }
      
      try {
        /**
         * trying to do position to the block 2.
         * The tape should be mounted and have at least 2 blocks written.
         */
        drive->positionToLogicalObject(2);
      } catch (std::exception & e) {
        std::string temp = e.what();
        std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                  << temp 
                  << "----------------------------------------------" << std::endl;
      }
      
      try {
        Tape::positionInfo posInfo = drive->getPositionInfo();
        std::cout << "-- INFO --------------------------------------" << std::endl 
                  << "  posInfo.currentPosition   : "  << posInfo.currentPosition <<std::endl
                  << "  posInfo.oldestDirtyObject : "<< posInfo.oldestDirtyObject <<std::endl
                  << "  posInfo.dirtyObjectsCount : "<< posInfo.dirtyObjectsCount <<std::endl
                  << "  posInfo.dirtyBytesCount   : "  << posInfo.dirtyBytesCount <<std::endl
                  << "----------------------------------------------" << std::endl;
      } catch (std::exception & e) {
        std::string temp = e.what();
        std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                  << temp 
                  << "----------------------------------------------" << std::endl;
      }
      
      try {  // switch off compression on the drive
        std::cout << "** set density and compression" << std::endl;
        drive->setDensityAndCompression(false);
      } catch (std::exception & e) {
        std::string temp = e.what();
        std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                  << temp 
                  << "----------------------------------------------" << std::endl;
      }

      try {
        /**
         * Trying to get compression from the drive. Read or write should be
         * done before to have something in the data fields.
         */
        Tape::compressionStats comp = drive->getCompression();
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
        std::string temp = e.what();
        std::cout << "-- EXCEPTION ---------------------------------" << std::endl
                << temp
                << "----------------------------------------------" << std::endl;
      }
      
      std::vector<std::string> Alerts(drive->getTapeAlerts());
      while (Alerts.size()) {
        std::cout << "Tape alert: " << Alerts.back() << std::endl;
        Alerts.pop_back();
      }
      
      //drive->SCSI_inquiry();  we use INQUIRY for tapes in getDeviceInfo
    }  
  }
}
