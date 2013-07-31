// ----------------------------------------------------------------------
// File: SCSI/Constants.hh
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

#pragma once

#include <string>
#include <stdint.h>

namespace SCSI {
  /* Extracted from linux kernel's include/scsi/scsi.h. System-level include 
   is less complete */
  class Types {
  public:

    enum {
      disk = 0x00,
      tape = 0x01,
      printer = 0x02,
      processor = 0x03, /* HP scanners use this */
      worm = 0x04, /* Treated as ROM by our system */
      rom = 0x05,
      scanner = 0x06,
      mod = 0x07, /* Magneto-optical disk -
                          * - treated as TYPE_DISK */
      mediumChanger = 0x08,
      comm = 0x09, /* Communications device */
      raid = 0x0c,
      enclosure = 0x0d, /* Enclosure Services Device */
      rbc = 0x0e,
      noLun = 0x7f
    };
  };
  
  class Commands {
  public:

    enum {
      /*
       *      SCSI opcodes, taken from linux kernel sources
       *      Linux kernel's is more complete than system's
       *      includes.
       */
      TEST_UNIT_READY                               = 0x00,
      REZERO_UNIT                                   = 0x01,
      REQUEST_SENSE                                 = 0x03,
      FORMAT_UNIT                                   = 0x04,
      READ_BLOCK_LIMITS                             = 0x05,
      REASSIGN_BLOCKS                               = 0x07,
      INITIALIZE_ELEMENT_STATUS                     = 0x07,
      READ_6                                        = 0x08,
      WRITE_6                                       = 0x0a,
      SEEK_6                                        = 0x0b,
      READ_REVERSE                                  = 0x0f,
      WRITE_FILEMARKS                               = 0x10,
      SPACE                                         = 0x11,
      INQUIRY                                       = 0x12,
      RECOVER_BUFFERED_DATA                         = 0x14,
      MODE_SELECT                                   = 0x15,
      RESERVE                                       = 0x16,
      RELEASE                                       = 0x17,
      COPY                                          = 0x18,
      ERASE                                         = 0x19,
      MODE_SENSE                                    = 0x1a,
      START_STOP                                    = 0x1b,
      RECEIVE_DIAGNOSTIC                            = 0x1c,
      SEND_DIAGNOSTIC                               = 0x1d,
      ALLOW_MEDIUM_REMOVAL                          = 0x1e,
      
      READ_FORMAT_CAPACITIES                        = 0x23,
      SET_WINDOW                                    = 0x24,
      READ_CAPACITY                                 = 0x25,
      READ_10                                       = 0x28,
      WRITE_10                                      = 0x2a,
      SEEK_10                                       = 0x2b,
      POSITION_TO_ELEMENT                           = 0x2b,
      WRITE_VERIFY                                  = 0x2e,
      VERIFY                                        = 0x2f,
      SEARCH_HIGH                                   = 0x30,
      SEARCH_EQUAL                                  = 0x31,
      SEARCH_LOW                                    = 0x32,
      SET_LIMITS                                    = 0x33,
      PRE_FETCH                                     = 0x34,
      READ_POSITION                                 = 0x34,
      SYNCHRONIZE_CACHE                             = 0x35,
      LOCK_UNLOCK_CACHE                             = 0x36,
      READ_DEFECT_DATA                              = 0x37,
      MEDIUM_SCAN                                   = 0x38,
      COMPARE                                       = 0x39,
      COPY_VERIFY                                   = 0x3a,
      WRITE_BUFFER                                  = 0x3b,
      READ_BUFFER                                   = 0x3c,
      UPDATE_BLOCK                                  = 0x3d,
      READ_LONG                                     = 0x3e,
      WRITE_LONG                                    = 0x3f,
      CHANGE_DEFINITION                             = 0x40,
      WRITE_SAME                                    = 0x41,
      UNMAP                                         = 0x42,
      READ_TOC                                      = 0x43,
      READ_HEADER                                   = 0x44,
      GET_EVENT_STATUS_NOTIFICATION                 = 0x4a,
      LOG_SELECT                                    = 0x4c,
      LOG_SENSE                                     = 0x4d,
      XDWRITEREAD_10                                = 0x53,
      MODE_SELECT_10                                = 0x55,
      RESERVE_10                                    = 0x56,
      RELEASE_10                                    = 0x57,
      MODE_SENSE_10                                 = 0x5a,
      PERSISTENT_RESERVE_IN                         = 0x5e,
      PERSISTENT_RESERVE_OUT                        = 0x5f,
      VARIABLE_LENGTH_CMD                           = 0x7f,
      REPORT_LUNS                                   = 0xa0,
      SECURITY_PROTOCOL_IN                          = 0xa2,
      MAINTENANCE_IN                                = 0xa3,
      MAINTENANCE_OUT                               = 0xa4,
      MOVE_MEDIUM                                   = 0xa5,
      EXCHANGE_MEDIUM                               = 0xa6,
      READ_12                                       = 0xa8,
      WRITE_12                                      = 0xaa,
      READ_MEDIA_SERIAL_NUMBER                      = 0xab,
      WRITE_VERIFY_12                               = 0xae,
      VERIFY_12                                     = 0xaf,
      SEARCH_HIGH_12                                = 0xb0,
      SEARCH_EQUAL_12                               = 0xb1,
      SEARCH_LOW_12                                 = 0xb2,
      SECURITY_PROTOCOL_OUT                         = 0xb5,
      READ_ELEMENT_STATUS                           = 0xb8,
      SEND_VOLUME_TAG                               = 0xb6,
      WRITE_LONG_2                                  = 0xea,
      EXTENDED_COPY                                 = 0x83,
      RECEIVE_COPY_RESULTS                          = 0x84,
      ACCESS_CONTROL_IN                             = 0x86,
      ACCESS_CONTROL_OUT                            = 0x87,
      READ_16                                       = 0x88,
      WRITE_16                                      = 0x8a,
      READ_ATTRIBUTE                                = 0x8c,
      WRITE_ATTRIBUTE                               = 0x8d,
      VERIFY_16                                     = 0x8f,
      SYNCHRONIZE_CACHE_16                          = 0x91,
      WRITE_SAME_16                                 = 0x93,
      SERVICE_ACTION_IN                             = 0x9e
    };
  }; // class commands
  class OtherConstants {
  public:
    enum {
      /* values for service action in */
      SAI_READ_CAPACITY_16                          = 0x10,
      SAI_GET_LBA_STATUS                            = 0x12,
      /* values for VARIABLE_LENGTH_CMD service action codes
       * see spc4r17 Section D.3.5, table D.7 and D.8 */
      VLC_SA_RECEIVE_CREDENTIAL                     = 0x1800,
      /* values for maintenance in */
      MI_REPORT_IDENTIFYING_INFORMATION             = 0x05,
      MI_REPORT_TARGET_PGS                          = 0x0a,
      MI_REPORT_ALIASES                             = 0x0b,
      MI_REPORT_SUPPORTED_OPERATION_CODES           = 0x0c,
      MI_REPORT_SUPPORTED_TASK_MANAGEMENT_FUNCTIONS = 0x0d,
      MI_REPORT_PRIORITY                            = 0x0e,
      MI_REPORT_TIMESTAMP                           = 0x0f,
      MI_MANAGEMENT_PROTOCOL_IN                     = 0x10,
      /* values for maintenance out */
      MO_SET_IDENTIFYING_INFORMATION                = 0x06,
      MO_SET_TARGET_PGS                             = 0x0a,
      MO_CHANGE_ALIASES                             = 0x0b,
      MO_SET_PRIORITY                               = 0x0e,
      MO_SET_TIMESTAMP                              = 0x0f,
      MO_MANAGEMENT_PROTOCOL_OUT                    = 0x10,
      /* values for variable length command */
      XDREAD_32                                     = 0x03,
      XDWRITE_32                                    = 0x04,
      XPWRITE_32                                    = 0x06,
      XDWRITEREAD_32                                = 0x07,
      READ_32                                       = 0x09,
      VERIFY_32                                     = 0x0a,
      WRITE_32                                      = 0x0b,
      WRITE_SAME_32                                 = 0x0d,
      
      /* Values for T10/04-262r7 */
      ATA_16		                            = 0x85,	/* 16-byte pass-thru */
      ATA_12		                            = 0xa1	/* 12-byte pass-thru */
    };
  }; // class OtherConstans
  
  /**
   * Helper function turning tape alerts to strings.
   */
  std::string tapeAlertToString(uint16_t parameterCode);
  
  class Status {
  public:
    enum {
      GOOD = 0x00,
      CHECK_CONDITION = 0x02,
      CONDITION_MET = 0x04,
      BUSY = 0x08,
      RESERVATION_CONFLICT = 0x18,
      TASK_SET_FULL = 0x28,
      ACA_ACTIVE = 0x30,
      TASK_ABORTED = 0x40
    };
  };
  
  /**
   * Helper function turning SCSI status to string
   */
  std::string statusToString(unsigned char status);
  
  class logSensePages {
  public:
    enum {
      tapeAlert = 0x2e,
      sequentialAccessDevicePage = 0x0C
    };
  };
  /**
   * Sun StorageTekTM T10000 Tape Drive Fibre Channel Interface Reference Manual
   */
  class sequentialAccessDevicePage {
  public:
    enum {
      receivedFromInitiator = 0x0000, // write command
      writtenOnTape         = 0x0001,
      readFromTape          = 0x0002,
      readByInitiator       = 0x0003,
      cleaning              = 0x0100, // 000 no cleaning required, 001 cleaning required
      leftOnTape            = 0x8000  // number of 4k bytes left on tape from the current position
    };
  };
}; // namespace SCSI
