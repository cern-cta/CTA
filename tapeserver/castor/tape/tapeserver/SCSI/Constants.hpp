/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <string>
#include <stdint.h>

namespace castor::tape::SCSI {

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
      MODE_SELECT_6                                 = 0x15,
      RESERVE                                       = 0x16,
      RELEASE                                       = 0x17,
      COPY                                          = 0x18,
      ERASE                                         = 0x19,
      MODE_SENSE                                    = 0x1a,
      MODE_SENSE_6                                  = 0x1a, 
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
      LOCATE_10                                     = 0x2b,
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
  
  /**
   * Helper function turning tape alerts to mixed case compact strings.
   */
  std::string tapeAlertToCompactString(uint16_t parameterCode);

  /**
   * Checks if the tape alert is critical for the writing session.
   * @param code The tape alert parameter code.
   * @return True if it is critical for the writing session or true otherwise.
   */
  bool isTapeAlertCriticalForWrite(const uint16_t code);
  
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
    } Status_t;
  };
 
  class HostStatus {
  public:
    enum {
      OK          = 0x00, /* NO error */
      NO_CONNECT  = 0x01, /* Couldn't connect before timeout period */
      BUS_BUSY    = 0x02, /* BUS stayed busy through time out period */
      TIME_OUT    = 0x03, /* TIMED OUT for other reason (often this an
                           *  unexpected device selection timeout) */
      BAD_TARGET  = 0x04, /* BAD target, device not responding */
      ABORT       = 0x05, /* Told to abort for some other reason. From lk
                           * 2.4.15 the SCSI subsystem supports 16 byte commands
                           * however few adapter drivers do. Those HBA drivers
                           * that don't support 16 byte commands will yield this
                           * error code if a 16 byte command is passed to a SCSI
                           *  device they control. */
      PARITY      = 0x06, /* Parity error. Older SCSI parallel buses have a
                           * parity bit for error detection. This probably
                           * indicates a cable or termination problem. */
      ERROR       = 0x07, /* Internal error detected in the host adapter. This
                           * may not be fatal (and the command may have
                           * succeeded). The aic7xxx and sym53c8xx adapter
                           * drivers sometimes report this for data underruns or
                           * overruns */
      RESET       = 0x08, /* The SCSI bus (or this device) has been reset. Any
                           * SCSI device on a SCSI bus is capable of instigating
                           *  a reset. */
      BAD_INTR    = 0x09, /* Received an unexpected interrupt */
      PASSTHROUGH = 0x0a, /* Force command past mid-level */
      SOFT_ERROR  = 0x0b, /* The low-level driver wants a retry */
    } HostStatus_t;
  };

  class DriverStatus {
  public:
    enum {
      OK      = 0x00, /* Typically no suggestion */
      BUSY    = 0x01,
      SOFT    = 0x02,
      MEDIA   = 0x03,
      ERROR   = 0x04,
      INVALID = 0x05,
      TIMEOUT = 0x06, /* Adapter driver is unable to control the SCSI bus
                       * to its is setting its devices offline
                       * (and giving up) */
      HARD    = 0x07,
      SENSE   = 0x08, /* Implies sense_buffer output */
      MASK    = 0x0f  /* The mask for driver status codes */
    } DriverStatus_t;
  };

  class DriverStatusSuggest {
  public:
    enum {
      RETRY = 0x10,
      ABORT = 0x20,
      REMAP = 0x30,
      DIE   = 0x40,
      SENSE = 0x80,
      MASK  = 0xf0    /* The mask for driver status suggestion codes */
    } DriverStatusSuggest_t;
  };

  /**
   * Helper function turning SCSI status to string
   */
  std::string statusToString(unsigned char status);

  /**
   * Turn a SCSI host_status code into a string
   * @param  The host status
   * @return The string representation for the host status
   */
  std::string hostStatusToString(const unsigned short int hostStatus);

  /**
   * Turn a SCSI driver_status code into a string
   * @param  The driver status
   * @return The string representation for the driver status
   */
  std::string driverStatusToString(const unsigned short int driverStatus);

  /**
   * Turn a SCSI driver_status code suggestions into a string
   * @param  The driver status
   * @return The string representation for the driver status code suggestions
   */
  std::string driverStatusSuggestionsToString(
    const unsigned short int driverStatus);

  namespace encryption {
    const unsigned int ENC_KEY_LENGTH = 0x20;

    class spinSecurityProtocolPages {
    public:
      enum {
        securityProtocolInformation = 0x00,
        tapeDataEncrytpion = 0x20
      };
    };

    class spoutSecurityProtocolPages {
    public:
      enum{
        tapeDataEncryption = 0x20
      };
    };

    class spoutSecurityProtocolSpecificPages {
    public:
      enum {
        setDataEncryptionPage = 0x10,
        saEncapsulationPage   = 0x11 // not supported by IBM or Oracle
      };
    };

    class encryptionNexusScopes {
    public:
      enum {
        scopePublic     = 0,/* All fields other than the scope field and LOCK bit shall be ignored.
                                The I_T nexus shall use data encryption parameters that are shared
                                by other I_T nexuses. If no I_T nexuses are sharing data encryption
                                parameters, the device server shall use default data encryption parameters. */
        scopeLocal      = 1,/* The data encryption parameters are unique to the I_T nexus associated
                               with the SECURITY PROTOCOL OUT command and shall not be shared
                               with other I_T nexuses.*/
        scopeAllITNexus = 2 /* The data encryption parameters shall be shared with all I_T nexuses. */
      };
    };

    class checkExternalEncryptionMode {
    public:
      enum {
        vendorSpecific    = 0,  /* Vendor specific */
        noEncryptionCheck = 1,  /* Do not check the encryption mode that was in use when the block
                                   was written to the medium.*/
        checkExternal     = 2,  /* On read and verify commands, check the encryption mode that
                                   was in use when the block was written to the medium. Report an
                                   error if the block was written in EXTERNAL mode */
        checkEncrypt      = 3   /* On read and verify commands, check the encryption mode that
                                   was in use when the block was written to the medium. Report
                                   an error if the block was written in ENCRYPT mode */
      };
    };

    class rawDecryptionModeControl {
    public:
      enum {
        rdmcDefault   = 0,      /* The device server shall mark each encrypted block per the default
                                 setting for the algorithm  */
        // value 1 is reserved, according to SSC-3
        rdmcUnprotect = 2,    /* The device server shall mark each encrypted block written to the
                                 medium in a format specific manner as enabled for raw decryption
                                 mode operations. */
        rdmcProtect   = 3     /* The device server shall mark each encrypted block written to the
                                 medium in a format specific manner as disabled for raw decryption
                                 mode operations. */
      };
    };

    class encryptionModes {
    public:
      enum {
        modeDisable  = 0,   /* Data encryption is disabled. */
        modeExternal = 1,   /* The data associated with the WRITE(6) and WRITE(16) commands
                               has been encrypted by a system that is compatible with the
                               algorithm specified by the ALGORITHM INDEX field. */
        modeEncrypt  = 2    /* The device server shall encrypt all data that it receives for
                               a WRITE(6) or WRITE(16) command using the algorithm specified
                               in the ALGORITHM INDEX field and the key specified in the KEY field. */
      };
    };

    class decryptionModes {
    public:
      enum {
        modeDisable = 0,  /* Data decryption is disabled. If the device server encounters an encrypted
                             logical block while reading, it shall not allow access to the data. */
        modeRaw     = 1,  /* Data decryption is disabled. If the device server encounters an encrypted
                             logical block while reading, it shall pass the encrypted block to the host
                             without decrypting it. The encrypted block may contain data that is not user data. */
        modeDecrypt = 2,  /* The device server shall decrypt all data that is read from the medium when
                             processing a READ(6), READ(16), READ REVERSE(6), READ REVERSE(16), or
                             RECOVER BUFFERED DATA command or verified when processing a VERIFY(6)
                             or VERIFY(16) command. The data shall be decrypted using the algorithm specified
                             in the ALGORITHM INDEX field and the key specified in the KEY field.*/
        modeMixed   = 3   /* The device server shall decrypt all data that is read from the medium that
                             the device server determines was encrypted when processing a
                             READ(6), READ(16), READ REVERSE(6), READ REVERSE(16), or RECOVER BUFFERED DATA
                             command or verified when processing a VERIFY(6) or VERIFY(16) command.
                             The data shall be decrypted using the algorithm specified in the ALGORITHM INDEX field
                             and the key specified in the KEY field.
                             If the device server encounters unencrypted data when processing a READ(6), READ(16),
                             READ REVERSE(6), READ REVERSE(16), RECOVER BUFFERED DATA, VERIFY(6), or VERIFY(16)
                             command, the data shall be processed without decrypting. */
      };
    };

    class keyFormatTypes {
    public:
      enum {
        keyFormatNormal    = 0, /* The KEY field contains the key to be used to encrypt or decrypt data. */
        keyFormatReference = 1, /* The KEY field contains a vendor-specific key reference. */
        keyFormatWrapped   = 2, /* The KEY field contains the key wrapped by the device server public key. */
        keyFormatESPSCSI   = 3  /* The KEY field contains a key that is encrypted using ESP-SCSI. */
      };
    };

    class keyDescriptorTypes {
    public:
      enum {
        ukad  = 0,  /* Unauthenticated Key Associated Data */
        akad  = 1,  /* Authenticated Key Associated Data */
        nonce = 2,  /* Nonce value */
        meta  = 3   /* Metadata Key Associated data */
      };
    };
  } // namespace encryption

  class logSensePages {
  public:
    enum {
      writeErrors                = 0x02,
      readErrors                 = 0x03,
      nonMediumErrors            = 0x06,
      sequentialAccessDevicePage = 0x0C,
      volumeStatistics           = 0x17, // IBM specific page
      tapeAlert                  = 0x2e,
      dataCompression32h         = 0x32, // for LTO, SDLT. We have Data Compression 1Bh for LTO5,6 and DataComppression 0Fh in SSC-3 
      driveWriteErrors           = 0x32, // IBM specific page
      driveWriteErrorsLTO        = 0x33, // LTO SCSI specific page
      driveReadForwardErrors     = 0x34, // IBM and LTO specific page
      driveReadBackwardErrors    = 0x36, // IBM specific page
      performanceCharacteristics = 0x37, // IBM specific page
      vendorUniqueDriveStatistics= 0x3d, // Oracle T1000D/E specific page
      blockBytesTransferred      = 0x38  // parameters in this page are reset when a cartridge is loaded
    };
  };
  
  /**
   * Constants for used MODE SENSE/SELECT pages
   */
  class modeSensePages {
  public:
    enum {
      controlDataProtection = 0x0A,
      deviceConfiguration   = 0x10
    };
  };
  
  /**
   * The value for subpage code for the Control Data Protection  MODE PAGE
   */
  class modePageControlDataProtection {
  public:
    enum {
      subpageCode = 0xF0
    };
  };
  
  class inquiryVPDPages {
  public:
    enum {
      unitSerialNumber = 0x80 
    };
  };
 
  class writeErrorsDevicePage {
  public:
    enum {
      totalCorrectedErrors        = 0x0003,
      totalProcessed              = 0x0005,
      totalUncorrectedErrors      = 0x0006
    };
  };

  class readErrorsDevicePage {
  public:
    enum {
      totalCorrectedErrors        = 0x0003,
      totalProcessed              = 0x0005,
      totalUncorrectedErrors      = 0x0006
    };
  };

  class nonMediumErrorsDevicePage {
  public:
    enum {
      totalCount                  = 0x0000
    };
  };

  class volumeStatisticsPage { // IBM 3592 specific
  public:
    enum {
      validityFlag                            = 0x0000,
      volumeMounts                            = 0x0001,
      volumeRecoveredWriteDataErrors          = 0x0003,
      volumeUnrecoveredWriteDataErrors        = 0x0004,
      volumeRecoveredReadErrors               = 0x0008,
      volumeUnrecoveredReadErrors             = 0x0009,
      volumeManufacturingDate                 = 0x0046,
      BOTPasses                               = 0x0101,
      MOTPasses                               = 0x0102
    };
  };

  class performanceCharacteristicsQualitySummaryPage { // IBM 3592 specific
  public:
    enum {
      driveEfficiency             = 0x0000,
      mediaEfficiency             = 0x0001,
      primaryInterfaceEfficiency0 = 0x0011,
      primaryInterfaceEfficiency1 = 0x0012,
      libraryInterfaceEfficiency  = 0x001a
    };
  };

  class performanceCharacteristicsHostCommandsPage { // IBM 3592 specific
  public:
    enum {
      readPerformanceEfficiency   = 0x03d0,
      writePerformanceEfficiency  = 0x04d0
    };
  };

  class driveWriteErrorsPage { // IBM 3592 specific
  public:
    enum {
      servoTransients             = 0x0001,
      dataTransients              = 0x0002,
      servoTemps                  = 0x0006,
      dataTemps                   = 0x0007,
      totalRetries                = 0x0008
    };
  };

  class driveReadErrorsPage { // IBM 3592 specific (both FW and BW)
  public:
    enum {
      servoTransients             = 0x0001,
      dataTransients              = 0x0002,
      servoTemps                  = 0x0006,
      dataTemps                   = 0x0007,
      totalRetries                = 0x0015
    };
  };

  class vendorUniqueDriveStatistics { // Oracle T10K specific
  public:
    enum {
      // Drive stats
      readRecoveryRetries         = 0x0104,
      readTransientConditions     = 0x0105,
      writeTransientConditions    = 0x0106,
      servoTepomporaries          = 0x0107,
      servoTransientConditions    = 0x0108,
      writeRecoveryRetries        = 0x0112,
      temporaryDriveErrors        = 0x0203,
      permanentDriveErrors        = 0x0204,

      // Quality stats
      tapeEfficiency              = 0x0400,
      readQualityIndex            = 0x0401,
      readBackCheckQualityIndex   = 0x0402,
      writeEfficiency             = 0x0407
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
  /**
   * IBM System Storage Tape Drive 3592 SCSI Reference
   */
  class blockBytesTransferred {
  public:
    enum {
      hostWriteBlocksProcessed       = 0x0000,
      hostWriteKiBProcessed          = 0x0001,
      hostReadBlocksProcessed        = 0x0002,
      hostReadKiBProcessed           = 0x0003,
      deviceWriteDatasetsProcessed   = 0x0004,
      deviceWriteKiBProcessed        = 0x0005,
      deviceReadDatasetsProcessed    = 0x0006,
      deviceReadKiBProcessed         = 0x0007,
      deviceWriteDatasetsTransferred = 0x0008,
      deviceWriteKiBTransferred      = 0x0009,
      deviceReadDatasetsTransferred  = 0x000A,
      deviceReadKiBTransferred       = 0x000B,
      nominalCapacityOfPartition     = 0x000C,  // in KiB
      fractionOfPartitionTraversed   = 0x000D,
      nominalCapacityOfVolume        = 0x000E,  // in KiB
      fractionOfVolumeTravesed       = 0x000F,
      remainingCapacityOfVolume      = 0x0010,  // in KiB
      remainingCapacityOfPartition   = 0x0011
    };
  };
  /**
   * IBM TotalStorage LTO Ultrium Tape Drive SCSI Reference
   */
  class dataCompression32h {
  public:
    enum {
      readCompressionRatio       = 0x0000, // x100
      writeCompressionRation     = 0x0001, // x100
      mbTransferredToServer      = 0x0002, // rounded to the nearest megabyte 10^6
      bytesTransferredToServer   = 0x0003, // a signed number and may be negative
      mbReadFromTape             = 0x0004, // rounded to the nearest megabyte 10^6
      bytesReadFromTape          = 0x0005, // a signed number and may be negative
      mbTransferredFromServer    = 0x0006, // rounded to the nearest megabyte 10^6
      bytesTransferredFromServer = 0x0007, // a signed number and may be negative
      mbWrittenToTape            = 0x0008, // rounded to the nearest megabyte 10^6
      bytesWrittenToTape         = 0x0009  // a signed number and may be negative
    };
  };
 
  class senseConstants {
  public:
    /* Structures from the Linux Kernel. It holds ASC/ASCQ to string 
     * translations (from http://www.t10.org/lists/asc-num.txt) */
    struct error_info {
      uint16_t code12; /* 0x0302 looks better than 0x03,0x02 */
      const char* text;
    };
    struct error_range_info {
      uint8_t asc;
      uint8_t ascq_min;
      uint8_t ascq_max;
      const char* text;
    };
    static std::string getASCString(uint8_t asc, uint8_t ascq);

  private:
    static const struct error_info ascStrings[];
    static const struct error_range_info ascRangesStrings[];    
  };

  class senseKeys {
  public:
    enum {
      noSense        = 0x0,
      recoveredError = 0x1, 
      notReady       = 0x2,
      mediumError    = 0x3,
      hardwareError  = 0x4,
      illegalRequest = 0x5,
      unitAttention  = 0x6, 
      dataProtect    = 0x7,
      blankCheck     = 0x8,
      vendorSpecific = 0x9,
      copyAborted    = 0xA,
      abortedCommand = 0xB,
      equal          = 0xC,
      volumeOverflow = 0xD,
      miscompare     = 0xE,
      lastWithText   = 0xE
    };
    static const char * const senseKeysText[];      
  }; 
 
  /**
   * Logic block protection as defined in SSC-5 (latest drafts) 
   * 8.4.9 Control Data Protection mode page
   */
  
  class logicBlockProtectionMethod {
  public:
    enum {
      DoNotUse    = 0x00, // do not use logical block protection
      ReedSolomon = 0x01, // Reed-Solomon CRC as specified in ECMA-319
      CRC32C      = 0x02  // CRC32C polynomial as specified for iSCSI 
    };
    enum {
      ReedSolomonLength = 4, // Reed-Solomon CRC length in bytes
      CRC32CLength = 4       // CRC32C length in bytes
    };
    enum {
      ReedSolomonSeed = 0x0,        // The default seed for Read-Solomon CRC
      CRC32CSeed      = 0xFFFFFFFF  // The default seed for CRC32C
    };
  };    
  /**
   * Turn a LBP method code into a string
   * @param LBPmethod  The integer presentation of Logical Block Protection method
   * @return           The string presentation for LBP method or Unknown if method 
   *                   unknown.
   */
  std::string LBPMethodToString(const unsigned char LBPMethod);  
  
  /**
   * Addition for the mode page Length for the Control Data Protection Mode Page
   */
  const unsigned char controlDataProtectionModePageLengthAddition = 4; 
  
} // namespace castor::tape::SCSI
