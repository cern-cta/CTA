// ----------------------------------------------------------------------
// File: Drive/Drive.hh
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

#include "../SCSI/Device.hh"
#include "../SCSI/Structures.hh"
#include "../System/Wrapper.hh"
#include "../Exception/Exception.hh"

/**
 * Class wrapping the tape server. Has to be templated (and hence fully in .hh)
 * to allow unit testing against system wrapper.
 */
namespace Tape {
  
  /**
   * Compressions statistics container, returned by Drive::getCompression()
   */
  class compressionStats{
  public:
    uint64_t fromHost;
    uint64_t fromTape;
    uint64_t toHost;
    uint64_t toTape;
  };
  
  /**
   * Device information, returned by getDeviceInfo()
   */
  class deviceInfo{
  public:
    std::string vendor;
    std::string product;
    std::string productRevisionLevel;
  };
  
  /**
   * Position info, returning both the logical position and the 
   * buffer writing status.
   * Returned by getPositionInfo()
   */
  class positionInfo {
    uint32_t currentPosition;
    uint32_t oldestDirtyObject;
    uint32_t dirtyObjectsCount;
    uint32_t dirtyBytesCount;
  };
  
  class tapeAlerts {
    bool hardError;
    bool mediaError;
    bool readFailure;
    bool writeFailure;
  };

  /**
   * Class abstracting the tape drives. Gets initialized from 
   */
  template <class sysWrapperClass>
  class Drive {
  public:
    Drive(SCSI::DeviceInfo di, sysWrapperClass & sw): m_sysWrapper(sw),
            m_SCSIInfo(di), m_tapeFD(-1), m_genericFD(-1)
    {
      /* Open the device files */
      /* We open the tape device file non-blocking as blocking open on rewind tapes (at least)
       * will fail after a long timeout when no tape is present (at least with mhvtl) 
       */
      m_tapeFD = m_sysWrapper.open(m_SCSIInfo.nst_dev.c_str(), O_RDWR | O_NONBLOCK);
      if (-1 == m_tapeFD)
        throw Tape::Exceptions::Errnum(std::string("Could not open device file: "+ m_SCSIInfo.nst_dev));
      m_genericFD = m_sysWrapper.open(m_SCSIInfo.sg_dev.c_str(), O_RDWR);
      if (-1 == m_genericFD)
        throw Tape::Exceptions::Errnum(std::string("Could not open device file: "+ m_SCSIInfo.sg_dev));
      /* Read drive status */
      if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &m_mtInfo))
        throw Tape::Exceptions::Errnum(std::string("Could not read drive status: "+ m_SCSIInfo.nst_dev));
      /* Read Generic SCSI information (INQUIRY) */
    }
    
    /* Operations to be used by the higher levels */
    virtual compressionStats getCompression()  throw (Exception) { throw Exception("Not implemented"); }
    virtual void clearCompressionStats() throw (Exception) { throw Exception("Not implemented"); }
    /**
     * Information about the drive. The vendor id is used in the user labels of the files.
     * @return 
     */
    virtual deviceInfo getDeviceInfo() throw (Exception) { throw Exception("Not implemented"); }
    /**
     * Position to logical object address (i.e. block). This function is non-blocking:
     * the immediate bit is not set.
     * @param blockId The blockId, represented in local endianness.
     */
    virtual void positionToLogicalObject (uint32_t blockId) { throw Exception("Not implemented"); }
    /**
     * Return logical position of the drive. This is the address of the next object
     * to read or write.
     * @return positionInfo class. This contains the logical position, plus information
     * on the dirty data still in the write buffer.
     */
    virtual positionInfo getPositionInfo () { throw Exception("Not implemented"); }
    virtual ~Drive() {
      if(-1 != m_tapeFD)
        m_sysWrapper.close(m_tapeFD);
      if (-1 != m_genericFD)
        m_sysWrapper.close(m_genericFD);
    }
    void SCSI_inquiry() { 
      std::cout << "Doing a SCSI inquiry via generic device:" << std::endl;
      SCSI_inquiry(m_genericFD);
      std::cout << "Re-doing a SCSI inquiry via st device:" << std::endl;
      SCSI_inquiry(m_tapeFD);
    }
  private:
    SCSI::DeviceInfo m_SCSIInfo;
    int m_tapeFD;
    int m_genericFD;
#undef ConvenientCoding
#ifdef ConvenientCoding
    syntax error here!!!; /* So we don't compile in this configuration */
    /* This makes code completion more efficient in editors */
    Tape::System::fakeWrapper & m_sysWrapper;
#else
    sysWrapperClass & m_sysWrapper;
#endif
    struct mtget m_mtInfo;
    private:
    void SCSI_inquiry(int fd) {
      unsigned char dataBuff[130];
      unsigned char senseBuff[256];
      SCSI::Structures::inquiryCDB_t cdb; 
      memset(&dataBuff, 0, sizeof (dataBuff));
      /* Build command: nothing to do. We go with defaults. */

      sg_io_hdr_t sgh;
      memset(&sgh, 0, sizeof (sgh));
      sgh.interface_id = 'S';
      sgh.cmdp = (unsigned char *)&cdb;
      sgh.cmd_len = sizeof (cdb);
      sgh.sbp = senseBuff;
      sgh.mx_sb_len = 255;
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      sgh.dxferp = dataBuff;
      sgh.dxfer_len = sizeof(dataBuff);
      sgh.timeout = 30000;
      if (-1 == m_sysWrapper.ioctl(fd, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      std::cout << "INQUIRY result: " << std::endl
              << "sgh.dxfer_len=" << sgh.dxfer_len
              << " sgh.sb_len_wr=" << ((int) sgh.sb_len_wr)
              << " sgh.status=" << ((int) sgh.status)
              << " sgh.info=" << ((int) sgh.info)
              << std::endl;
      std::cout << SCSI::Structures::hexDump(dataBuff)
              << SCSI::Structures::toString(*((SCSI::Structures::inquiryData_t *) dataBuff));
    }
  };
}
