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
#include "../System/Wrapper.hh"
#include "../Exception/Exception.hh"

/**
 * Class wrapping the tape server. Has to be templated (and hence fully in .hh)
 * to allow unit testing against system wrapper.
 */
namespace Tape {

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
      m_tapeFD = m_sysWrapper.open(m_SCSIInfo.st_dev.c_str(), O_RDWR | O_NONBLOCK);
      if (-1 == m_tapeFD)
        throw Tape::Exceptions::Errnum(std::string("Could not open device file: "+ m_SCSIInfo.st_dev));
      m_genericFD = m_sysWrapper.open(m_SCSIInfo.sg_dev.c_str(), O_RDWR);
      if (-1 == m_genericFD)
        throw Tape::Exceptions::Errnum(std::string("Could not open device file: "+ m_SCSIInfo.sg_dev));
      /* Read drive status */
      if (-1 == m_sysWrapper.ioctl(m_tapeFD, MTIOCGET, &m_mtInfo))
        throw Tape::Exceptions::Errnum(std::string("Could not read drive status: "+ m_SCSIInfo.st_dev));
      /* Read Generic SCSI information (INQUIRY) */
    }
    virtual ~Drive() {
      if(-1 != m_tapeFD)
        m_sysWrapper.close(m_tapeFD);
      if (-1 != m_genericFD)
        m_sysWrapper.close(m_genericFD);
    }
    void SCSI_inquiry() { SCSI_inquiry(m_genericFD); SCSI_inquiry(m_tapeFD); }
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
      unsigned char dataBuff[512];
      unsigned char senseBuff[256];
      unsigned char cdb[6];
      memset(&cdb, 0, sizeof (cdb));
      /* Build command */
      cdb[0] = SCSI::Commands::INQUIRY;

      sg_io_hdr_t sgh;
      memset(&sgh, 0, sizeof (sgh));
      sgh.interface_id = 'S';
      sgh.cmdp = cdb;
      sgh.cmd_len = sizeof (cdb);
      sgh.sbp = senseBuff;
      sgh.mx_sb_len = 255;
      sgh.dxfer_direction = SG_DXFER_FROM_DEV;
      sgh.dxferp = dataBuff;
      sgh.dxfer_len = 512;
      sgh.timeout = 30000;
      if (-1 == m_sysWrapper.ioctl(fd, SG_IO, &sgh))
        throw Tape::Exceptions::Errnum("Failed SG_IO ioctl");
      std::cout << "INQUIRY result: " << std::endl
              << "sgh.dxfer_len=" << sgh.dxfer_len
              << " sgh.sb_len_wr=" << ((int) sgh.sb_len_wr)
              << " sgh.status=" << ((int) sgh.status)
              << " sgh.info=" << ((int) sgh.info)
              << std::endl;
      std::stringstream hex;
      hex << std::hex << std::setfill('0');
      int pos = 0;
      while (pos < 128) {
        hex << std::setw(4) << pos << " | ";
        for (int i=0; i<8; i++)
          hex << std::setw(2) << ((int) dataBuff[pos + i]) << " ";
        hex << "| ";
        for (int i=0; i<8; i++)
          hex << std::setw(0) << dataBuff[pos + i];
        hex << std::endl;
        pos += 8;
      }
      std::cout << hex.str();
      
      /*
       * Inquiry data as described in SPC-4.
       */
      typedef struct  {
        unsigned char perifDevType  : 5;
        unsigned char perifQualifyer: 3;
        
        unsigned char               : 7;
        unsigned char RMB           : 1;
        
        unsigned char version       : 8;
        
        unsigned char respDataFmt   : 4;
        unsigned char HiSup         : 1;
        unsigned char normACA       : 1;
        unsigned char               : 2;
        
        unsigned char addLength     : 8;
        
        unsigned char protect       : 1;
        unsigned char               : 2;
        unsigned char threePC       : 1;
        unsigned char TPGS          : 2;
        unsigned char ACC           : 1;
        unsigned char SCCS          : 1;

        unsigned char addr16        : 1;
        unsigned char               : 3;
        unsigned char multiP        : 1;
        unsigned char VS1            : 1;
        unsigned char encServ       : 1;
        unsigned char               : 1;
        
        unsigned char VS2            : 1;
        unsigned char cmdQue        : 1;
        unsigned char               : 2;
        unsigned char sync          : 1;
        unsigned char wbus16        : 1;
        unsigned char               : 2;
        
        char T10Vendor[8];
        char prodId[16];
        char prodRevLvl[4];
        char vendorSpecific1[20];
        
        unsigned char IUS           : 1;
        unsigned char QAS           : 1;
        unsigned char clocking      : 2;
        unsigned char               : 4;
        
        unsigned char reserved1;
        
        unsigned char vendorDescriptior[8][2];
        
        unsigned char reserved2[22];
        unsigned char vendorSpecific2[1];
      } inquiryData_t;
      inquiryData_t & inq = *((inquiryData_t *) dataBuff);
      std::stringstream inqDump;
      inqDump << std::hex << "inq.perifDevType=" << (int) inq.perifDevType << std::endl;
      inqDump << "inq.perifQualifyer=" << (int) inq.perifQualifyer << std::endl;
      
      inqDump << "inq.T10Vendor=";
      inqDump.write(inq.T10Vendor, std::find(inq.T10Vendor, inq.T10Vendor + 8, '\0') - inq.T10Vendor);
      inqDump << std::endl;
      std::cout << inqDump.str();
    }
  };
}
