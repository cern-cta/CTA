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
    void SCSI_inquiry() { 
      std::cout << "Doing a SCSI inquiry via generic device:" << std::endl;
      SCSI_inquiry(m_genericFD);
      std::cout << "Re-doing a SCSI inquiry via st device:" << std::endl;
      SCSI_inquiry(m_tapeFD); }
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

      SCSI::Structures::inquiryData_t & inq = *((SCSI::Structures::inquiryData_t *) dataBuff);
      std::stringstream inqDump;
      inqDump << std::hex << std::showbase << std::nouppercase
              << "inq.perifDevType=" << (int) inq.perifDevType << std::endl
              << "inq.perifQualifyer=" << (int) inq.perifQualifyer << std::endl
              << "inq.RMB="            << (int) inq.RMB << std::endl
              << "inq.version="        << (int) inq.version << std::endl
              << "inq.respDataFmt="    << (int) inq.respDataFmt << std::endl
              << "inq.HiSup="          << (int) inq.HiSup << std::endl
              << "inq.normACA="        << (int) inq.normACA << std::endl
              << "inq.addLength="      << (int) inq.addLength << std::endl
              << "inq.protect="        << (int) inq.protect << std::endl
              << "inq.threePC="        << (int) inq.threePC << std::endl
              << "inq.TPGS="           << (int) inq.TPGS << std::endl
              << "inq.ACC="            << (int) inq.ACC << std::endl
              << "inq.SCCS="           << (int) inq.SCCS << std::endl         
              << "inq.addr16="         << (int) inq.addr16 << std::endl       
              << "inq.multiP="         << (int) inq.multiP << std::endl
              << "inq.VS1="            << (int) inq.VS1 << std::endl
              << "inq.encServ="        << (int) inq.encServ << std::endl
              << "inq.VS2="            << (int) inq.VS2 << std::endl
              << "inq.cmdQue="         << (int) inq.cmdQue << std::endl
              << "inq.sync="           << (int) inq.sync << std::endl
              << "inq.wbus16="         << (int) inq.wbus16  << std::endl 
              << "inq.T10Vendor="      << SCSI::Structures::toString(inq.T10Vendor) << std::endl
              << "inq.prodId="         << SCSI::Structures::toString(inq.prodId) << std::endl
              << "inq.prodRevLv="      << SCSI::Structures::toString(inq.prodRevLvl) << std::endl
              << "inq.vendorSpecific1="<< SCSI::Structures::toString(inq.vendorSpecific1)<< std::endl
              << "inq.IUS="            << (int) inq.IUS << std::endl
              << "inq.QAS="            << (int) inq.QAS << std::endl
              << "inq.clocking="       << (int) inq.clocking << std::endl;
      for (int i=0; i < 8; i++)
        inqDump << "inq.versionDescriptor[" << i << "]=" << SCSI::Structures::toU16(inq.versionDescriptor[i]) << std::endl;
      std::cout << inqDump.str();
    }
  };
}
