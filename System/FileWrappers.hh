// ----------------------------------------------------------------------
// File: System/FileWrapper.hh
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
#include <sys/types.h>
#include <sys/mtio.h>
#include <scsi/sg.h>
#include <stdint.h>

namespace Tape {
namespace System {
/**
 * A dummy class allowing simple open/read/close/ioctl interface simulating
 * different types of files (regular files, device files (like tape devices)
 */
  class vfsFile {
  public:
    virtual ~vfsFile() {};
    virtual ssize_t read(void* buf, size_t nbytes);
    virtual ssize_t write(const void *buf, size_t nbytes);
    virtual int ioctl(unsigned long int request, struct mtop * mt_cmd);
    virtual int ioctl(unsigned long int request, struct mtget * mt_status);
    virtual int ioctl(unsigned long int request, sg_io_hdr_t * sgio_h);
    /** Reset the read/write pointers at open. This ensures coherent behavior on multiple access */
    virtual void reset() = 0;
  };
  
  /**
   * Class representing real files
   */
  class regularFile: public vfsFile {
  public:
    regularFile(): m_read_pointer(0) {};
    regularFile(const std::string & c): m_content(c), m_read_pointer(0) {};
    virtual void reset() { m_read_pointer = 0; };
    void operator = (const std::string & s) { m_content = s; m_read_pointer = 0; }
    virtual ssize_t read(void* buf, size_t nbytes);
    virtual ssize_t write(const void *buf, size_t nbytes);
  private:
    std::string m_content;
    int m_read_pointer;
  };
  /**
    * Class representing a tape device
    */
  class stDeviceFile : public vfsFile {
  public:
    stDeviceFile();
    virtual void reset() {clearCompressionStats = false; blockID=0xFFFFFFFF;};
    virtual int ioctl(unsigned long int request, struct mtop * mt_cmd);
    virtual int ioctl(unsigned long int request, struct mtget * mt_status);
    virtual int ioctl(unsigned long int request, sg_io_hdr_t * sgio_h);
  private:
    struct mtget m_mtStat;
    struct mtop m_mtCmd;
    uint32_t blockID;  
    bool clearCompressionStats;
    /**
     * This function handles READ_POSITION CDB and prepares the replay.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int ioctlReadPosition(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles LOG_SELECT CDB and only checks the CDB for the 
     * correct values and sets internal trigger for 0 compression as true.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int ioctlLogSelect(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles LOCATE_10 CDB and only checks the CDB for the 
     * correct values and sets internal blockID variable (logical seek).
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int ioctlLocate10(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles LOG_SENSE CDB and prepares the replay with
     * compression data.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int ioctlLogSense(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles MODE_SENSE_6 CDB and prepares the replay with
     * random data.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int ioctlModSense6(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles MODE_SELECT_6 CDB and only checks the CDB for the
     * correct values.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int ioctlModSelect6(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles INQUIRY CDB and prepares the standart inquiry
     * replay or the unit serial number vital product data replay.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int ioctlInquiry(sg_io_hdr_t * sgio_h);
    
    /**
     * This function prepares the replay with compression statistics for 
     * LOG SENSE CDB with log page Sequential Access Device Page. We use this 
     * log page for T10000 drives.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int logSenseSequentialAccessDevicePage(sg_io_hdr_t * sgio_h);
    
    /**
     * This function prepares the replay with compression statistics for 
     * LOG SENSE CDB with log page Data Compression (32h). We use this 
     * log page for LTO drives.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int logSenseDataCompression32h(sg_io_hdr_t * sgio_h);
    
    /**
     * This function prepares the replay with compression statistics for 
     * LOG SENSE CDB with log page Block Bytes Transferred. We use this 
     * log page for IBM drives.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int logSenseBlockBytesTransferred(sg_io_hdr_t * sgio_h);
    /**
     * This function replies with a pre-cooked error record. As with the real devices,
     * many parameter codes get reported with a flag set to 0, and a few will
     * show up with the flag set.
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int logSenseTapeAlerts(sg_io_hdr_t * sgio_h);
  };
  
  class tapeGenericDeviceFile: public vfsFile {
  public:
    tapeGenericDeviceFile() {};
    virtual void reset() {};
    virtual int ioctl(unsigned long int request, sg_io_hdr_t * sgio_h);
  };
} // namespace System
} // namespace Tape
