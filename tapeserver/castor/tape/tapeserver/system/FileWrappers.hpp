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
#include <sys/types.h>
#include <sys/mtio.h>
#include <scsi/sg.h>
#include <stdint.h>

namespace castor::tape::System {

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
  protected:
    struct mtget m_mtStat;
    struct mtop m_mtCmd;
    uint32_t blockID;  
    bool clearCompressionStats;
    unsigned char m_LBPInfoMethod;
    unsigned char m_LBPInfoLength;
    unsigned char m_LBPInfo_R;
    unsigned char m_LBPInfo_W;
    /**
     * This function handles READ_POSITION CDB and prepares the reply.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlReadPosition(sg_io_hdr_t * sgio_h);

    /**
     * This function handles REQUEST_SENSE CDB and prepares the reply.
     *
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with
     *                ioctl call data
     * @retval         0 success
     * @retval        -1 if an error occurred (errno is set)
     */
    virtual int ioctlRequestSense(sg_io_hdr_t * sgio_h);

    /**
     * This function handles LOG_SELECT CDB and only checks the CDB for the 
     * correct values and sets internal trigger for 0 compression as true.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlLogSelect(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles LOCATE_10 CDB and only checks the CDB for the 
     * correct values and sets internal blockID variable (logical seek).
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlLocate10(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles LOG_SENSE CDB and prepares the replay with
     * compression data.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlLogSense(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles MODE_SENSE_6 CDB and prepares the replay with
     * random data.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlModSense6(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles MODE_SELECT_6 CDB and only checks the CDB for the
     * correct values.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlModSelect6(sg_io_hdr_t * sgio_h);
    
    /**
     * This function handles INQUIRY CDB and prepares the standard inquiry
     * replay or the unit serial number vital product data replay.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlInquiry(sg_io_hdr_t * sgio_h) = 0;

    /**
     * Simulates the response of a LogSense ioctl from the drive to ReadErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseReadErrorsPage(sg_io_hdr_t * sgio_h) = 0;

    /**
     * Simulates the response of a LogSense ioctl from the drive to WriteErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseWriteErrorsPage(sg_io_hdr_t * sgio_h) = 0;

    /**
     * Simulates the response of a LogSense ioctl from the drive to NonMediumErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseNonMediumErrorsPage(sg_io_hdr_t * sgio_h) = 0;

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
    virtual int logSenseSequentialAccessDevicePage(sg_io_hdr_t * sgio_h);
    
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
    virtual int logSenseDataCompression32h(sg_io_hdr_t * sgio_h);
    
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
    virtual int logSenseBlockBytesTransferred(sg_io_hdr_t * sgio_h);

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
    
    /**
     * This function only checks the corectness of the parameters in sg_io_hdr_t
     * sturcture and returns random data.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int modeSenseDeviceConfiguration(sg_io_hdr_t * sgio_h);
    
    /**
     * This function checks the corectness of the parameters in sg_io_hdr_t and
     * returns filled filds:
     *   controlDataProtection.modePage.LBPMethod
     *   controlDataProtection.modePage.LBPInformationLength
     *   controlDataProtection.modePage.LBP_R
     *   controlDataProtection.modePage.LBP_W
     * All other filds in SCSI replay are random.
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int modeSenseControlDataProtection(sg_io_hdr_t * sgio_h);
     /**
     * This function only checks the corectness of the parameters in sg_io_hdr_t
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int modeSelectDeviceConfiguration(sg_io_hdr_t * sgio_h);
    /**
     * This function only checks the corectness of the parameters in sg_io_hdr_t
     * 
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with 
     *                ioctl call data
     * @return        Returns 0 in success and 
     *                -1 with appropriate  errno if an error occurred.
     */
    int modeSelectControlDataProtection(sg_io_hdr_t * sgio_h);
  };

  /**
   * Specific st device class for IBM 3592.
   * Used to test vendor specific features.
   */
  class stIBM3592DeviceFile : public stDeviceFile {
  public:
    /**
     * This function handles LOG_SENSE CDB and prepares the replay with
     * compression data.
     *
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with
     *                ioctl call data
     * @return        Returns 0 in success and
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlLogSense(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to ReadErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseReadErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to WriteErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseWriteErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to NonMediumErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseNonMediumErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to VolumeStatisticsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseVolumeStatisticsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to driveWriteErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseDriveWriteErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to ReadForwardErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseDriveReadForwardErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to ReadBackwardErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseDriveReadBackwardErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to PerformanceCharacteristicsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSensePerformanceCharacteristicsPage(sg_io_hdr_t * sgio_h);

    virtual int ioctlInquiry(sg_io_hdr_t * sgio_h);
  };

  /**
   * Specific st device class for Oracle T10000D.
   * Used to test vendor specific features.
   */
  class stOracleT10000Device : public stDeviceFile {
  public:
    /**
     * This function handles LOG_SENSE CDB and prepares the replay with
     * compression data.
     *
     * @param sgio_h  The pointer to the sg_io_hdr_t structure with
     *                ioctl call data
     * @return        Returns 0 in success and
     *                -1 with appropriate  errno if an error occurred.
     */
    virtual int ioctlLogSense(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to ReadErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseReadErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to WriteErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseWriteErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to NonMediumErrorsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseNonMediumErrorsPage(sg_io_hdr_t * sgio_h);

    /**
     * Simulates the response of a LogSense ioctl from the drive to VendorUniqueDriveStatisticsPage.
     * @param sgio_h The pointer to the sg_io_hdr_t structure with
                     ioctl call data.
     * @return       Returns 0 in success and -1 with appropriate errno if an error has occurred.
     */
    virtual int logSenseVendorUniqueDriveStatisticsPage(sg_io_hdr_t * sgio_h);

    virtual int ioctlInquiry(sg_io_hdr_t * sgio_h);
  };
} // namespace castor::tape::System
