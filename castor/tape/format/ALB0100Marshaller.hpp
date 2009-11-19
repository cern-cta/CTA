
#ifndef _CASTOR_TAPE_FORMAT_ALB0100MARSCHALLER_HPP
#define	_CASTOR_TAPE_FORMAT_ALB0100MARSCHALLER_HPP

#include "castor/exception/Exception.hpp"
#include "castor/tape/format/Marshaller.hpp"

#include <stdint.h>

namespace castor {
namespace tape {
namespace format {

/**
 * Responsible for marshalling tape data blocks for version 01.00 of the ALB
 * (Ansi Label with Block formatted migrations) tape format.
 */
class ALB0100Marshaller: public Marshaller {

public:

  /**
   * Structure used to iniatialize the header with all migration specific data
   */
  struct MigHeader: public Header {

    char        checksum_algorithm[alb0100::CHECKSUM_ALGORITHM_LEN+1];// 10 bytes original value 32 bit binary

    u_signed64  tape_mark_count;                                      //  8 bytes original value 32/64 bit binary
    uint32_t    block_size;                                           //  4 bytes original value 23 bit binary
    u_signed64  block_count;                                          //  8 bytes original value 32/64 bit binary      // NEED THE FIRST ONE!!!!

    char        stager_version[alb0100::STAGER_VERSION_LEN+1];        // 15 bytes
    char        stager_host[alb0100::STAGER_HOST_LEN+1];              //    TO BE TRUNCATE
    char        drive_name[alb0100::DRIVE_NAME_LEN+1];                // 10 bytes
    char        drive_serial[alb0100::DRIVE_SERIAL_LEN+1];            // 20 bytes 
    char        drive_firmware[alb0100::DRIVE_FIRMWARE_LEN+1];        // 10 bytes
    char        drive_host[alb0100::DRIVE_HOST_LEN+1];                //    TO BE TRUNCATE   
    char        vol_density[alb0100::VOL_DENSITY_LEN+1];              // 10 bytes
    char        vol_id[alb0100::VOL_ID_LEN+1];                        // 20 bytes
    char        vol_serial[alb0100::VOL_SERIAL_LEN+1];                // 20 bytes
    char        device_group_name[alb0100::DEVICE_GROUP_NAME_LEN+1];  // 10 bytes      
  };


  /**
   * Structure used to iniatialize the header with all file specific data
   */
  struct FileHeader: public  Header {
    u_signed64 file_size;
    uint32_t   file_checksum;
    char       file_ns_host[alb0100::FILE_NS_HOST_LEN+1];
    u_signed64 file_ns_id;
    char       file_name[alb0100::FILE_NAME_LEN+1];
  };


private:

  char          m_headerStamp[HEADER_SIZE+1];  
  u_signed64    m_block_count;                // 8 bytes 
  u_signed64    m_file_block_count;           // 4 bytes original value 32 bit binary. Data file offset of the data in this block (0, 255, 510, ...)
  uint32_t      m_block_size;
  uint32_t      m_file_progressive_checksum;  // 4 bytes original value 32 bit binary
  uint32_t      m_header_checksum;		  
  uint32_t      m_payload_size;
  u_signed64    m_file_size;
  uint32_t      m_file_checksum;
    
public:

  /**
   * Destructor.
   */
  ~ALB0100Marshaller();

  /**
   * Method that specify the begin of a new migration.
   * Parse the migration specifics values and write them ,in the right format, into the header structure
   *
   * @param data reference to a structure containind the migration specific data.
   */
  void startMigration(Header &data) throw (castor::exception::Exception);

  /**
   * Method that specify the begin of a new file.
   * Parse the file specifics values and write them ,in the right format, into the header structure
   *
   * @param data reference to a structure containind the file specific data.
   */
  void startFile(Header &data) throw (castor::exception::Exception);

  /**
   * Method that compute the required values block specific.
   * Complete the header and copy it on top of the block containing the payload.
   * Receive the block memory pointer and fill up with the header the first 1KiB
   *
   * @param data reference to the memory struct cotaining the data to marshall.
   */
  void marshall(char* block);
      
private:

  /** Internal method that visualize on the terminal the header content 
   *
   *  @param block pointer to the memory area containing the data to be marshall.
   */
  void printHeader(char *buff);
}; 

} // namespace format
} // namespace castor
} // namespace tape

#endif	// _CASTOR_TAPE_FORMAT_ALB0100MARSCHALLER_HPP
