#ifndef _ALB0100UNMARSHALL_HPP
#define	_ALB0100UNMARSHALL_HPP

#include "castor/tape/format/Unmarshaller.hpp"

#include <sstream>

namespace castor {
namespace tape   {
namespace format {

/**
 * Responsible for unmarshalling tape data blocks for version 01.00 of the ALB
 * (Ansi Label with Block formatted migrations) tape format.
 */
class ALB0100Unmarshaller : public Unmarshaller{

public:

  /**
   * Structure containing the header's data specific for Version 01.00
   */
  struct ALB0100Header: public  Header{

    char        checksum_algorithm[alb0100::CHECKSUM_ALGORITHM_LEN+1];// 10 bytes original value 32 bit binary
    uint32_t    header_checksum;                                      //  4 bytes original value 23 bit binary
    uint64_t    tape_mark_count;                                      //  8 bytes original value 32/64 bit binary
    uint32_t    block_size;                                           //  4 bytes original value 23 bit binary
    uint64_t    block_count;                                          //  8 bytes original value 32/64 bit binary
    uint32_t    timestamp;                                            //  4 bytes original value 23 bit binary
    char        stager_version[alb0100::STAGER_VERSION_LEN+1];        // 15 bytes
    char        stager_host[alb0100::STAGER_HOST_LEN+1];              // 30 char
    char        drive_name[alb0100::DRIVE_NAME_LEN+1];                // 10 bytes
    char        drive_serial[alb0100::DRIVE_SERIAL_LEN+1];            // 20 chars original value "x" digits
    char        drive_firmware[alb0100::DRIVE_FIRMWARE_LEN+1];        // 10 bytes
    char        drive_host[alb0100::DRIVE_HOST_LEN+1];                // 30 char
    char        vol_density[alb0100::VOL_DENSITY_LEN+1];              // 10 bytes
    char        vol_id[alb0100::VOL_ID_LEN+1];                        // 20 bytes
    char        vol_serial[alb0100::VOL_SERIAL_LEN+1];                // 20 bytes
    char        device_group_name[alb0100::DEVICE_GROUP_NAME_LEN+1];  // 10 bytes
    uint64_t    file_size;                                            //  8 bytes original value 32/64 bit binary
    uint32_t    file_checksum;                                        //  4 bytes original value 23 bit binary
    char        file_ns_host[alb0100::FILE_NS_HOST_LEN+1];            // 30 char
    uint64_t    file_ns_id;                                           //  8 bytes 64 bit binary
    uint32_t    file_progressive_checksum;                            //  4 bytes original value 32 bit binary 
    uint64_t    file_block_count;                                     //  4 bytes original value 32 bit binary. Data file offset of the data in this block
    char        file_name[alb0100::FILE_NAME_LEN+1];                  //649 char
  };

  /**
   * Char array containing the header elements for version 01.00.
   */
  static const char *headerName_V0[];

  /**
   * Bool array containing the status of the header elements (version 01.00) unmurshalled.
   */
  bool               m_errorList[25];

private: 

  ALB0100Header      m_header;
  uint32_t           m_payload_size;

public:
  /**
   * Unmarshall the header of the block
   * checking if there are errors.
   *
   * @param block    pointer to the memory block to unmarshall;
   * @param newFile  flag to specify "first block of a new file".
   */
  const Header *unmarshallHeader(char* block, bool newFile);

protected:
  /**
   * Print on the standard output a readable version of the header's values
   * that has been unmarshalled with the relatibe error flag. 
   *
   * @param header  reference to the header data structure
   */
  void printHeader(ALB0100Header &header);


};

} //namespace format 
} //namespace tape 
} //namespace castor 

#endif // _ALB0100UNMARSHALL_HPP
