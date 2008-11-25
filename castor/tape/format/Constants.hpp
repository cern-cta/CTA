


#ifndef _CONSTANTS_HPP

#define	_CONSTANTS_HPP

#include <cstring>

namespace castor {
namespace tape   {
namespace format {

  // Header data length
  //=================================================
  const size_t VERSION_NUMBER_LEN =                5;
  const size_t HEADER_SIZE_LEN =                   5;

  // Header data offset
  //=================================================
  const size_t VERSION_NUMBER_OFFSET =             0;
  const size_t HEADER_SIZE_OFFSET =                5;

  // Usefull constants
  //=================================================
  const size_t HEADER_SIZE =                    1024;
  const size_t HEADER_ELEMENTS =                  25;
  const size_t BLOCK_SIZE =    sizeof(char)*1024*256;

  /**
   * ALB (Ansi Label with Block formatted migrations) format version 01.00
   */
  namespace alb0100 {

    //const char *VERSION_NUMBER = "0";  //  <-- is it usefull????

    // Header data length
    //===============================================
    const size_t CHECKSUM_ALGORITHM_LEN =         10;
    const size_t HEADER_CHECKSUM_LEN =            10;
    const size_t TAPE_MARK_COUNT_LEN =            20;
    const size_t BLOCK_SIZE_LEN =                 10;
    const size_t BLOCK_COUNT_LEN =                20;
    const size_t BLOCK_TIME_STAMP_LEN =           10;
    const size_t STAGER_VERSION_LEN =             15;
    const size_t STAGER_HOST_LEN =                30;
    const size_t DRIVE_NAME_LEN =                 10;
    const size_t DRIVE_SERIAL_LEN =               20;
    const size_t DRIVE_FIRMWARE_LEN =             10;
    const size_t DRIVE_HOST_LEN =                 30;
    const size_t VOL_DENSITY_LEN =                10;
    const size_t VOL_ID_LEN =                     20;
    const size_t VOL_SERIAL_LEN =                 20;
    const size_t DEVICE_GROUP_NAME_LEN =          10;
    const size_t FILE_SIZE_LEN =                  20;
    const size_t FILE_CHECKSUM_LEN =              10;
    const size_t FILE_NS_HOST_LEN =               30;
    const size_t FILE_NS_ID_LEN =                 20;
    const size_t FILE_PROGRESSIVE_CHECKSUM_LEN=   10;
    const size_t FILE_BLOCK_COUNT_LEN =           20;
    const size_t FILE_NAME_LEN =                 649;


    // Header data offset
    //=================================================
    const size_t CHECKSUM_ALGORITHM_OFFSET =        10;
    const size_t HEADER_CHECKSUM_OFFSET =           20;
    const size_t TAPE_MARK_COUNT_OFFSET =           30;
    const size_t BLOCK_SIZE_OFFSET =                50;
    const size_t BLOCK_COUNT_OFFSET =               60;
    const size_t BLOCK_TIME_STAMP_OFFSET =          80;
    const size_t STAGER_VERSION_OFFSET =            90;
    const size_t STAGER_HOST_OFFSET =              105;
    const size_t DRIVE_NAME_OFFSET =               135;
    const size_t DRIVE_SERIAL_OFFSET =             145;
    const size_t DRIVE_FIRMWARE_OFFSET =           165;
    const size_t DRIVE_HOST_OFFSET =               175;
    const size_t VOL_DENSITY_OFFSET =              205;
    const size_t VOL_ID_OFFSET =                   215;
    const size_t VOL_SERIAL_OFFSET =               235;
    const size_t DEVICE_GROUP_NAME_OFFSET =        255;
    const size_t FILE_SIZE_OFFSET =                265;
    const size_t FILE_CHECKSUM_OFFSET =            285;
    const size_t FILE_NS_HOST_OFFSET =             295;
    const size_t FILE_NS_ID_OFFSET =               325;
    const size_t FILE_PROGRESSIVE_CHECKSUM_OFFSET= 345;
    const size_t FILE_BLOCK_COUNT_OFFSET =         355;
    const size_t FILE_NAME_OFFSET =                375;
 
    } // namespace alb0100
	

      //=============================================================
      //                    IL format version 2
      //=============================================================
      //
      //  namespace alb0200 {
      //  .....
      //  }

} // namespace format
} // namespace tape
} // namespace castor


#endif	/* _CONSTANTS_HPP */

