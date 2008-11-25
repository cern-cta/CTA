#ifndef _CASTOR_TAPE_FORMAT_MARSHALLER_HPP
#define	_CASTOR_TAPE_FORMAT_MARSHALLER_HPP

#include "castor/tape/format/Constants.hpp"

#include <stdint.h>
#include <sstream>

namespace castor {
namespace tape   {
namespace format {

//==============================================================================
//                        Marshaller
//==============================================================================
class Marshaller{

public:

  /**
   * Standard fields used by all versions of the ALB (Ansi Lable with
   * Block formated migration) tape formats.
   */
  struct Header {
    char        version_number[VERSION_NUMBER_LEN+1];   
    uint16_t    header_size; 

    virtual ~Header() {}
  };


  virtual~Marshaller();


protected:
  /* Those (3) methodes are on porpuse declared "virtual",
   * to force the code structure of the futures IL tape format implementation.
   */
  virtual void startMigration(Header& data) = 0;

  virtual void startFile(Header& data) = 0;

  virtual void marshall(char* block) = 0;

  /** 
   *  Internal method to format the values and store them at the correct location of the header.
   *  @param buff   pointer to the memory block containing the header and the dada;
   *  @param value  pointer to the array of char containing the value to be Marshall;
   *  @param length length of the char array;
   *  @param offset offset where Marshall the value in buff;
   *  @param pad    character used to pad (specify also the padding system to use) 
   */
  void parseCharData(char *const buff, const char *value, const size_t length, const size_t offset, const char pad); 
 
}; 

} // namespace format
} // namespace tape
} // namespace castor

#endif //_CASTOR_TAPE_FORMAT_MARSHALLER_HPP
