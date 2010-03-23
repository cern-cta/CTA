#ifndef _CASTOR_TAPE_FORMAT__UNMARSHALL_HPP
#define	_CASTOR_TAPE_FORMAT__UNMARSHALL_HPP

#include <stdint.h>
#include <sstream>

#include "castor/tape/format/Constants.hpp"

namespace castor {
namespace tape {
namespace format {

//==============================================================================
//                        Unmarshaller
//==============================================================================
class Unmarshaller{

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


  /** 
   * Return the version number and store all the values of the header.
   */
  char *readVersion(char *block);

  /**
   * Function that verify that all parameter in the header of a block
   * mach with its paylod.
   *
   * @param block pointer the the memory containing the block readed.
   * @param variable set to TRUE if the block belong to a new file. 
   */
  virtual const Header *unmarshallHeader(char* block, bool newFile) = 0;

  /** 
   * Destructor
   */
  virtual~Unmarshaller();

protected:

  Header m_fix_header;

  /** 
   * Template Function that convert a numeric string into a uintXX_t
   * Return FALSE if the string contain non digit or 
   *              if the length of the string bring to an oveflow of the variable "num",
   *              "num" is set to Zero.
   * Return TRUE  if OK and the result in the reference "num".
   *
   * @param str     pointer to the begin of the strint to check;
   * @param length  length of the string;
   * @param num     return value.
   */ 
  template<typename T> 
  bool my_atoi(const char *str, const size_t &length, T &num);
  
  /** Template Function that Compare the value pointed by "offset" long "length"
   * with the value of "marterValue".
   * Return:     TRUE if equels.
   *
   * @param offset      pointer to the begin of the strint to check;
   * @param length      length of the string;
   * @param masterValue master value to compare with.
   */
  template<typename T> 
  bool my_memcmp(const char *offset, const size_t &length, T &masterValue);

  /** 
   * Template Function that Copy the value pointed by "offset" long "length" minus tha padding
   * with the value ofin the variable referencec by "returnValue".
   * Return:     TRUE if OK. 
   *
   * @param offset pointer to the begin of the strint to check;
   * @param length length of the string;
   * @param returnValue reference to the destination variable.
   */
  template<typename T> 
  bool my_memcpy(const char *offset, const size_t &length, T &returnValue);


}; 


#endif  /*_UNMARSHALL_HPP */


/*=====================================================================
 *|  N.B. When using the "inclusion model" of templates, 
 *|  place both the declaration AND the definition of template functions 
 *|  into the header file */
//=====================================================================


//==============================================================================
/* Template Function that convert a numeric string into a uintXX_t
 * Return 1 if the string contain non digit or 
 *          if the length of the string bring to an oveflow of the variable "num",
 *          "num" is set to Zero.
 * Return 0 if OK and the result in the reference "num".
 */ 
template<typename T> 
bool Unmarshaller::my_atoi(const char *str, const size_t &length, T &num){
  
  num = 0;
  uint32_t t = sizeof(num); // size in # of Bytes

  t=t*2+(t/2+t%2);// compute the max # of decimals need for the conversion
  if(length>t) return false;
  for (size_t i=0; i<length; i++){
    int t = *str-'0';// convert the character in a digit 
    if((t < 0) || (t>9)){
      num = 0;
      return false;
    }
    num=(num<<3) + (num<<1) + t;
    // "<<" is the "Bitwise shift left" num=8*num+2*num+digit=10*num+digit
    str++;
  }
  return true;
} 


//==============================================================================
/* Template Function that Compare the value pointed by "offset" long "length"
 * with the value of "marterValue".
 * Return:     THRUE if equels.
 * Paremeters: pointer to the begin of the strint to check;
 *             length of the string;
 *             master value to compare with.
 */
template<typename T> 
bool Unmarshaller::my_memcmp(const char *offset, const size_t &length, T &masterValue){

  T str;
  int size = length-1;

  if(length >= sizeof(masterValue)) return false;
  while((*(offset+size)==' ')&&(size>=0)){    // Count how many blank spaces there are 
    size--;                                   // at the end of the string
  }
  memcpy(str, offset, size+1);                // Copy only the effective string (no padding)
  return(memcmp(str,masterValue, strlen(masterValue))==0); // Check if is the same of the "master"
}


//==============================================================================
/* Template Function that Copy the value pointed by "offset" long "length" minus tha padding
 * with the value ofin the variable referencec by "returnValue".
 * Return:     THRUE if OK.
 * Paremeters: pointer to the begin of the strint to check;
 *             length of the string;
 *             reference to the destination variable.
 */
template<typename T> 
bool Unmarshaller::my_memcpy(const char *offset, const size_t &length, T &returnValue){

  int size = length-1;

  if(length >= sizeof(returnValue)) return false;
  while((*(offset+size)==' ')&&(size>=0)){    // Count how many blank spaces there are 
    size--;                                   // at the end of the string
  }
  memcpy(returnValue, offset, size+1);        // Copy only the effective string (no padding)
  memset(returnValue+size+1, '\0', 1);        // Add string termonator
  return(true);
}



} //namespace format
} //namespace tape
} //namespace castor



