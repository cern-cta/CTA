#ifndef _HEADER_HPP

#define	_HEADER_HPP

#include <cstring>

namespace castor {
  namespace tape   {
    namespace format {

      /**
       * Standard fields used by all versions of the ALB (Ansi Lable with
       * Block formated migration) tape formats.
       */
      struct Header {
	char        version_number[VERSION_NUMBER_LEN+1];   //  5 bytes
	uint16_t    header_size;                            //  2 bytes original values 5 digits

	virtual ~Header() {}
      };
    } // namespace format
  } // namespace tape
} // namespace castor


#endif // _HEADER_HPP
