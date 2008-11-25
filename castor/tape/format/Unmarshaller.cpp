
#include "castor/tape/format/Unmarshaller.hpp"

#include <iostream>
#include <stdlib.h>


//==============================================================================
//                         Unmarshaller
//==============================================================================

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
castor::tape::format::Unmarshaller::~Unmarshaller() {
  // Do nothing
}


char *castor::tape::format::Unmarshaller::readVersion(char *block){
    
  my_memcpy(block+VERSION_NUMBER_OFFSET, VERSION_NUMBER_LEN, m_fix_header.version_number);
  my_atoi(block+HEADER_SIZE_OFFSET, HEADER_SIZE_LEN, m_fix_header.header_size);

  return m_fix_header.version_number;
}





