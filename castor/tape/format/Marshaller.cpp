
#include "castor/tape/format/Marshaller.hpp"
#include <string.h>

#include <iostream>
#include <stdlib.h>


//==============================================================================
//                         Marshaller
//==============================================================================

//----------------------------------------------------------------------------
// Destructor
//----------------------------------------------------------------------------
castor::tape::format::Marshaller::~Marshaller() {
  // Do nothing
}


void castor::tape::format::Marshaller::parseCharData(char *const buff, const char *value, const size_t length, const size_t offset, const char pad){
    
    uint16_t size_diff;
        
    if(strlen(value) < length){
        if(isblank(pad)){// ' ' pad and left justified
            size_diff = length-strlen(value);
            memset(buff+offset+strlen(value), pad, size_diff);
            memcpy(buff+offset, value, length-size_diff); 
        }
        else{// '0' pad and right justified
            size_diff = length-strlen(value);
            memset(buff+offset, pad, size_diff);
            memcpy(buff+offset+size_diff, value, length-size_diff); 
        }
    }
    else{// left truncate
        size_diff = strlen(value)-length;
        memcpy(buff+offset, value+size_diff, length); 
    }
return;
}
