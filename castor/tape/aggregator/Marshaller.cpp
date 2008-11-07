#include "castor/tape/aggregator/Marshaller.hpp"

#include <arpa/inet.h>
#include <string.h>


//------------------------------------------------------------------------------
// marshallString
//------------------------------------------------------------------------------
char* castor::tape::aggregator::Marshaller::marshallString(char *const ptr,
  const char *const value) throw() {

  strcpy(ptr, value);

  return ptr + strlen(value) + 1;
}


//------------------------------------------------------------------------------
// unmarshallInt32
//------------------------------------------------------------------------------
char* castor::tape::aggregator::Marshaller::unmarshallUint32(char *const ptr,
  uint32_t &value) throw() {

  value = htonl(value);

  memcpy(ptr, &value, sizeof(value));

  return ptr + sizeof(value);
}
