#include "castor/tape/aggregator/Marshaller.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <iostream>
#include <string.h>


//------------------------------------------------------------------------------
// marshallUint32
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallUint32(uint32_t src,
  char * &dst) throw (castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Pointer to destination buffer is NULL";
    throw ex;
  }

  src = htonl(src);

  memcpy(dst, &src, sizeof(src));

  dst += sizeof(src);
}


//------------------------------------------------------------------------------
// unmarshallInt32
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallUint32(char * &src,
  size_t &srcLen, uint32_t &dst) throw(castor::exception::Exception) {

  if(src == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Pointer to source buffer is NULL";
    throw ex;
  }

  if(srcLen < sizeof(dst)) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Source buffer length is too small: Expected length: "
      << sizeof(dst) << ": Actual length: " << srcLen;
    throw ex;
  }

  memcpy(&dst, src, sizeof(dst));
  dst = ntohl(dst);

  src    += sizeof(dst);
  srcLen -= sizeof(dst);
}


//------------------------------------------------------------------------------
// marshallString
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallString(const char * src,
  char * &dst) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Pointer to destination buffer is NULL";
    throw ex;
  }

  strcpy(dst, src);

  dst += strlen(src) + 1;
}


//------------------------------------------------------------------------------
// marshallString
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::marshallString(
  const std::string &src, char * &dst) throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Pointer to destination buffer is NULL";
    throw ex;
  }

  strcpy(dst, src.c_str());

  dst += strlen(src.c_str()) + 1;
}


//------------------------------------------------------------------------------
// unmarshallString
//------------------------------------------------------------------------------
void castor::tape::aggregator::Marshaller::unmarshallString(char * &src,
  size_t &srcLen, char *dst, const size_t dstLen)
  throw (castor::exception::Exception) {

  if(src == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Pointer to source buffer is NULL";
    throw ex;
  }

  if(srcLen == 0) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Source buffer length is 0";
    throw ex;
  }

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Pointer to destination buffer is NULL";
    throw ex;
  }

  if(dstLen == 0) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "Destination buffer length is 0";
    throw ex;
  }

  // Calculate the maximum number of bytes that could be unmarshalled
  size_t maxlen = 0;
  if(srcLen < dstLen) {
    maxlen = srcLen;
  } else {
    maxlen = dstLen;
  }

  bool strTerminatorReached = false;

  // While there are potential bytes to copy and the string terminator has not
  // been reached
  for(size_t i=0; i<maxlen && !strTerminatorReached; i++) {
    // If the string terminator has been reached
    if((*dst++ = *src++) == '\0') {
      strTerminatorReached = true;
    }

    srcLen--;
  }

  // If all potential bytes were copied but the string terminator was not
  // reached
  if(!strTerminatorReached) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << "String terminator of source buffer was not reached";
    throw ex;
  }
}
