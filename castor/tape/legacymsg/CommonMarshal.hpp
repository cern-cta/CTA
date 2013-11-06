/******************************************************************************
 *                      castor/tape/legacymsg/CommonMarshal.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_LEGACYMSG_COMMONMARSHAL_HPP
#define CASTOR_TAPE_LEGACYMSG_COMMONMARSHAL_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"

#include <errno.h>
#include <stdint.h>
#include <string>
#include <time.h>


namespace castor    {
namespace tape      {
namespace legacymsg {
  
/**
 * Marshals the specified unsigned 8-bit integer into the specified
 * destination buffer.
 *
 * @param src The unsigned 8-bit integer to be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the unsigned 8-bit integer should be marshalled to and on
 * return points to the byte in the destination buffer immediately after.
 * the marshalled unsigned 8-bit integer.
 */
void marshalUint8(uint8_t src, char * &dst)
  throw(castor::exception::Exception);

/**
 * Unmarshals an unsigned 8-bit integer from the specified source buffer
 * into the specified destination unsigned 8-bit integer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the unsigned 8-bit integer should be unmarshalled from and
 * on return points to the byte in the source buffer immediately after the
 * unmarshalled unsigned 8-bit integer.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the unsigned 8-bit integer should be
 * unmarshalled and on return is the number of bytes remaining in the
 * source buffer.
 * @param dst The destination unsigned 8-bit integer.
 */
void unmarshalUint8(const char * &src, size_t &srcLen,
  uint8_t &dst) throw(castor::exception::Exception);

/**
 * Marshals the specified unsigned 16-bit integer into the specified
 * destination buffer.
 *
 * @param src The unsigned 16-bit integer to be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the unsigned 16-bit integer should be marshalled to and on
 * return points to the byte in the destination buffer immediately after
 * the marshalled unsigned 16-bit integer.
 */
void marshalUint16(uint16_t src, char * &dst)
  throw(castor::exception::Exception);

/**
 * Unmarshals an unsigned 16-bit integer from the specified source buffer
 * into the specified destination unsigned 16-bit integer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the unsigned 16-bit integer should be unmarshalled from and
 * on return points to the byte in the source buffer immediately after the
 * unmarshalled unsigned 16-bit integer.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the unsigned 16-bit integer should be
 * unmarshalled and on return is the number of bytes remaining in the
 * source buffer.
 * @param dst The destination unsigned 16-bit integer.
 */
void unmarshalUint16(const char * &src, size_t &srcLen,
  uint16_t &dst) throw(castor::exception::Exception);

/**
 * Marshals the specified unsigned 32-bit integer into the specified
 * destination buffer.
 *
 * @param src The unsigned 32-bit integer to be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the unsigned 32-bit integer should be marshalled to and on
 * return points to the byte in the destination buffer immediately after
 * the marshalled unsigned 32-bit integer.
 */
void marshalUint32(uint32_t src, char * &dst)
  throw(castor::exception::Exception);

/**
 * Unmarshals an unsigned 32-bit integer from the specified source buffer
 * into the specified destination unsigned 32-bit integer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the unsigned 32-bit integer should be unmarshalled from and
 * on return points to the byte in the source buffer immediately after the
 * unmarshalled unsigned 32-bit integer.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the unsigned 32-bit integer should be
 * unmarshalled and on return is the number of bytes remaining in the
 * source buffer.
 * @param dst The destination unsigned 32-bit integer.
 */
void unmarshalUint32(const char * &src, size_t &srcLen,
  uint32_t &dst) throw(castor::exception::Exception);

/**
 * Marshals the specified signed 32-bit integer into the specified
 * destination buffer.
 *
 * @param src The signed 32-bit integer to be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the signed 32-bit integer should be marshalled to and on
 * return points to the byte in the destination buffer immediately after
 * the marshalled signed 32-bit integer.
 */
void marshalInt32(int32_t src, char * &dst)
  throw(castor::exception::Exception);

/**
 * Unmarshals a signed 32-bit integer from the specified source buffer
 * into the specified destination unsigned 32-bit integer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the signed 32-bit integer should be unmarshalled from and
 * on return points to the byte in the source buffer immediately after the
 * unmarshalled signed 32-bit integer.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the signed 32-bit integer should be
 * unmarshalled and on return is the number of bytes remaining in the
 * source buffer.
 * @param dst The destination signed 32-bit integer.
 */
void unmarshalInt32(const char * &src, size_t &srcLen,
  int32_t &dst) throw(castor::exception::Exception);

/**
 * Marshals the specified unsigned 64-bit integer into the specified
 * destination buffer.
 *
 * @param src The unsigned 64-bit integer to be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the unsigned 64-bit integer should be marshalled to and on
 * return points to the byte in the destination buffer immediately after
 * the marshalled unsigned 64-bit integer.
 */
void marshalUint64(uint64_t src, char * &dst)
  throw(castor::exception::Exception);

/**
 * Unmarshals an unsigned 64-bit integer from the specified source buffer
 * into the specified destination unsigned 64-bit integer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the unsigned 64-bit integer should be unmarshalled from and
 * on return points to the byte in the source buffer immediately after the
 * unmarshalled unsigned 64-bit integer.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the unsigned 64-bit integer should be
 * unmarshalled and on return is the number of bytes remaining in the
 * source buffer.
 * @param dst The destination unsigned 64-bit integer.
 */
void unmarshalUint64(const char * &src, size_t &srcLen,
  uint64_t &dst) throw(castor::exception::Exception);

/**
 * Marshals the specified time_t value into the specified destination buffer.
 *
 * @param src The time_t value to be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the time_t value should be marshalled to and on
 * return points to the byte in the destination buffer immediately after
 * the marshalled time_t value.
 */
void marshalTime(time_t src, char * &dst)
  throw(castor::exception::Exception);

/**
 * Unmarshals a time_t value from the specified source buffer into the
 * specified destination.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the time_t value should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the unmarshalled
 * time_t value.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the time_t value should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination.
 */
void unmarshalTime(const char * &src, size_t &srcLen,
  time_t &dst) throw(castor::exception::Exception);

/**
 * Marshals the specified string into the specified destination buffer.
 *
 * @param src The string to be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the string should be marshalled to and on return points to
 * the byte in the destination buffer immediately after the marshalled
 * string.
 */
void marshalString(const char *src, char * &dst)
  throw(castor::exception::Exception);

/**
 * Marshals the specified string into the specified destination buffer.
 *
 * @param src The string to be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the string should be marshalled to and on return points to
 * the byte in the destination buffer immediately after the marshalled
 * string.
 */
void marshalString(const std::string &src, char * &dst)
  throw(castor::exception::Exception);

/**
 * Unmarshals a string from the specified source buffer into the specified
 * destination buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the string should be unmarshalled from and on return points
 * to the byte in the source buffer immediately after the unmarshalled
 * string.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the string should be unmarshalled and on return
 * is the number of bytes remaining in the source buffer
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the string should be unmarshalled to and on return points
 * to the byte in the destination buffer immediately after the unmarshalled
 * string .
 * @param dstLen The length of the destination buffer where the string
 * should be unmarshalled to.
 */
void unmarshalString(const char * &src, size_t &srcLen, char *dst,
  const size_t dstLen) throw(castor::exception::Exception);

/**
 * Unmarshals a string from the specified source buffer into the specified
 * destination buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the string should be unmarshalled from and on return points
 * to the byte in the source buffer immediately after the unmarshalled
 * string.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the string should be unmarshalled and on return
 * is the number of bytes remaining in the source buffer.
 * @param dst In/out parameter, before invocation points to the destination
 * buffer where the string should be unmarshalled to and on return points
 * to the byte in the destination buffer immediately after the unmarshalled
 * string.
 */
template<int n> void unmarshalString(const char * &src,
  size_t &srcLen, char (&dst)[n]) throw(castor::exception::Exception) {
  unmarshalString(src, srcLen, dst, n);
}

/**
 * Marshals the specified source message header structure into the
 * specified destination buffer.
 *
 * @param dst    The destination buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the header.
 */
size_t marshal(char *const dst, const size_t dstLen,
  const MessageHeader &src) throw(castor::exception::Exception);

/**
 * Marshals the specified source message header structure into the
 * specified destination buffer.
 *
 * @param dst The destination buffer.
 * @param src The source structure.
 * @return    The total length of the header.
 */
template<int n> size_t marshal(char (&dst)[n],
  const MessageHeader &src) throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

/**
 * Unmarshals a message header from the specified source buffer into the
 * specified destination message header structure.
 *
 * @param src    In/out parameter, before invocation points to the source
 *               buffer where the message header should be unmarshalled from
 *               and on return points to the byte in the source buffer
 *               immediately after the unmarshalled message header.
 * @param srcLen In/our parameter, before invocation is the length of the
 *               source buffer from where the message header should be
 *               unmarshalled and on return is the number of bytes remaining in
 *               the source buffer.
 * @param dst    The destination structure.
 */
void unmarshal(const char * &src, size_t &srcLen, MessageHeader &dst)
  throw(castor::exception::Exception);


/**
 * Marshals the specified src value into the specified destination buffer.
 *
 * @param src The source value be marshalled.
 * @param dst In/out parameter, before invocation points to the destination
 *            buffer where the source value should be marshalled to and on
 * return     Points to the byte in the destination buffer immediately after
 *            the marshalled value.
 */
template<typename T> void marshalValue(T src, char * &dst)
  throw(castor::exception::Exception) {

  if(dst == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to destination buffer is NULL";
    throw ex;
  }

  char *const src_ptr = (char *)(&src);

  // src: Intel x86 (little endian)
  // dst: Network   (big    endian)
  for(size_t i=sizeof(src); i>0; i--) {
    *dst++ = *(src_ptr + i - 1);
  }
}

/**
 * Unmarshals a value from the specified source buffer into the specified
 * destination.
 *
 * @param src    In/out parameter: Before invocation points to the source
 *               buffer where the value should be unmarshalled from and on
 *               return points to the byte in the source buffer immediately
 *               after the unmarshalled value.
 * @param srcLen In/our parameter: Before invocation is the length of the
 *               source buffer from where the value should be unmarshalled and
 *               on return is the number of bytes remaining in the source
 *               buffer.
 * @param dst    Out parameter: The destination.
 */
template<typename T> void unmarshalValue(const char * &src,
  size_t &srcLen, T &dst) throw(castor::exception::Exception) {

  if(src == NULL) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Pointer to source buffer is NULL";
    throw ex;
  }

  if(srcLen < sizeof(dst)) {
    castor::exception::Exception ex(EINVAL);

    ex.getMessage() << __PRETTY_FUNCTION__
      << ": Source buffer length is too small: Expected length: "
      << sizeof(dst) << ": Actual length: " << srcLen;
    throw ex;
  }

  char *const dst_ptr = (char *)(&dst);

  // src: Network   (big    endian)
  // dst: Intel x86 (little endian)
  for(size_t i=sizeof(dst); i>0; i--) {
    *(dst_ptr + i - 1) = *src++;
  }

  srcLen -= sizeof(dst);
}

} // namespace legacymsg
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_LEGACYMSG_COMMONMARSHAL_HPP
