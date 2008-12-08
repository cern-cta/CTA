/******************************************************************************
 *                      Marshaller.hpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP
#define CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/aggregator/MessageHeader.hpp"
#include "castor/tape/aggregator/RcpJobReplyMessage.hpp"
#include "castor/tape/aggregator/RcpJobRequestMessage.hpp"
#include "castor/tape/aggregator/RtcpAcknowledgeMessage.hpp"
#include "castor/tape/aggregator/RtcpTapeRequestMessage.hpp"

#include <errno.h>
#include <stdint.h>
#include <string>


namespace castor {
namespace tape {
namespace aggregator {
    
  /**
   * Collection of static methods to marshall / unmarshall network messages.
   */
  class Marshaller {
  private:

    /**
     * Function template to help marshall simple data types.  Please note that
     * this function does not perform a host to network byte order conversion.
     * If such a conversion is necessary, then it must be done before calling
     * this function.
     *
     * @param src The source data to be marshalled.
     * @param dst The destination buffer where the data will be marshalled to.
     */
    template<class T> static void marshall(T src, char * &dst)
      throw (castor::exception::Exception) {

      if(dst == NULL) {
        castor::exception::Exception ex(EINVAL);

        ex.getMessage() << ": Pointer to destination buffer is NULL";
        throw ex;
      }

      memcpy(dst, &src, sizeof(src));

      dst += sizeof(src);
    }

    /**
     * Function template to help unmarshal simple data types.  Please note that
     * this function does not perform a network to host byte order conversion.
     * If such a conversion is necessary, then it must be done after this
     * function has returned.
     *
     * @param src The source buffer from the where the data will be
     * unmarshalled.
     * @param dst The destination data to which the data will be unmarshalled.
     */
    template<class T> static void unmarshall(const char * &src, size_t &srcLen,
      T &dst) throw(castor::exception::Exception) {

      if(src == NULL) {
        castor::exception::Exception ex(EINVAL);

        ex.getMessage() << ": Pointer to source buffer is NULL";
        throw ex;
      }

      if(srcLen < sizeof(dst)) {
        castor::exception::Exception ex(EINVAL);

        ex.getMessage() << __PRETTY_FUNCTION__
          << ": Source buffer length is too small: Expected length: "
          << sizeof(dst) << ": Actual length: " << srcLen;
        throw ex;
      }

      memcpy(&dst, src, sizeof(dst));

      src    += sizeof(dst);
      srcLen -= sizeof(dst);
    }

  public:

    /**
     * Marshalls the specified unsigned 8-bit integer into the specified
     * destination buffer.
     *
     * @param src The unsigned 8-bit integer to be marshalled.
     * @param dst In/out parameter, before invocation points to the destination
     * buffer where the unsigned 8-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after.
     * the marshalled unsigned 8-bit integer.
     */
    static void marshallUint8(uint8_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls an unsigned 8-bit integer from the specified source buffer
     * into the specified destination unsigned 8-bit integer.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the unsigned 8-bit integer should be unmarshalled from and
     * on return points to the byte in the source buffer immediately after the
     * unmarshalled unsigned 8-bit integer.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the unsigned 8-bit integer should unmarshalled
     * and on return is the number of bytes remaining in the source buffer.
     * @param dst The destination unsigned 8-bit integer.
     */
    static void unmarshallUint8(const char * &src, size_t &srcLen,
      uint8_t &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified unsigned 16-bit integer into the specified
     * destination buffer.
     *
     * @param src The unsigned 16-bit integer to be marshalled.
     * @param dst In/out parameter, before invocation points to the destination
     * buffer where the unsigned 16-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled unsigned 16-bit integer.
     */
    static void marshallUint16(uint16_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls an unsigned 16-bit integer from the specified source buffer
     * into the specified destination unsigned 16-bit integer.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the unsigned 16-bit integer should be unmarshalled from and
     * on return points to the byte in the source buffer immediately after the
     * unmarshalled unsigned 16-bit integer.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the unsigned 16-bit integer should unmarshalled
     * and on return is the number of bytes remaining in the source buffer.
     * @param dst The destination unsigned 16-bit integer.
     */
    static void unmarshallUint16(const char * &src, size_t &srcLen,
      uint16_t &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified unsigned 32-bit integer into the specified
     * destination buffer.
     *
     * @param src The unsigned 32-bit integer to be marshalled.
     * @param dst In/out parameter, before invocation points to the destination
     * buffer where the unsigned 32-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled unsigned 32-bit integer.
     */
    static void marshallUint32(uint32_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls an unsigned 32-bit integer from the specified source buffer
     * into the specified destination unsigned 32-bit integer.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the unsigned 32-bit integer should be unmarshalled from and
     * on return points to the byte in the source buffer immediately after the
     * unmarshalled unsigned 32-bit integer.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the unsigned 32-bit integer should unmarshalled
     * and on return is the number of bytes remaining in the source buffer.
     * @param dst The destination unsigned 32-bit integer.
     */
    static void unmarshallUint32(const char * &src, size_t &srcLen,
      uint32_t &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified string into the specified destination buffer.
     *
     * @param src The string to be marshalled.
     * @param dst In/out parameter, before invocation points to the destination
     * buffer where the string should be marshalled to and on return points to
     * the byte in the destination buffer immediately after the marshalled
     * string.
     */
    static void marshallString(const char *src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified string into the specified destination buffer.
     *
     * @param src The string to be marshalled.
     * @param dst In/out parameter, before invocation points to the destination
     * buffer where the string should be marshalled to and on return points to
     * the byte in the destination buffer immediately after the marshalled
     * string.
     */
    static void marshallString(const std::string &src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls a string from the specified source buffer into the specified
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
     * should unmarshalled to.
     */
    static void unmarshallString(const char * &src, size_t &srcLen, char *dst,
      const size_t dstLen) throw(castor::exception::Exception);

    /**
     * Unmarshalls a string from the specified source buffer into the specified
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
    template<int n> static void unmarshallString(const char * &src,
      size_t &srcLen, char (&dst)[n]) throw(castor::exception::Exception) {
      unmarshallString(src, srcLen, dst, n);
    }

    /**
     * Marshalls the specified source message header structure into the
     * specified destination buffer.
     *
     * @param dst The destination buffer.
     * @param dstLen The length of the destination buffer.
     * @param src The source structure.
     * @return The total length of the header.
     */
    static size_t marshallMessageHeader(char *const dst, const size_t dstLen,
      const MessageHeader &src) throw(castor::exception::Exception);

    /**
     * Marshalls the specified source message header structure into the
     * specified destination buffer.
     *
     * @param dst The destination buffer.
     * @param src The source structure.
     * @return The total length of the header.
     */
    template<int n> static size_t marshallMessageHeader(char (&dst)[n],
      const MessageHeader &src) throw(castor::exception::Exception) {
      return  marshallMessageHeader(dst, n, src);
    }

    /**
     * Unmarshalls a message header from the specified source buffer into the
     * specified destination message header structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message header should be unmarshalled from and on
     * return points to the byte in the source buffer immediately after the
     * unmarshalled message header.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message header should unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination structure.
     */
    static void unmarshallMessageHeader(const char * &src, size_t &srcLen,
      MessageHeader &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified source RCP job submission request structure into
     * the specified destination buffer.
     *
     * @param dst The destination message buffer.
     * @param dstLen The length of the destination buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    static size_t marshallRcpJobRequestMessage(char *const dst,
      const size_t dstLen, const RcpJobRequestMessage &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source RCP job submission request structure into
     * the specified destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRcpJobRequestMessage(char (&dst)[n],
      const RcpJobRequestMessage &src) throw(castor::exception::Exception) {
      return  marshallRcpJobRequestMessage(dst, n, src);
    }

    /**
     * Unmarshalls the message body of an RCP job submission request from the
     * specified source buffer into the specified destination request
     * structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message body should unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination request structure.
     */
    static void unmarshallRcpJobRequestMessage(const char * &src,
      size_t &srcLen, RcpJobRequestMessage &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified RCP job reply structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer.
     * @param dstLen The length of the destination buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    static size_t marshallRcpJobReplyMessage(char *const dst,
      const size_t dstLen, const RcpJobReplyMessage &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified RCP job reply structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRcpJobReplyMessage(char (&dst)[n],
      const RcpJobReplyMessage &src) throw(castor::exception::Exception) {
      return  marshallRcpJobReplyMessage(dst, n, src);
    }

    /**
     * Unmarshalls the message body of an RCP job reply from the specified
     * source buffer into the specified destination reply structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message body should unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination reply structure.
     */
    static void unmarshallRcpJobReplyMessage(const char * &src, size_t &srcLen,
      RcpJobReplyMessage &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified source tape request structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer.
     * @param dstLen The length of the destination buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    static size_t marshallRtcpTapeRequestMessage(char *const dst,
      const size_t dstLen, const RtcpTapeRequestMessage &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source tape request structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRtcpTapeRequestMessage(
      char (&dst)[n], const RtcpTapeRequestMessage &src)
      throw(castor::exception::Exception) {
      return marshallRtcpTapeRequestMessage(dst, n, src);
    }

    /**
     * Unmarshalls the message body of a tape request from the specified source
     * buffer into the specified destination request structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message body should unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination request structure.
     */
    static void unmarshallRtcpTapeRequestMessage(const char * &src,
      size_t &srcLen, RtcpTapeRequestMessage &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source RTCP acknowledge message structure into
     * the specified destination buffer.
     *
     * @param dst The destination message buffer.
     * @param dstLen The length of the destination buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    static size_t marshallRtcpAcknowledgeMessage(char *const dst,
      const size_t dstLen, const RtcpAcknowledgeMessage &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source RTCP acknowledge message structure into
     * the specified destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRtcpAcknowledgeMessage(
      char (&dst)[n], const RtcpAcknowledgeMessage &src)
      throw(castor::exception::Exception) {
      return marshallRtcpAcknowledgeMessage(dst, n, src);
    }

    /**
     * Unmarshalls the message body of an RTCP acknowledge message from the
     * specified source buffer into the specified destination message
     * structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message body should unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination message structure.
     */
    static void unmarshallRtcpAcknowledgeMessage(const char * &src,
      size_t &srcLen, RtcpAcknowledgeMessage &dst)
      throw(castor::exception::Exception);

  }; // class Utils

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP
