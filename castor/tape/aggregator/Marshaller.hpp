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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP
#define CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/aggregator/MessageHeader.hpp"
#include "castor/tape/aggregator/RcpJobReplyMsgBody.hpp"
#include "castor/tape/aggregator/RcpJobRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpAcknowledgeMsg.hpp"
#include "castor/tape/aggregator/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstErrMsgBody.hpp"

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
     * source buffer from where the unsigned 8-bit integer should be
     * unmarshalled and on return is the number of bytes remaining in the
     * source buffer.
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
     * source buffer from where the unsigned 16-bit integer should be
     * unmarshalled and on return is the number of bytes remaining in the
     * source buffer.
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
     * source buffer from where the unsigned 32-bit integer should be
     * unmarshalled and on return is the number of bytes remaining in the
     * source buffer.
     * @param dst The destination unsigned 32-bit integer.
     */
    static void unmarshallUint32(const char * &src, size_t &srcLen,
      uint32_t &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified signed 32-bit integer into the specified
     * destination buffer.
     *
     * @param src The signed 32-bit integer to be marshalled.
     * @param dst In/out parameter, before invocation points to the destination
     * buffer where the signed 32-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled signed 32-bit integer.
     */
    static void marshallInt32(int32_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls a signed 32-bit integer from the specified source buffer
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
    static void unmarshallInt32(const char * &src, size_t &srcLen,
      int32_t &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified unsigned 64-bit integer into the specified
     * destination buffer.
     *
     * @param src The unsigned 64-bit integer to be marshalled.
     * @param dst In/out parameter, before invocation points to the destination
     * buffer where the unsigned 64-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled unsigned 64-bit integer.
     */
    static void marshallUint64(uint64_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls an unsigned 64-bit integer from the specified source buffer
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
    static void unmarshallUint64(const char * &src, size_t &srcLen,
      uint64_t &dst) throw(castor::exception::Exception);

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
     * should be unmarshalled to.
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
     * source buffer from where the message header should be unmarshalled and on
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
    static size_t marshallRcpJobRqstMsgBody(char *const dst,
      const size_t dstLen, const RcpJobRqstMsgBody &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source RCP job submission request structure into
     * the specified destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRcpJobRqstMsgBody(char (&dst)[n],
      const RcpJobRqstMsgBody &src) throw(castor::exception::Exception) {
      return  marshallRcpJobRqstMsgBody(dst, n, src);
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
     * source buffer from where the message body should be unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination request structure.
     */
    static void unmarshallRcpJobRqstMsgBody(const char * &src,
      size_t &srcLen, RcpJobRqstMsgBody &dst)
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
    static size_t marshallRcpJobReplyMsgBody(char *const dst,
      const size_t dstLen, const RcpJobReplyMsgBody &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified RCP job reply structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRcpJobReplyMsgBody(char (&dst)[n],
      const RcpJobReplyMsgBody &src) throw(castor::exception::Exception) {
      return  marshallRcpJobReplyMsgBody(dst, n, src);
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
     * source buffer from where the message body should be unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination reply structure.
     */
    static void unmarshallRcpJobReplyMsgBody(const char * &src,
      size_t &srcLen, RcpJobReplyMsgBody &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source tape request structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer.
     * @param dstLen The length of the destination buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    static size_t marshallRtcpTapeRqstErrMsgBody(char *const dst,
      const size_t dstLen, const RtcpTapeRqstErrMsgBody &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source tape request structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRtcpTapeRqstErrMsgBody(
      char (&dst)[n], const RtcpTapeRqstErrMsgBody &src)
      throw(castor::exception::Exception) {
      return marshallRtcpTapeRqstErrMsgBody(dst, n, src);
    }

    /**
     * Unmarshalls the message body of a tape request with error appendix from
     * the specified source buffer into the specified destination request
     * structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message body should be unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination request structure.
     */
    static void unmarshallRtcpTapeRqstErrMsgBody(const char * &src,
      size_t &srcLen, RtcpTapeRqstErrMsgBody &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls the message body of a tape request without error appendix
     * from the specified source buffer into the specified destination request
     * structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message body should be unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination request structure.
     */
    static void unmarshallRtcpTapeRqstMsgBody(const char * &src,
      size_t &srcLen, RtcpTapeRqstMsgBody &dst)
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
    static size_t marshallRtcpAcknowledgeMsg(char *const dst,
      const size_t dstLen, const RtcpAcknowledgeMsg &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source RTCP acknowledge message structure into
     * the specified destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRtcpAcknowledgeMsg(
      char (&dst)[n], const RtcpAcknowledgeMsg &src)
      throw(castor::exception::Exception) {
      return marshallRtcpAcknowledgeMsg(dst, n, src);
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
     * source buffer from where the message body should be unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination message structure.
     */
    static void unmarshallRtcpAcknowledgeMsg(const char * &src,
      size_t &srcLen, RtcpAcknowledgeMsg &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source file request with error appendix
     * structure into the specified destination buffer.
     *
     * @param dst The destination message buffer.
     * @param dstLen The length of the destination buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    static size_t marshallRtcpFileRqstErrMsgBody(char *const dst,
      const size_t dstLen, const RtcpFileRqstErrMsgBody &src)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified source file request with error appendix
     * structure into the specified destination buffer.
     *
     * @param dst The destination message buffer.
     * @param src The source structure.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRtcpFileRqstErrMsgBody(
      char (&dst)[n], const RtcpFileRqstErrMsgBody &src)
      throw(castor::exception::Exception) {
      return marshallRtcpFileRqstErrMsgBody(dst, n, src);
    }

    /**
     * Unmarshalls the message body of a file request with error appendix from
     * the specified source buffer into the specified destination request
     * structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message body should be unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination request structure.
     */
    static void unmarshallRtcpFileRqstErrMsgBody(const char * &src,
      size_t &srcLen, RtcpFileRqstErrMsgBody &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls the message body of a file request without error appendix
     * from the specified source buffer into the specified destination request
     * structure.
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the message body should be unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination request structure.
     */
    static void unmarshallRtcpFileRqstMsgBody(const char * &src,
      size_t &srcLen, RtcpFileRqstMsgBody &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls a "no more requests" message into the specified destination
     * buffer.
     *
     * Please note that a "no more requests" message is bodiless with body
     * length field of the header being set to 0.
     *
     * @param dst The destination message buffer.
     * @param dstLen The length of the destination buffer.
     * @return The total length of the message (header + body).
     */
    static size_t marshallRtcpNoMoreRequestsMsgBody(char *const dst,
      const size_t dstLen) throw(castor::exception::Exception);

    /**
     * Marshalls a "no more requests" message into the specified destination
     * buffer.
     *
     * Please note that a "no more requests" message is bodiless with body
     * length field of the header being set to 0.
     *
     * @param dst The destination message buffer.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRtcpNoMoreRequestsMsgBody(
      char (&dst)[n])
      throw(castor::exception::Exception) {
      return marshallRtcpNoMoreRequestsMsgBody(dst, n);
    }

   /**
     * Marshalls an "abort" message into the specified destination
     * buffer.
     *
     * Please note that a "abort" message is bodiless with body
     * length field of the header being set to 0.
     *
     * @param dst The destination message buffer.
     * @return The total length of the message (header + body).
     **/
    static size_t marshallRtcpAbortMsg(
      char *dst, const size_t dstLen) throw(castor::exception::Exception); 

     /**
     * Marshalls an "abort" message into the specified destination
     * buffer.
     *
     * Please note that a "abort" message is bodiless with body
     * length field of the header being set to 0.
     *
     * @param dst The destination message buffer.
     * @return The total length of the message (header + body).
     */
    template<int n> static size_t marshallRtcpAbortMsg(
      char (&dst)[n])
      throw(castor::exception::Exception) {
      return marshallRtcpAbortMsg(dst, n);
    }


  private:

    /**
     * Marshalls the specified integer into the specified destination buffer.
     *
     * The type of the source integer is a typename template variable to allow
     * this implementation to be instantiated for unsigned and signed integers
     * and for integers of different sizes (8, 16, 32, and 64).
     *
     * @param src The integer to be marshalled.
     * @param dst In/out parameter, before invocation points to the destination
     * buffer where the integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled integer.
     */
    template<typename T> static void marshallInteger(T src, char * &dst)
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
     * Unmarshalls an integer from the specified source buffer into the
     * specified destination integer.
     *
     * The type of the destination integer is a typename template variable to
     * allow this implementation to be instantiated for unsigned and signed
     * integers and for integers of different sizes (8, 16, 32, and 64).
     *
     * @param src In/out parameter, before invocation points to the source
     * buffer where the integer should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled integer.
     * @param srcLen In/our parameter, before invocation is the length of the
     * source buffer from where the integer should be unmarshalled and on
     * return is the number of bytes remaining in the source buffer.
     * @param dst The destination integer.
     */
    template<typename T> static void unmarshallInteger(const char * &src,
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
  }; // class Marshaller

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP
