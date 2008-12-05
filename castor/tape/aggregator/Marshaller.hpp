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
#include "castor/tape/aggregator/RcpJobReply.hpp"
#include "castor/tape/aggregator/RcpJobRequest.hpp"
#include "castor/tape/aggregator/RtcpTapeRequest.hpp"

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
     * Function template to help in marshalling simple data types.  Please note
     * that this function does not perform a host to network byte order
     * conversion.  If such a conversion is necessary, then it must be done
     * before calling this function.
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
     * Function template to help in unmarshalling simple data types.  Please
     * note that this function does not perform a network to host byte order
     * conversion.  If such a conversion is necessary, then it must be done
     * after this function has returned.
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
     * @param src the unsigned 8-bit integer to be marshalled
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the unsigned 8-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled unsigned 8-bit integer
     */
    static void marshallUint8(uint8_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls an unsigned 8-bit integer from the specified source buffer
     * into the specified destination unsigned 8-bit integer.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the unsigned 8-bit integer should be unmarshalled from and
     * on return points to the byte in the source buffer immediately after the
     * unmarshalled unsigned 8-bit integer
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the unsigned 8-bit integer should unmarshalled
     * and on return is the number of bytes remaining in the source buffer
     * @param dst the destination unsigned 8-bit integer
     */
    static void unmarshallUint8(const char * &src, size_t &srcLen,
      uint8_t &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified unsigned 16-bit integer into the specified
     * destination buffer.
     *
     * @param src the unsigned 16-bit integer to be marshalled
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the unsigned 16-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled unsigned 16-bit integer
     */
    static void marshallUint16(uint16_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls an unsigned 16-bit integer from the specified source buffer
     * into the specified destination unsigned 16-bit integer.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the unsigned 16-bit integer should be unmarshalled from and
     * on return points to the byte in the source buffer immediately after the
     * unmarshalled unsigned 16-bit integer
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the unsigned 16-bit integer should unmarshalled
     * and on return is the number of bytes remaining in the source buffer
     * @param dst the destination unsigned 16-bit integer
     */
    static void unmarshallUint16(const char * &src, size_t &srcLen,
      uint16_t &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified unsigned 32-bit integer into the specified
     * destination buffer.
     *
     * @param src the unsigned 32-bit integer to be marshalled
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the unsigned 32-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled unsigned 32-bit integer
     */
    static void marshallUint32(uint32_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls an unsigned 32-bit integer from the specified source buffer
     * into the specified destination unsigned 32-bit integer.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the unsigned 32-bit integer should be unmarshalled from and
     * on return points to the byte in the source buffer immediately after the
     * unmarshalled unsigned 32-bit integer
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the unsigned 32-bit integer should unmarshalled
     * and on return is the number of bytes remaining in the source buffer
     * @param dst the destination unsigned 32-bit integer
     */
    static void unmarshallUint32(const char * &src, size_t &srcLen,
      uint32_t &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified string into the specified destination buffer.
     *
     * @param src the string to be marshalled
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the string should be marshalled to and on return points to
     * the byte in the destination buffer immediately after the marshalled
     * string
     */
    static void marshallString(const char *src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified string into the specified destination buffer.
     *
     * @param src the string to be marshalled
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the string should be marshalled to and on return points to
     * the byte in the destination buffer immediately after the marshalled
     * string
     */
    static void marshallString(const std::string &src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls a string from the specified source buffer into the specified
     * destination buffer.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the string should be unmarshalled from and on return points
     * to the byte in the source buffer immediately after the unmarshalled
     * string
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the string should be unmarshalled and on return
     * is the number of bytes remaining in the source buffer
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the string should be unmarshalled to and on return points
     * to the byte in the destination buffer immediately after the unmarshalled
     * string 
     * @param dstLen the length of the destination buffer where the string
     * should unmarshalled to
     */
    static void unmarshallString(const char * &src, size_t &srcLen, char *dst,
      const size_t dstLen) throw(castor::exception::Exception);

    /**
     * Unmarshalls a string from the specified source buffer into the specified
     * destination buffer.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the string should be unmarshalled from and on return points
     * to the byte in the source buffer immediately after the unmarshalled
     * string
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the string should be unmarshalled and on return
     * is the number of bytes remaining in the source buffer
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the string should be unmarshalled to and on return points
     * to the byte in the destination buffer immediately after the unmarshalled
     * string
     */
    template<int n> static void unmarshallString(const char * &src,
      size_t &srcLen, char (&dst)[n]) throw(castor::exception::Exception) {
      unmarshallString(src, srcLen, dst, n);
    }

    /**
     * Marshalls the specified source RCP job submission request structure into
     * the specified destination buffer.
     *
     * @param dst The destination message buffer
     * @param dstLen The length of the destination buffer
     * @param src The source structure
     * @return The total length of the message (header + body)
     */
    static size_t marshallRcpJobRequest(char *const dst, const size_t dstLen,
      const RcpJobRequest &src) throw (castor::exception::Exception);

    /**
     * Marshalls the specified source RCP job submission request structure into
     * the specified destination buffer.
     *
     * @param dst The destination message buffer
     * @param src The source structure
     * @return The total length of the message (header + body)
     */
    template<int n> static size_t marshallRcpJobRequest(char (&dst)[n],
      const RcpJobRequest &src) throw(castor::exception::Exception) {
      return  marshallRcpJobRequest(dst, n, src);
    }

    /**
     * Unmarshalls the message body of an RCP job submission request from the
     * specified source buffer into the specified destination request
     * structure.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the message body should unmarshalled and on
     * return is the number of bytes remaining in the source buffer
     * @param dst the destination request structure
     */
    static void unmarshallRcpJobRequest(const char * &src, size_t &srcLen,
      RcpJobRequest &dst) throw(castor::exception::Exception);

    /**
     * Unmarshalls the message body of an RCP job reply from the specified
     * source buffer into the specified destination reply structure.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the message body should unmarshalled and on
     * return is the number of bytes remaining in the source buffer
     * @param dst the destination reply structure
     */
    static void unmarshallRcpJobReply(const char * &src, size_t &srcLen,
      RcpJobReply &dst) throw(castor::exception::Exception);

    /**
     * Marshalls the specified status code and possible error message into
     * the specified destination buffer in order to create an RTCP acknowledge
     * message.
     *
     * @param dst The destination message buffer
     * @param dstLen The length of the destination buffer
     * @param status The status code to be marshalled
     * @param errorMsg The error message to be marshalled
     * @return The total length of the message (header + body)
     */
    static size_t marshallRtcpAckn(char *const dst, const size_t dstLen,
      const uint32_t status, const char *errorMsg)
      throw (castor::exception::Exception);

    /**
     * Marshalls the specified source tape request structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer
     * @param dstLen The length of the destination buffer
     * @param src The source structure
     * @return The total length of the message (header + body)
     */
    static size_t marshallRtcpTapeRequest(char *const dst, const size_t dstLen,
      const RtcpTapeRequest &src) throw(castor::exception::Exception);

    /**
     * Marshalls the specified source tape request structure into the specified
     * destination buffer.
     *
     * @param dst The destination message buffer
     * @param src The source structure
     * @return The total length of the message (header + body)
     */
    template<int n> static size_t marshallRtcpTapeRequest(char (&dst)[n],
      const RtcpTapeRequest &src) throw(castor::exception::Exception) {
      return marshallRtcpTapeRequest(dst, n, src);
    }

    /**
     * Unmarshalls the message body of a tape request from the specified source
     * buffer into the specified destination request structure.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the message body should be unmarshalled from and on return
     * points to the byte in the source buffer immediately after the
     * unmarshalled message body
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the message body should unmarshalled and on
     * return is the number of bytes remaining in the source buffer
     * @param dst the destination request structure
     */
    static void unmarshallRtcpTapeRequest(const char * &src, size_t &srcLen,
      RtcpTapeRequest &dst) throw(castor::exception::Exception);

  }; // class Utils

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP
