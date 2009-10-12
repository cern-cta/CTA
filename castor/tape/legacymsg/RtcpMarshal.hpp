/******************************************************************************
 *                      castor/tape/legacymsg/RtcpUtils.hpp
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

#ifndef CASTOR_TAPE_LEGACYMSG_RTCPMARSHAL_HPP
#define CASTOR_TAPE_LEGACYMSG_RTCPMARSHAL_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/GiveOutpMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpJobReplyMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpAbortMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpDumpTapeRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpNoMoreRequestsMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpFileRqstErrMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/legacymsg/RtcpTapeRqstErrMsgBody.hpp"

#include <errno.h>
#include <stdint.h>
#include <string>


namespace castor    {
namespace tape      {
namespace legacymsg {

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst    The destination message buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen,
  const RtcpJobRqstMsgBody &src) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst The destination message buffer.
 * @param src The source structure.
 * @return    The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n],
  const RtcpJobRqstMsgBody &src) throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen,
  RtcpJobRqstMsgBody &dst) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst    The destination message buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen,
  const RtcpJobReplyMsgBody &src) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst The destination message buffer.
 * @param src The source structure.
 * @return    The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n],
  const RtcpJobReplyMsgBody &src) throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen,
  RtcpJobReplyMsgBody &dst) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst    The destination message buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen,
  const RtcpTapeRqstErrMsgBody &src) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst The destination message buffer.
 * @param src The source structure.
 * @return    The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n],
  const RtcpTapeRqstErrMsgBody &src) throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen,
  RtcpTapeRqstErrMsgBody &dst) throw(castor::exception::Exception);

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen,
  RtcpTapeRqstMsgBody &dst) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst    The destination message buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen,
  const RtcpFileRqstErrMsgBody &src) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst The destination message buffer.
 * @param src The source structure.
 * @return    The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n],
  const RtcpFileRqstErrMsgBody &src) throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen,
  RtcpFileRqstErrMsgBody &dst) throw(castor::exception::Exception);

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen,
  RtcpFileRqstMsgBody &dst) throw(castor::exception::Exception);

/**
 * Unmarshals a message body with the specified destination structure type
 * from the specified source buffer.
 *
 * @param src In/out parameter, before invocation points to the source
 * buffer where the message body should be unmarshalled from and on return
 * points to the byte in the source buffer immediately after the
 * unmarshalled message body.
 * @param srcLen In/our parameter, before invocation is the length of the
 * source buffer from where the message body should be unmarshalled and on
 * return is the number of bytes remaining in the source buffer.
 * @param dst The destination message body structure.
 */
void unmarshal(const char * &src, size_t &srcLen,
  GiveOutpMsgBody &dst) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst    The destination message buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen,
  const RtcpNoMoreRequestsMsgBody &src) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst The destination message buffer.
 * @param src The source structure.
 * @return    The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n],
  const RtcpNoMoreRequestsMsgBody &src)
  throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst    The destination message buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen,
  const RtcpAbortMsgBody &src) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst The destination message buffer.
 * @param src The source structure.
 * @return    The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n],
  const RtcpAbortMsgBody &src) throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst    The destination message buffer.
 * @param dstLen The length of the destination buffer.
 * @param src    The source structure.
 * @return       The total length of the message (header + body).
 */
size_t marshal(char *const dst, const size_t dstLen,
  const RtcpDumpTapeRqstMsgBody &src) throw(castor::exception::Exception);

/**
 * Marshalls the specified source message body structure into the
 * specified destination buffer.
 *
 * @param dst The destination message buffer.
 * @param src The source structure.
 * @return    The total length of the message (header + body).
 */
template<int n> size_t marshal(char (&dst)[n],
  const RtcpDumpTapeRqstMsgBody &src) throw(castor::exception::Exception) {
  return marshal(dst, n, src);
}

} // namespace legacymsg
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_LEGACYMSG_RTCPMARSHAL_HPP
