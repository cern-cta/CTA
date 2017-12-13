/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Input stream to receive a stream of protocol buffers from the server
 * @copyright      Copyright 2017 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <iostream> // delete me
#include <google/protobuf/io/coded_stream.h>
#include "XrdSsiPbException.hpp"

namespace XrdSsiPb {

/*!
 * Input Stream Buffer class
 *
 * This implementation is for a record-based stream. The client should be configured with a XrdSsi stream buffer size
 * which is large with respect to the maximum size of DataType. This is mainly for efficiency reasons as there is a
 * little extra data copying overhead in handling records which are split across two SSI buffers. Note that there is
 * also a hard limit: the record size cannot exceed the buffer size.
 *
 * The buffer size parameter is set in the constructor to XrdSsiPbServiceClientSide. The size of DataType is the maximum
 * encoded size of the DataType protocol buffer on the wire.
 *
 * If there is a requirement to stream arbitrarily large binary blobs rather than records, this functionality will need
 * to be added. See the comments on Request::ProcessResponseData().
 */
template<typename DataType>
class IStreamBuffer
{
public:
   IStreamBuffer(uint32_t bufsize) :
      m_max_msglen(bufsize-sizeof(uint32_t)),
      m_split_buffer(std::unique_ptr<uint8_t[]>(new uint8_t[m_max_msglen])),
      m_split_buflen(0)
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] IStreamBuffer() constructor" << std::endl;
#endif
   }

   ~IStreamBuffer() {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] IStreamBuffer() destructor" << std::endl;
#endif
   }

   /*!
    * Push a new buffer onto the input stream
    *
    * NOTE: This method is not reentrant; it is assumed it will be called from the XrdSsi framework
    *       in single-threaded mode. Each client or client thread must set up its own stream.
    *
    * @param[in]    buf_ptr    XRootD SSI stream or data buffer
    * @param[in]    buf_len    Size of buf_ptr
    */
   void Push(const char *buf_ptr, int buf_len);

private:
   /*!
    * Pop a single record from an input stream and pass it to the client
    *
    * If the message is split across the boundary between buffers, the partial message is saved and
    * the method returns false.
    *
    * @param[in]        msg_len         Size of the next Protocol Buffer message on the wire
    * @param[in,out]    input_stream    Protocol Buffer Coded Input Stream object wrapping the XRootD
    *                                   SSI buffer
    *
    * @retval    true     There are more messages to process on the stream
    * @retval    false    End of the input stream was reached
    */
   bool popRecord(int msg_len, google::protobuf::io::CodedInputStream &input_stream);

   /*!
    * Callback to handle stream data
    *
    * Define a specialised version of this method on the client side to handle a specific type of data stream
    */
   void DataCallback(DataType record) const {
      throw XrdSsiException("Stream/data payload received, but IStreamBuffer::DataCallback() has not been defined");
   }

   // Member variables

   const uint32_t             m_max_msglen;      //!< Maximum allowed length of a protobuf on the wire
   std::unique_ptr<uint8_t[]> m_split_buffer;    //!< Holding buffer for partial messages split across two input buffers
   int                        m_split_buflen;    //!< Length of data stored in m_split_buffer
};



template<typename DataType>
void IStreamBuffer<DataType>::Push(const char *buf_ptr, int buf_len)
{
   google::protobuf::io::CodedInputStream input_stream(reinterpret_cast<const uint8_t*>(buf_ptr), buf_len);

   uint32_t msg_len;

   if(m_split_buflen > 0) {
      // Stitch together the saved partial record and the incoming record

      if(m_split_buflen <= sizeof(uint32_t)) {
         // The size field is split across the boundary, just copy that one field

         int bytes_to_copy = sizeof(uint32_t) - m_split_buflen;
         memcpy(m_split_buffer.get() + m_split_buflen, buf_ptr, bytes_to_copy);
         input_stream.Skip(bytes_to_copy);

         google::protobuf::io::CodedInputStream::ReadLittleEndian32FromArray(m_split_buffer.get(), &msg_len);
std::cerr << "[1] " << msg_len << std::endl;
         popRecord(msg_len, input_stream);
      } else {
         // The payload is split across the boundary, copy the entire record

         google::protobuf::io::CodedInputStream::ReadLittleEndian32FromArray(m_split_buffer.get(), &msg_len);
         if(msg_len > m_max_msglen) {
            throw XrdSsiException("IStreamBuffer::Push(): Data record size (" + std::to_string(msg_len) +
               " bytes) exceeds XRootD SSI buffer size (" + std::to_string(m_max_msglen) + " bytes)");
         }
         int bytes_to_copy = msg_len + sizeof(uint32_t) - m_split_buflen;
         memcpy(m_split_buffer.get() + m_split_buflen, buf_ptr, bytes_to_copy);
         input_stream.Skip(bytes_to_copy);

         google::protobuf::io::CodedInputStream split_stream(reinterpret_cast<const uint8_t*>(m_split_buffer.get() + sizeof(uint32_t)), msg_len);
std::cerr << "[2] " << msg_len << std::endl;
         popRecord(msg_len, split_stream);
      }
   }

   // Extract remaining records from the input buffer

   do {
      const char *buf_ptr;

      // Get pointer to next record
      if(!input_stream.GetDirectBufferPointer(reinterpret_cast<const void**>(&buf_ptr), &buf_len)) break;
std::cerr << "[3] buf_len = " << buf_len;

      if(buf_len < static_cast<int>(sizeof(uint32_t))) {
         // Size field is split across the boundary, save the partial field and finish
         m_split_buflen = buf_len;
         memcpy(m_split_buffer.get(), buf_ptr, m_split_buflen);
         break;
      }

      // Get size of next item on the stream
      input_stream.ReadLittleEndian32(&msg_len);
std::cerr << ", msg_len = " << msg_len << std::endl;
   } while(popRecord(msg_len, input_stream));
}



template<typename DataType>
bool IStreamBuffer<DataType>::popRecord(int msg_len, google::protobuf::io::CodedInputStream &input_stream)
{
   const char *buf_ptr;
   int buf_len;

   if(msg_len > m_max_msglen) {
      throw XrdSsiException("IStreamBuffer::popRecord(): Data record size (" + std::to_string(msg_len) +
         " bytes) exceeds XRootD SSI buffer size (" + std::to_string(m_max_msglen) + " bytes)");
   }

   // Get pointer to next record
   if(!input_stream.GetDirectBufferPointer(reinterpret_cast<const void**>(&buf_ptr), &buf_len)) buf_len = 0;

   if(buf_len < msg_len) {
      // Record payload is split across the boundary, save the partial record
      google::protobuf::io::CodedOutputStream::WriteLittleEndian32ToArray(msg_len, m_split_buffer.get());
      memcpy(m_split_buffer.get() + sizeof(uint32_t), buf_ptr, buf_len);
      m_split_buflen = buf_len + sizeof(uint32_t);

      return false;
   } else {
      DataType record;

      record.ParseFromArray(buf_ptr, msg_len);
      input_stream.Skip(msg_len);
      DataCallback(record);

      // If the message terminates at the end of the buffer, we are done, otherwise keep going
      return buf_len != msg_len;
   }
}

} // namespace XrdSsiPb

