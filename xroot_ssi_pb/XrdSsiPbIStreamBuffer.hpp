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

#include <google/protobuf/io/coded_stream.h>

#include "XrdSsiPbException.hpp"

namespace XrdSsiPb {

/*!
 * Input Stream Buffer class
 *
 * This implementation is for a record-based stream, i.e. the client must be configured with a XrdSsi stream buffer size
 * which is large with respect to the maximum size of DataType. This is mainly for efficiency reasons as there is a
 * little extra overhead in handling records which are split across two SSI buffers. There is a hard limit that the
 * record size cannot exceed the buffer size.
 *
 * The buffer size parameter is set in the constructor to XrdSsiPbServiceClientSide. The size of DataType is the maximum
 * encoded size of the DataType protocol buffer on the wire.
 *
 * If there is a requirement to stream arbitrarily large binary blobs rather than records, this functionality needs to
 * be added. See the comments on Request::ProcessResponseData().
 */
template<typename DataType>
class IStreamBuffer
{
public:
   IStreamBuffer(uint32_t bufsize) :
      m_max_msglen(bufsize-sizeof(uint32_t)),
      m_split_buflen(0)
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] IStreamBuffer() constructor" << std::endl;
#endif
      m_split_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[m_max_msglen]);
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
    */
   void push(const char *buf_ptr, int buf_len);

private:
   /*!
    * Pop a single record of known size from an input stream and pass it to the client
    */
   void popRecord(int msg_len, google::protobuf::io::CodedInputStream &input_stream);

   /*!
    * Pop all items from an input stream and pass them to the client one-at-a-time
    */
   void popRecords(google::protobuf::io::CodedInputStream &input_stream);

   // Member variables

   const uint32_t             m_max_msglen;      //!< Maximum allowed length of a protobuf on the wire
   std::unique_ptr<uint8_t[]> m_split_buffer;    //!< Holding buffer for partial messages split across two input buffers
   int                        m_split_buflen;    //!< Length of data stored in m_split_buffer
};



template<typename DataType>
void IStreamBuffer<DataType>::push(const char *buf_ptr, int buf_len)
{
   google::protobuf::io::CodedInputStream input_stream(reinterpret_cast<const uint8_t*>(buf_ptr), buf_len);

   if(m_split_buflen > 0) {
      // Stitch together the partial record

      if(m_split_buflen < sizeof(uint32_t)) {
         // The size field is split across the boundary, only need to copy that field

         int bytes_to_copy = sizeof(uint32_t) - m_split_buflen;
         memcpy(m_split_buffer.get() + m_split_buflen, buf_ptr, bytes_to_copy);

         uint32_t msg_len;
         google::protobuf::io::CodedInputStream::ReadLittleEndian32FromArray(m_split_buffer.get(), &msg_len);
         input_stream.Skip(bytes_to_copy);
         popRecord(msg_len, input_stream);
      } else {
         // The payload is split across the boundary, copy the rest of the record

         uint32_t msg_len;
         google::protobuf::io::CodedInputStream::ReadLittleEndian32FromArray(m_split_buffer.get(), &msg_len);
         if(msg_len > m_max_msglen) {
            throw XrdSsiException("Data record size exceeds XRootD SSI buffer size");
         }
         int bytes_to_copy = msg_len - m_split_buflen - sizeof(uint32_t);
         memcpy(m_split_buffer.get() + m_split_buflen, buf_ptr, bytes_to_copy);
         input_stream.Skip(bytes_to_copy);

         google::protobuf::io::CodedInputStream split_stream(reinterpret_cast<const uint8_t*>(m_split_buffer.get() + sizeof(uint32_t)), msg_len);
         popRecord(msg_len, split_stream);
      }
   }

   // Extract records from the input buffer
   popRecords(input_stream);

   // Copy any leftover partial record to the holding buffer
   input_stream.GetDirectBufferPointer(reinterpret_cast<const void**>(&buf_ptr), &m_split_buflen);
   memcpy(m_split_buffer.get(), buf_ptr, m_split_buflen);
}



template<typename DataType>
void IStreamBuffer<DataType>::popRecord(int msg_len, google::protobuf::io::CodedInputStream &input_stream)
{
   const char *buf_ptr;
   int buf_len;

   if(msg_len > m_max_msglen) {
      throw XrdSsiException("Data record size exceeds XRootD SSI buffer size");
   }

   input_stream.GetDirectBufferPointer(reinterpret_cast<const void**>(&buf_ptr), &buf_len);
std::cout << "[POP_RECORD] buf_len = " << buf_len << std::endl;

   if(buf_len < msg_len) {
      // Record payload is split across the boundary, save the partial record
      m_split_buflen = msg_len;
      memcpy(m_split_buffer.get(), buf_ptr, m_split_buflen);
   } else {
      DataType record;

      record.ParseFromArray(buf_ptr, msg_len);
   }
   input_stream.Skip(msg_len);
}



template<typename DataType>
void IStreamBuffer<DataType>::popRecords(google::protobuf::io::CodedInputStream &input_stream)
{
   uint32_t msg_len;
   int buf_len;

   do {
      const char *buf_ptr;

      // Get size of next item on the stream

      input_stream.GetDirectBufferPointer(reinterpret_cast<const void**>(&buf_ptr), &buf_len);
std::cout << "buf_len = " << buf_len << std::endl;

      if(buf_len < static_cast<int>(sizeof(uint32_t))) {
         // Size field is split across the boundary, save the partial field and finish
         m_split_buflen = buf_len;
         memcpy(m_split_buffer.get(), buf_ptr, m_split_buflen);
         input_stream.Skip(m_split_buflen);
         break;
      }

      input_stream.ReadLittleEndian32(&msg_len);
std::cout << "Bytesize = " << msg_len << std::endl;

      // Get next item on the stream

      popRecord(msg_len, input_stream);

   } while(buf_len != static_cast<int>(msg_len));
}



#if 0
template<typename DataType>
void IStreamBuffer<DataType>::popSplitItem(google::protobuf::io::CodedInputStream *first_stream, google::protobuf::io::CodedInputStream *second_stream)
{
   DataType record;

   uint32_t msg_len;
   int buf_len1, buf_len2;
   const char *buf_ptr1, *buf_ptr2;

   bool one = first_stream->GetDirectBufferPointer(reinterpret_cast<const void**>(&buf_ptr1), &buf_len1);
   bool two = second_stream->GetDirectBufferPointer(reinterpret_cast<const void**>(&buf_ptr2), &buf_len2);

   // Get size of next item on the stream
   if(buf_len1 < static_cast<int>(sizeof(uint32_t))) {
   } else {
      // Case 2: message length is in the first buffer, payload is split over two buffers
      first_stream->ReadLittleEndian32(&msg_len);
   }
}
#endif



#if 0
/*!
 * Data callback.
 *
 * Defines how Data/Stream messages should be handled
 */
template<>
void XrdSsiPbRequestType::DataCallback(XrdSsiRequest::PRD_Xeq &post_process, char *response_bufptr, int response_buflen)
{
   IStreamBuffer istream;

   istream.push(response_bufptr, response_buflen);
   google::protobuf::io::CodedInputStream coded_stream(reinterpret_cast<const uint8_t*>(response_bufptr), response_buflen);


      //OutputJsonString(std::cout, &line_item);

      std::cout << std::setfill(' ') << std::setw(7)  << std::right << line_item.af().archive_file_id() << ' '
                << std::setfill(' ') << std::setw(7)  << std::right << line_item.copy_nb()              << ' '
                << std::setfill(' ') << std::setw(7)  << std::right << line_item.tf().vid()             << ' '
                << std::setfill(' ') << std::setw(7)  << std::right << line_item.tf().f_seq()           << ' '
                << std::setfill(' ') << std::setw(8)  << std::right << line_item.tf().block_id()        << ' '
                << std::setfill(' ') << std::setw(8)  << std::right << line_item.af().disk_instance()   << ' '
                << std::setfill(' ') << std::setw(7)  << std::right << line_item.af().disk_file_id()    << ' '
                << std::setfill(' ') << std::setw(12) << std::right << line_item.af().file_size()       << ' '
                << std::setfill(' ') << std::setw(13) << std::right << line_item.af().cs().type()       << ' '
                << std::setfill(' ') << std::setw(14) << std::right << line_item.af().cs().value()      << ' '
                << std::setfill(' ') << std::setw(13) << std::right << line_item.af().storage_class()   << ' '
                << std::setfill(' ') << std::setw(8)  << std::right << line_item.af().df().owner()      << ' '
                << std::setfill(' ') << std::setw(8)  << std::right << line_item.af().df().group()      << ' '
                << std::setfill(' ') << std::setw(13) << std::right << line_item.af().creation_time()   << ' '
                << line_item.af().df().path() << std::endl;
}
#endif

} // namespace XrdSsiPb

