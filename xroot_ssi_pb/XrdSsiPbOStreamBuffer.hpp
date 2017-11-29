/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Output stream to send a stream of protocol buffers from server to client
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

#include <XrdSsi/XrdSsiStream.hh>

namespace XrdSsiPb {

/*!
 * Output Stream Buffer class.
 *
 * This class binds XrdSsiStream::Buffer to the stream interface.
 *
 * This is a naïve implementation, where memory is allocated and deallocated on every use. It favours
 * computation efficiency over memory efficiency: the buffer allocated is twice the hint size, but data
 * copying is minimized.
 *
 * A more performant implementation could be implemented using a buffer pool, using the Recycle()
 * method to return buffers to the pool.
 */
template<typename DataType>
class OStreamBuffer : public XrdSsiStream::Buffer
{
public:
   /*!
    * Constructor
    *
    * data is a public member of XrdSsiStream::Buffer. It is an unmanaged char* pointer. We initialize
    * it to double the hint size, with the implicit rule that the size of an individual serialized
    * record on the wire cannot exceed the hint size.
    */
   OStreamBuffer(uint32_t hint_size) : XrdSsiStream::Buffer(new char[hint_size * 2]),
      m_hint_size(hint_size),
      m_data_ptr(data),
      m_data_size(0) {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] OStreamBuffer() constructor" << std::endl;
#endif
   }

   ~OStreamBuffer() {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] OStreamBuffer() destructor" << std::endl;
#endif
      delete[] data;
   }

   /*!
    * Get the data size
    */
   uint32_t Size() const {
      return m_data_size;
   }

   /*!
    * Push a protobuf into the queue
    *
    * @retval    true     The buffer has been filled and is ready for sending
    * @retval    false    There is room in the buffer for more records
    */
   bool Push(const DataType &record) {
      uint32_t bytesize = record.ByteSize();

      if(m_data_size + bytesize > m_hint_size * 2) {
         throw XrdSsiException("OStreamBuffer::Push(): Stream buffer overflow");
      }

      // Write the size of the next record into the buffer
      google::protobuf::io::CodedOutputStream::WriteLittleEndian32ToArray(bytesize, reinterpret_cast<google::protobuf::uint8*>(m_data_ptr));
      m_data_ptr += sizeof(uint32_t);

      // Serialize the Protocol Buffer
      record.SerializeToArray(m_data_ptr, bytesize);
      m_data_ptr += bytesize;

      m_data_size += sizeof(uint32_t) + bytesize;

      return m_data_size >= m_hint_size;
   }

private:
   /*!
    * Called by the XrdSsi framework when it is finished with the object
    */
   virtual void Recycle() {
      std::cerr << "[DEBUG] OStreamBuffer::Recycle()" << std::endl;
      delete this;
   }

   // Member variables

   const uint32_t                              m_hint_size;     //!< Requested size of the buffer from the XRootD framework
   char                                       *m_data_ptr;      //!< Pointer to the raw storage
   uint32_t                                    m_data_size;     //!< Total size of the buffer on the wire
   std::vector<std::pair<uint32_t, DataType>>  m_protobuf_q;    //!< Queue of protobufs to be serialized
};

} // namespace XrdSsiPb

