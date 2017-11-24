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

#include <arpa/inet.h>
#include <XrdSsi/XrdSsiStream.hh>

namespace XrdSsiPb {

/*!
 * Stream Buffer.
 *
 * This class binds XrdSsiStream::Buffer to the stream interface.
 *
 * This is a naïve implementation, where memory is allocated and deallocated on every use.
 *
 * A more performant implementation could be implemented using a buffer pool, using the Recycle()
 * method to return buffers to the pool.
 */
class OStreamBuffer : public XrdSsiStream::Buffer
{
public:
   OStreamBuffer() : XrdSsiStream::Buffer(nullptr) {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] StreamBuffer() constructor" << std::endl;
#endif
   }

   ~OStreamBuffer() {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] StreamBuffer() destructor" << std::endl;
#endif
      // data is a public member of XrdSsiStream::Buffer. It is an unmanaged char* pointer.
      delete[] data;
   }

   //! Serialize a Protocol Buffer into the Stream Buffer and return its size
   template<typename ProtoBuf>
   int serialize(const ProtoBuf &pb) {
      // Drop any existing data. A more sophisticated implementation could chain new objects onto the buffer,
      // using XrdSsiStream::Buffer::Next
      delete[] data;

      uint32_t bytesize = pb.ByteSize();
      data = new char[bytesize + sizeof(uint32_t)];

      // Write the size into the buffer in network order
      *reinterpret_cast<uint32_t*>(data) = htonl(bytesize);

      // Serialize the Protocol Buffer
      pb.SerializeToArray(data + sizeof(uint32_t), bytesize);

      // Return the total size of the buffer including bytesize
      return bytesize + sizeof(uint32_t);
   }

private:
   //! Called by the XrdSsi framework when it is finished with the object
   virtual void Recycle() {
      std::cerr << "[DEBUG] StreamBuffer::Recycle()" << std::endl;
      delete this;
   }
};

} // namespace XrdSsiPb

