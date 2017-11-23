/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend Archive File Ls stream implementation
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
#include "catalogue/Catalogue.hpp"


namespace XrdSsiPb {

/*!
 * Memory buffer struct
 *
 * Implements a streambuf as a fixed-size char array so that we can provide a char* to XRootD
 */
struct Membuf: public std::streambuf
{
   Membuf(char *buffer, size_t size) {
      // Set the boundaries of the buffer
      this->setp(buffer, buffer + size);
   }

   //! Return the number of bytes in the buffer
   size_t length() const {
      return pptr()-pbase();
   }

   int overflow(int c) override {
      std::cerr << "OVERFLOW " << c << std::endl;
      return 0;
   }
};



/*!
 * Stream interface to the memory buffer
 */
struct omemstream: virtual Membuf, std::ostream
{
   omemstream(char *buffer, size_t size) :
      Membuf(buffer, size),
      std::ostream(this) {}
};



/*!
 * Stream Buffer.
 *
 * This class binds XrdSsiStream::Buffer to the stream interface.
 *
 * This is a naïve implementation, where buffers are allocated and deallocated each time they are
 * used. A more performant implementation could be implemented using a buffer pool. See the Recycle()
 * method below.
 */
class StreamBuffer : public XrdSsiStream::Buffer
{
public:
   StreamBuffer(size_t size) : XrdSsiStream::Buffer(new char[size]),
      m_out(data, size) {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] StreamBuffer() constructor" << std::endl;
#endif
   }

   ~StreamBuffer() {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] StreamBuffer() destructor" << std::endl;
#endif
      // data is a public member of XrdSsiStream::Buffer. It is an unmanaged char* pointer.
      delete[] data;
   }

   //! Return size of the stream buffer in bytes
   int length() const { return m_out.length(); }

   //! Overload << to allow streaming into the buffer
   template<typename T>
   omemstream &operator<<(const T &t) {
      m_out << t;
      return m_out;
   }

private:
   //! Called by the XrdSsi framework when it is finished with the object
   virtual void Recycle() {
      std::cerr << "[DEBUG] StreamBuffer::Recycle()" << std::endl;
      delete this;
   }

   // Member variables

   omemstream m_out;    //!< Ostream interface to the buffer
};

} // namespace XrdSsiPb



namespace cta { namespace xrd {

/*!
 * Stream object which implements "af ls" command.
 */
class ArchiveFileLsStream : public XrdSsiStream
{
public:
   ArchiveFileLsStream(cta::catalogue::ArchiveFileItor archiveFileItor) :
      XrdSsiStream(XrdSsiStream::isActive)
   {
      std::cerr << "[DEBUG] ArchiveFileLsStream() constructor" << std::endl;
tmp_num_items = 0;
   }

   virtual ~ArchiveFileLsStream() {
      std::cerr << "[DEBUG] ArchiveFileLsStream() destructor" << std::endl;
   }

   /*!
    * Synchronously obtain data from an active stream (server-side only).
    *
    * @param[out]       eInfo   The object to receive any error description.
    * @param[in,out]    dlen    input:  the optimal amount of data wanted (this is a hint)
    *                           output: the actual amount of data returned in the buffer.
    * @param[in,out]    last    input:  should be set to false.
    *                           output: if true it indicates that no more data remains to be returned
    *                                   either for this call or on the next call.
    *
    * @return    Pointer to the Buffer object that contains a pointer to the the data (see below). The
    *            buffer must be returned to the stream using Buffer::Recycle(). The next member is usable.
    * @retval    0    No more data remains or an error occurred:
    *                 last = true:  No more data remains.
    *                 last = false: A fatal error occurred, eRef has the reason.
    */
   virtual Buffer *GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) {
#ifdef XRDSSI_DEBUG
      std::cerr << "[DEBUG] ArchiveFileLsStream::GetBuff(): XrdSsi buffer fill request (" << dlen << " bytes)" << std::endl;
#endif
      if(tmp_num_items > 9)
      {
         // Nothing more to send, close the stream
         last = true;
         return nullptr;
      }

      // Get the next item and pass it back to the caller
      cta::admin::ArchiveFileLsItem item;
      item.mutable_af()->set_disk_instance("Hello");
      item.mutable_af()->set_disk_file_id("World");
      item.set_copy_nb(++tmp_num_items);

      std::string bufstr;
      item.SerializeToString(&bufstr);

      XrdSsiPb::StreamBuffer *buffer = new XrdSsiPb::StreamBuffer(dlen);
      *buffer << bufstr;
      dlen = buffer->length();

std::cerr << "Returning buffer with " << dlen << " bytes of data." << std::endl;

      return buffer;
   }

private:
   int tmp_num_items;
};

#if 0
ArchiveFileLsStream(m_catalogue.getArchiveFiles(searchCriteria), has_flag(OptionBoolean::SHOW_HEADER));

XrdOucErrInfo xrdSfsFileError;

m_listArchiveFilesCmd.reset(new xrootPlugins::ListArchiveFilesCmd(xrdSfsFileError, has_flag(OptionBoolean::SHOW_HEADER), std::move(archiveFileItor)));
#endif

}} // namespace cta::xrd

