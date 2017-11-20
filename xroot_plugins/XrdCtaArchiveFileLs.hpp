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

//#include <iostream>
//#include <algorithm>
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
   ArchiveFileLsStream(cta::catalogue::ArchiveFileItor archiveFileItor, bool show_header) :
      XrdSsiStream(XrdSsiStream::isActive),
      m_show_header(show_header) {
      std::cerr << "[DEBUG] ArchiveFileLsStream() constructor" << std::endl;
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
      XrdSsiPb::StreamBuffer *buffer = new XrdSsiPb::StreamBuffer(dlen);

      // Write the header on first call only
      if(m_show_header) {
         WriteHeader(buffer);
         m_show_header = false;
      }

      *buffer << "HELLO," << " WORLD! " << std::endl;
      last = false;

      dlen = buffer->length();
std::cerr << "Returning buffer with " << dlen << " bytes of data." << std::endl;

      return buffer;
   }

private:
   /*!
    * Write the af ls header into the buffer
    */
   void WriteHeader(XrdSsiPb::StreamBuffer *buffer) {
      const char* const TEXT_RED    = "\x1b[31;1m";
      const char* const TEXT_NORMAL = "\x1b[0m\n";

      *buffer << TEXT_RED <<
         std::setfill(' ') << std::setw(7)  << std::right << "id"             << ' ' <<
         std::setfill(' ') << std::setw(7)  << std::right << "copy no"        << ' ' <<
         std::setfill(' ') << std::setw(7)  << std::right << "vid"            << ' ' <<
         std::setfill(' ') << std::setw(7)  << std::right << "fseq"           << ' ' <<
         std::setfill(' ') << std::setw(8)  << std::right << "block id"       << ' ' <<
         std::setfill(' ') << std::setw(8)  << std::right << "instance"       << ' ' <<
         std::setfill(' ') << std::setw(7)  << std::right << "disk id"        << ' ' <<
         std::setfill(' ') << std::setw(12) << std::right << "size"           << ' ' <<
         std::setfill(' ') << std::setw(13) << std::right << "checksum type"  << ' ' <<
         std::setfill(' ') << std::setw(14) << std::right << "checksum value" << ' ' <<
         std::setfill(' ') << std::setw(13) << std::right << "storage class"  << ' ' <<
         std::setfill(' ') << std::setw(8)  << std::right << "owner"          << ' ' <<
         std::setfill(' ') << std::setw(8)  << std::right << "group"          << ' ' <<
         std::setfill(' ') << std::setw(13) << std::right << "creation time"  << ' ' <<
                                                             "path"           <<
         TEXT_NORMAL << std::endl;
   }

   // Member variables

   bool m_show_header;
};

#if 0
ArchiveFileLsStream(m_catalogue.getArchiveFiles(searchCriteria), has_flag(OptionBoolean::SHOW_HEADER));

XrdOucErrInfo xrdSfsFileError;

m_listArchiveFilesCmd.reset(new xrootPlugins::ListArchiveFilesCmd(xrdSfsFileError, has_flag(OptionBoolean::SHOW_HEADER), std::move(archiveFileItor)));
#endif
#if 0
   {
      m_readBuffer <<
        std::setfill(' ') << std::setw(7) << std::right << archiveFile.archiveFileID << " " <<
        std::setfill(' ') << std::setw(7) << std::right << copyNb << " " <<
        std::setfill(' ') << std::setw(7) << std::right << tapeFile.vid << " " <<
        std::setfill(' ') << std::setw(7) << std::right << tapeFile.fSeq << " " <<
        std::setfill(' ') << std::setw(8) << std::right << tapeFile.blockId << " " <<
        std::setfill(' ') << std::setw(8) << std::right << archiveFile.diskInstance << " " <<
        std::setfill(' ') << std::setw(7) << std::right << archiveFile.diskFileId << " " <<
        std::setfill(' ') << std::setw(12) << std::right << archiveFile.fileSize << " " <<
        std::setfill(' ') << std::setw(13) << std::right << archiveFile.checksumType << " " <<
        std::setfill(' ') << std::setw(14) << std::right << archiveFile.checksumValue << " " <<
        std::setfill(' ') << std::setw(13) << std::right << archiveFile.storageClass << " " <<
        std::setfill(' ') << std::setw(8) << std::right << archiveFile.diskFileInfo.owner << " " <<
        std::setfill(' ') << std::setw(8) << std::right << archiveFile.diskFileInfo.group << " " <<
        std::setfill(' ') << std::setw(13) << std::right << archiveFile.creationTime << " " <<
        archiveFile.diskFileInfo.path << "\n";
   }
#endif

}} // namespace cta::xrd
