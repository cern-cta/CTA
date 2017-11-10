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

namespace cta { namespace xrd {

/*!
 * Memory buffer struct
 *
 * Implements a streambuf as a fixed-size char array so that we can provide a char* to XRootD
 */
struct Membuf: public std::streambuf
{
   Membuf(char *buffer, size_t size) {
      // Set the boundaries of the buffer
      this->setp(buffer, buffer + size - 1);
   }

   size_t length() {
      return pptr()-pbase();
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
 * Buffer object.
 *
 * This is a very naïve implementation, where buffers are allocated and deallocated each time they are
 * used. A more performant implementation would use buffer pools.
 */
class ArchiveFileLsBuffer : public XrdSsiStream::Buffer
{
public:
   ArchiveFileLsBuffer(size_t size) : XrdSsiStream::Buffer(new char[size]),
      out(data, size)
   {
#ifdef XRDSSI_DEBUG
      std::cerr << "ArchiveFileLsBuffer() constructor" << std::endl;
#endif

      out << "HELLO, WORLD. ";
   }

   ~ArchiveFileLsBuffer() {
#ifdef XRDSSI_DEBUG
      std::cerr << "ArchiveFileLsBuffer() destructor" << std::endl;
#endif
      delete[] data;
   }

   int length() { return out.length(); }

#if 0
      if(m_displayHeader) {
               m_readBuffer <<
                          "\x1b[31;1m" << // Change the colour of the output text to red
                                  std::setfill(' ') << std::setw(7) << std::right << "id" << " " <<
                                          std::setfill(' ') << std::setw(7) << std::right << "copy no" << " " <<
                                                  std::setfill(' ') << std::setw(7) << std::right << "vid" << " " <<
                                                          std::setfill(' ') << std::setw(7) << std::right << "fseq" << " " <<
                                                                  std::setfill(' ') << std::setw(8) << std::right << "block id" << " " <<
                                                                          std::setfill(' ') << std::setw(8) << std::right << "instance" << " " <<
                                                                                  std::setfill(' ') << std::setw(7) << std::right << "disk id" << " " <<
                                                                                          std::setfill(' ') << std::setw(12) << std::right << "size" << " " <<
                                                                                                  std::setfill(' ') << std::setw(13) << std::right << "checksum type" << " " <<
                                                                                                          std::setfill(' ') << std::setw(14) << std::right << "checksum value" << " " <<
                                                                                                                  std::setfill(' ') << std::setw(13) << std::right << "storage class" << " " <<
                                                                                                                          std::setfill(' ') << std::setw(8) << std::right << "owner" << " " <<
                                                                                                                                  std::setfill(' ') << std::setw(8) << std::right << "group" << " " <<
                                                                                                                                          std::setfill(' ') << std::setw(13) << std::right << "creation time" << " " <<
                                                                                                                                                  "path" <<
                                                                                                                                                          "\x1b[0m\n"; // Return the colour of the output text
#endif


private:
   virtual void Recycle() {
      std::cerr << "ArchiveFileLsBuffer::Recycle()" << std::endl;
      delete this;
   }

   // Member variables

   omemstream out;    //!< Ostream interface to the buffer
};



/*!
 * Stream object which implements "af ls" command.
 */
class ArchiveFileLsStream : public XrdSsiStream
{
public:
   ArchiveFileLsStream(cta::catalogue::ArchiveFileItor archiveFileItor, bool show_header) : XrdSsiStream(XrdSsiStream::isActive) {
      std::cerr << "[DEBUG] ArchiveFileLsStream() constructor" << std::endl;
#if 0
ArchiveFileLsStream(m_catalogue.getArchiveFiles(searchCriteria), has_flag(OptionBoolean::SHOW_HEADER));

XrdOucErrInfo xrdSfsFileError;

m_listArchiveFilesCmd.reset(new xrootPlugins::ListArchiveFilesCmd(xrdSfsFileError, has_flag(OptionBoolean::SHOW_HEADER), std::move(archiveFileItor)));
#endif
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
std::cerr << "Called ArchiveFileLsStream::GetBuff with dlen=" << dlen << ", last=" << last << std::endl;
      ArchiveFileLsBuffer *buffer = new ArchiveFileLsBuffer(dlen);
      dlen = buffer->length();
std::cerr << "Got " << dlen << " bytes." << std::endl;
      last = true;

std::cerr << "Returning buffer" << std::endl;
      return buffer;
   }
};

}} // namespace cta::xrd
