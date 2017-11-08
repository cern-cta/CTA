/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend stream handler
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

class ArchiveFileLsBuffer : public XrdSsiStream::Buffer
{
public:
   ArchiveFileLsBuffer() {
      std::cerr << "ArchiveFileLsBuffer() constructor" << std::endl;
      data = test;
   }
   ~ArchiveFileLsBuffer() {
      std::cerr << "ArchiveFileLsBuffer() destructor" << std::endl;
   }

   int length() { return 14; }

private:
   virtual void Recycle() {
      std::cerr << "ArchiveFileLsBuffer::Recycle()" << std::endl;
      delete this;
   }

   char *test = const_cast<char*>("HELLO, WORLD. ");
};



class ArchiveFileLsStream : public XrdSsiStream
{
public:
   ArchiveFileLsStream() : XrdSsiStream(XrdSsiStream::isActive) {
      test_count = 100;
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
std::cerr << "Called ArchiveFileLsStream::GetBuff with dlen=" << dlen << ", last=" << last << std::endl;
      ArchiveFileLsBuffer *buffer = new ArchiveFileLsBuffer();
std::cerr << "Calling ArchiveFileLsBuffer::length" << std::endl;
      dlen = buffer->length();
      --test_count;
      last = (test_count == 0);

std::cerr << "Returning buffer" << std::endl;
      return buffer;
   }

   int test_count;
};

