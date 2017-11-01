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

/*!
 * The Buffer object is returned by active streams as they supply the buffer holding the requested
 * data. Once the buffer is no longer needed it must be recycled by calling Recycle().
 */
class ArchiveFileBuffer : public XrdSsiStream::Buffer
{
public:
   ArchiveFileBuffer(char *dp = nullptr) : Buffer(dp) {}

   virtual void Recycle() {}
};
#if 0
virtual void    Recycle() = 0;     //!> Call to recycle the buffer when finished

char           *data;              //!> -> Buffer containing the data
Buffer         *next;              //!> For chaining by buffer receiver

                Buffer(char *dp=0) : data(dp), next(0) {}
virtual        ~Buffer() {}
#endif



class ArchiveFileLsStream : public XrdSsiStream
{
public:
   ArchiveFileLsStream() :
      XrdSsiStream(XrdSsiStream::isActive),
      m_archiveFileLsBuffer(test_buf) {}

   virtual ~ArchiveFileLsStream() {}

   /*!
    * Synchronously obtain data from an active stream (server-side only).
    *
    * @param[out]       eRef    The object to receive any error description.
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
      dlen = strlen(m_archiveFileLsBuffer.data);
      last = false;

      return &m_archiveFileLsBuffer;
   }

private:
   char* test_buf = const_cast<char*>("Hello, world!\n");

   ArchiveFileBuffer m_archiveFileLsBuffer;
};

