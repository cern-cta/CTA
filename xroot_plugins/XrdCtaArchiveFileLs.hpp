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

class ArchiveFileLsStream : public XrdSsiStream
{
public:
            ArchiveFileLsStream() :
               XrdSsiStream(XrdSsiStream::isActive),
               m_archiveFileLsBuffer(test_buf)
            {}
   virtual ~ArchiveFileLsStream() {}

   Buffer *GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) {

      dlen = strlen(test_buff);
      last = false;

      return m_archiveFileLsBuffer;
   }

private:
   const char* const test_buf = "Hello, world!\n";

   Buffer m_archiveFileLsBuffer;
};

