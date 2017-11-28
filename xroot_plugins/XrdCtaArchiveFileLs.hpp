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

#include "xroot_ssi_pb/XrdSsiPbOStreamBuffer.hpp"
#include "catalogue/Catalogue.hpp"



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
      if(tmp_num_items > 30)
      {
         // Nothing more to send, close the stream
         last = true;
         return nullptr;
      }

      // Get the next item and pass it back to the caller
      cta::xrd::Data record;
      record.mutable_af_ls_item()->mutable_af()->set_disk_instance("Hello");
      record.mutable_af_ls_item()->mutable_af()->set_disk_file_id("World");
      record.mutable_af_ls_item()->set_copy_nb(++tmp_num_items);

      XrdSsiPb::OStreamBuffer *streambuf = new XrdSsiPb::OStreamBuffer();
      dlen = streambuf->serialize(record);

std::cerr << "Returning buffer with " << dlen << " bytes of data." << std::endl;

      return streambuf;
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

