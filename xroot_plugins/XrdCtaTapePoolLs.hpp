/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Frontend Tape Pool Ls stream implementation
 * @copyright      Copyright 2018 CERN
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

#include <XrdSsiPbOStreamBuffer.hpp>
#include <catalogue/Catalogue.hpp>



namespace cta { namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command.
 */
class TapePoolLsStream : public XrdSsiStream
{
public:
  TapePoolLsStream(cta::catalogue::Catalogue &catalogue) :
    XrdSsiStream(XrdSsiStream::isActive),
    m_tapePoolList(catalogue.getTapePools())
  {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "TapePoolLsStream() constructor");
  }

  virtual ~TapePoolLsStream() {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "~TapePoolLsStream() destructor");
  }

  /*!
   * Synchronously obtain data from an active stream
   *
   * Active streams can only exist on the server-side. This XRootD SSI Stream class is marked as an
   * active stream in the constructor.
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
  virtual Buffer *GetBuff(XrdSsiErrInfo &eInfo, int &dlen, bool &last) override {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): XrdSsi buffer fill request (", dlen, " bytes)");

    XrdSsiPb::OStreamBuffer<Data> *streambuf;

    try {
      if(m_tapePoolList.empty()) {
        // Nothing more to send, close the stream
        last = true;
        return nullptr;
      }

      streambuf = new XrdSsiPb::OStreamBuffer<Data>(dlen);

      for(bool is_buffer_full = false; !m_tapePoolList.empty() && !is_buffer_full; m_tapePoolList.pop_front())
      {
        Data record;

        // TapePool
        auto &tp      = m_tapePoolList.front();
        auto  tp_item = record.mutable_tpls_item();

        tp_item->set_name(tp.name);
        tp_item->set_vo(tp.vo);
        tp_item->set_num_tapes(tp.nbTapes);
        tp_item->set_num_partial_tapes(tp.nbPartialTapes);
        tp_item->set_num_physical_files(tp.nbPhysicalFiles);
        tp_item->set_capacity_bytes(tp.capacityBytes);
        tp_item->set_data_bytes(tp.dataBytes);
        tp_item->set_encrypt(tp.encryption);
        tp_item->set_supply(tp.supply ? tp.supply.value() : "");
        tp_item->mutable_created()->set_username(tp.creationLog.username);
        tp_item->mutable_created()->set_host(tp.creationLog.host);
        tp_item->mutable_created()->set_time(tp.creationLog.time);
        tp_item->mutable_modified()->set_username(tp.lastModificationLog.username);
        tp_item->mutable_modified()->set_host(tp.lastModificationLog.host);
        tp_item->mutable_modified()->set_time(tp.lastModificationLog.time);
        tp_item->set_comment(tp.comment);

        // is_buffer_full is set to true when we have one full block of data in the buffer, i.e.
        // enough data to send to the client. The actual buffer size is double the block size,
        // so we can keep writing a few additional records after is_buffer_full is true. These
        // will be sent on the next iteration. If we exceed the hard limit of double the block
        // size, Push() will throw an exception.
        is_buffer_full = streambuf->Push(record);
      }

      dlen = streambuf->Size();
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): Returning buffer with ", dlen, " bytes of data.");
    } catch(cta::exception::Exception &ex) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: Caught CTA exception: " << ex.what();
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
      delete streambuf;
    } catch(std::exception &ex) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: " << ex.what();
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
      delete streambuf;
    } catch(...) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: Caught an unknown exception";
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
      delete streambuf;
    }
    return streambuf;
  }

private:
  std::list<cta::catalogue::TapePool> m_tapePoolList;                     //!< List of tape pools from the catalogue

  static constexpr const char* const LOG_SUFFIX  = "TapePoolLsStream";    //!< Identifier for log messages
};

}} // namespace cta::xrd
