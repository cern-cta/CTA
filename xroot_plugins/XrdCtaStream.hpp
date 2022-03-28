/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <XrdSsiPbOStreamBuffer.hpp>
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>



namespace cta { namespace xrd {

/*!
 * Virtual stream object
 */
class XrdCtaStream : public XrdSsiStream
{
public:
  XrdCtaStream(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) :
    XrdSsiStream(XrdSsiStream::isActive),
    m_catalogue(catalogue),
    m_scheduler(scheduler)
  {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "XrdCtaStream() constructor");
  }

  virtual ~XrdCtaStream() {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "~XrdCtaStream() destructor");
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

    std::unique_ptr<XrdSsiPb::OStreamBuffer<Data>> streambuf;

    try {
      if(isDone()) {
        // Nothing more to send, close the stream
        last = true;
        return nullptr;
      }

      streambuf = std::make_unique<XrdSsiPb::OStreamBuffer<Data>>(dlen);

      dlen = fillBuffer(streambuf.get());

      XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "GetBuff(): Returning buffer with ", dlen, " bytes of data.");
    } catch(cta::exception::Exception &ex) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: Caught CTA exception: " << ex.what();
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
      return nullptr;
    } catch(std::exception &ex) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: " << ex.what();
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
      return nullptr;
    } catch(...) {
      std::ostringstream errMsg;
      errMsg << __FUNCTION__ << " failed: Caught an unknown exception";
      eInfo.Set(errMsg.str().c_str(), ECANCELED);
      return nullptr;
    }
    return streambuf.release();
  }

private:
  /*!
   * Returns true if there is nothing more to send (i.e. we can close the stream)
   */
  virtual bool isDone() const = 0;

  /*!
   * Fills the stream buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) = 0;

protected:
  cta::catalogue::Catalogue &m_catalogue;    //!< Reference to CTA Catalogue
  cta::Scheduler            &m_scheduler;    //!< Reference to CTA Scheduler

private:
  static constexpr const char* const LOG_SUFFIX  = "XrdCtaStream";    //!< Identifier for log messages
};

}} // namespace cta::xrd
