/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          XRootD SSI Responder class template
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

#ifndef __XRD_SSI_PB_REQUEST_PROC_H
#define __XRD_SSI_PB_REQUEST_PROC_H

#include <future>

#include <XrdSsi/XrdSsiResponder.hh>
#include "XrdSsiPbException.hpp"
#include "XrdSsiPbAlert.hpp"

namespace XrdSsiPb {

/*!
 * Exception Handler class.
 *
 * This is used to send framework exceptions back to the client. The client should specialize on this
 * class for the Response class.
 */

template<typename MetadataType, typename ExceptionType>
class ExceptionHandler
{
public:
   void operator()(MetadataType &response, const ExceptionType &ex);
};



/*!
 * Request Processing class.
 *
 * This is an agent object that the Service object creates for each Request that it receives. The Request
 * object will be bound to the XrdSsiResponder object via a call to XrdSsiResponder::BindRequest(). Once
 * the relationship is established, the XrdSsi framework keeps track of the Request object and manages
 * its lifetime.
 *
 * The XrdSsiResponder class contains the methods needed to interact with the Request object: get the
 * Request, release storage, send Alerts, and post a Response. It also knows how to safely interact with
 * the Request object, handling asynchronous requests such as cancellation, broken TCP connections, etc. 
 */

template <typename RequestType, typename MetadataType, typename AlertType>
class RequestProc : public XrdSsiResponder
{
public:
            RequestProc() {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] RequestProc() constructor" << std::endl;
#endif
   }
   virtual ~RequestProc() {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] ~RequestProc() destructor" << std::endl;
#endif
   }

           void Execute();
   virtual void Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel=false) override;

private:
   /*!
    * Encapsulate the Alert protocol buffer inside a XrdSsiRespInfoMsg object.
    *
    * Alert message objects are created on the heap with lifetime managed by the XrdSsiResponder class.
    */

   void Alert(const AlertType &alert)
   {
      XrdSsiResponder::Alert(*(new AlertMsg<AlertType>(alert)));
   }

   /*!
    * Handle bad protocol buffer Requests.
    *
    * This class should store the exception in the Response Protocol Buffer, which the framework will
    * send back to the client. The client needs to define the specialized version of this class.
    */

   ExceptionHandler<MetadataType, PbException> Throw;

   /*!
    * Execute action after deserialization of the Request Protocol Buffer.
    *
    * The client needs to define the specialized version of this method.
    */

   void ExecuteAction() {
#ifdef XRDSSI_DEBUG
      std::cout << "[DEBUG] Called default RequestProc::ExecuteAction()" << std::endl;
#endif
   }

   /*
    * Protocol Buffer members
    *
    * A metadata-only reply is appropriate when we just need to send a short response/acknowledgement,
    * as it has less overhead than a full response.
    *
    * Note that Metadata and Response buffers need to be member variables as they must stay in scope
    * after calling RequestProc() (until Finished() is called).
    *
    * The maximum amount of metadata that may be sent is defined by XrdSsiResponder::MaxMetaDataSZ
    * constant member.
    */

   RequestType  m_request;         //!< Request object
   MetadataType m_metadata;        //!< Metadata Response object


   std::string  m_metadata_str;    //!< Serialized Metadata Response buffer
   std::string  m_response_str;    //!< Response buffer

   std::promise<void> m_promise;   //!< Promise that the Request has been processed
};



template <typename RequestType, typename MetadataType, typename AlertType>
void RequestProc<RequestType, MetadataType, AlertType>::Execute()
{
   const int ExecuteTimeout = 15;    //< Maximum no. of seconds to wait before deleting myself

#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] RequestProc::Execute()" << std::endl;
#endif

   // Deserialize the Request

   int request_len;
   const char *request_buffer = GetRequest(request_len);

   if(m_request.ParseFromArray(request_buffer, request_len))
   {
      // Pass control from the framework to the application

      ExecuteAction();
   }
   else
   {
      // Pass an exception back to the client and continue processing

      Throw(m_metadata, PbException("m_request.ParseFromArray() failed"));
   }

   // Release the request buffer

   ReleaseRequestBuffer();

   // Serialize the Metadata

   if(!m_metadata.SerializeToString(&m_metadata_str))
   {
      throw PbException("m_metadata.SerializeToString() failed");
   }

   // Send the Metadata

   if(m_metadata_str.size() > 0)
   {
      SetMetadata(m_metadata_str.c_str(), m_metadata_str.size());
   }

   // Send the response. This must always be called, even if the response is empty, as Finished()
   // will not be called until the Response has been processed.

   SetResponse(m_response_str.c_str(), m_response_str.size());

   // Wait for the framework to call Finished()

   auto finished = m_promise.get_future();

   if(finished.wait_for(std::chrono::seconds(ExecuteTimeout)) == std::future_status::timeout)
   {
      throw XrdSsiException("RequestProc::Finished() was never called!");

      // Potentially could call Finished() with cancel == true here instead of throwing an exception?
   }
}



/*!
 * Clean up the Request Processing object.
 *
 * This is called when the Request has been processed or cancelled.
 *
 * If required, you can create specialized versions of this method to handle cancellation/cleanup for
 * specific message types.
 */

template <typename RequestType, typename MetadataType, typename AlertType>
void RequestProc<RequestType, MetadataType, AlertType>::Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel)
{
#ifdef XRDSSI_DEBUG
   std::cout << "[DEBUG] RequestProc::Finished()" << std::endl;
#endif

   if(cancel)
   {
      // Reclaim resources dedicated to the request and then tell caller the request object can be reclaimed.
   }
   else
   {
      // Reclaim any allocated resources
   }

   // Tell Execute() that we have Finished()

   m_promise.set_value();
}

} // namespace XrdSsiPb

#endif
