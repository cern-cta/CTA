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

#include <XrdSsi/XrdSsiResponder.hh>
#include "XrdSsiPbException.h"
#include "XrdSsiPbAlert.h"



//! Error codes that the framework knows about

enum XrdSsiRequestProcErr {
   PB_PARSE_ERR
};



/*!
 * The XrdSsiResponder class knows how to safely interact with the request object. It allows handling asynchronous
 * requests such as cancellation, broken TCP connections, etc.
 *
 * The XrdSsiResponder class contains all the methods needed to interact with the request object: get the request,
 * release storage, send alerts, and post a response.
 *
 * The Request object will be bound to the XrdSsiResponder object via a call to XrdSsiResponder::BindRequest().
 * Once the relationship is established, you no longer need to keep a reference to the request object. The SSI
 * framework keeps track of the request object for you.
 *
 * RequestProc is a kind of agent object that the service object creates for each request that it receives.
 */

template <typename RequestType, typename MetadataType, typename AlertType>
class RequestProc : public XrdSsiResponder
{
public:
                RequestProc() {
#ifdef XRDSSI_DEBUG
                   std::cout << "Called RequestProc() constructor" << std::endl;
#endif
                }
   virtual     ~RequestProc() {
#ifdef XRDSSI_DEBUG
                   std::cout << "Called ~RequestProc() destructor" << std::endl;
#endif
                }

           void Execute();
   virtual void Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel=false) override;

private:

   void Alert(const AlertType &alert)
   {
      // Encapsulate the Alert protocol buffer inside a XrdSsiRespInfoMsg object. Alert message objects
      // are created on the heap with lifetime managed by the XrdSsiResponder class.

      XrdSsiResponder::Alert(*(new AlertMsg<AlertType>(alert)));
   }

   // These methods should be specialized according to the needs of each <RequestType, MetadataType> pair

   void ExecuteAction() {
#ifdef XRDSSI_DEBUG
      std::cout << "Called default ExecuteAction()" << std::endl;
#endif
   }
   void ExecuteAlerts() {
#ifdef XRDSSI_DEBUG
      std::cout << "Called default ExecuteAlerts()" << std::endl;
#endif
   }
   void ExecuteMetadata() {
#ifdef XRDSSI_DEBUG
      std::cout << "Called default ExecuteMetadata()" << std::endl;
#endif
   }

   // This method must put the error into the Metadata to send it back to the client

   void HandleError(XrdSsiRequestProcErr err_num, const std::string &err_text = "");

   // Protocol Buffer members

   RequestType  m_request;
   MetadataType m_metadata;
   AlertType    m_alert;

   // Metadata and Response buffers must stay in scope until Finished() is called, so they need to be member variables

   std::string  m_metadata_str;
   std::string  m_response_str;
};



template <typename RequestType, typename MetadataType, typename AlertType>
void RequestProc<RequestType, MetadataType, AlertType>::Execute()
{
#ifdef XRDSSI_DEBUG
   std::cout << "Execute()" << std::endl;
#endif

   // Deserialize the Request

   int request_len;
   const char *request_buffer = GetRequest(request_len);

   if(m_request.ParseFromArray(request_buffer, request_len))
   {
      // Perform the requested action

      ExecuteAction();

      // Send alerts

      ExecuteAlerts();

      // Prepare to send metadata ahead of the response

      ExecuteMetadata();
   }
   else
   {
      // Return an error to the client

      HandleError(PB_PARSE_ERR, "m_request.ParseFromArray() failed");
   }

   // Release the request buffer

   ReleaseRequestBuffer();

   // Serialize the Metadata

   if(!m_metadata.SerializeToString(&m_metadata_str))
   {
      throw XrdSsiPbException("m_metadata.SerializeToString() failed");
   }

   // Send the Metadata

   if(m_metadata_str.size() > 0)
   {
      SetMetadata(m_metadata_str.c_str(), m_metadata_str.size());
   }

#if 0
   // Serialize the Response

   if(!m_response.SerializeToString(&m_response_str))
   {
      throw XrdSsiPbException("m_response.SerializeToString() failed");
   }
#endif

   // Send the response. This must always be called, even if the response is empty, as Finished()
   // will not be called until the Response has been processed.

   SetResponse(m_response_str.c_str(), m_response_str.size());
}



// Create specialized versions of this method to handle cancellation/cleanup for specific message types

template <typename RequestType, typename MetadataType, typename AlertType>
void RequestProc<RequestType, MetadataType, AlertType>::Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel)
{
#ifdef XRDSSI_DEBUG
   std::cout << "Finished()" << std::endl;
#endif

   if(cancel)
   {
      // Reclaim resources dedicated to the request and then tell caller the request object can be reclaimed.
   }
   else
   {
      // Reclaim any allocated resources
   }
}

#endif
