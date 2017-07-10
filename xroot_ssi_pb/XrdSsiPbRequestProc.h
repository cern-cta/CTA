#ifndef __XRD_SSI_PB_REQUEST_PROC_H
#define __XRD_SSI_PB_REQUEST_PROC_H

#include <XrdSsi/XrdSsiResponder.hh>
#include "XrdSsiPbException.h"
#include "XrdSsiPbAlert.h"

/*
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

template <typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
class RequestProc : public XrdSsiResponder
{
public:
                RequestProc() {}
   virtual     ~RequestProc() {}

           void Execute();
   virtual void Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel=false) override;

private:

   void Alert(const AlertType &alert)
   {
      // Encapsulate the Alert protocol buffer inside a XrdSsiRespInfoMsg object. Alert message objects
      // are created on the heap with lifetime managed by the XrdSsiResponder class.

      XrdSsiResponder::Alert(*(new AlertMsg<AlertType>(alert)));
   }

   // These methods should be specialized according to the needs of each <RequestType, ResponseType> pair

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

   RequestType  m_request;
   ResponseType m_response;
   AlertType    m_alert;
   MetadataType m_metadata;

   // Metadata and response buffers must stay in scope until Finished() is called, so they need to be member variables

   std::string  m_response_str;
   std::string  m_metadata_str;
};



template <typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
void RequestProc<RequestType, ResponseType, MetadataType, AlertType>::Execute()
{
#ifdef XRDSSI_DEBUG
   std::cout << "Execute()" << std::endl;
#endif

   // Deserialize the Request

   int request_len;
   const char *request_buffer = GetRequest(request_len);

   if(!m_request.ParseFromArray(request_buffer, request_len))
   {
      throw XrdSsiPbException("m_request.ParseFromArray() failed");
   }

   // Release the request buffer

   ReleaseRequestBuffer();

   // Perform the requested action

   ExecuteAction();

   // Send alerts

   ExecuteAlerts();

   // Prepare to send metadata ahead of the response

   ExecuteMetadata();

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

   // Serialize the Response

   if(!m_response.SerializeToString(&m_response_str))
   {
      throw XrdSsiPbException("m_response.SerializeToString() failed");
   }

   // Send the response

   if(m_response_str.size() > 0)
   {
      SetResponse(m_response_str.c_str(), m_response_str.size());
   }
}



// Create specialized versions of this method to handle cancellation/cleanup for specific message types

template <typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
void RequestProc<RequestType, ResponseType, MetadataType, AlertType>::Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel)
{
#ifdef XRDSSI_DEBUG
   std::cout << "Finished()" << std::endl;
#endif

   // Reclaim any allocated resources
}

#endif
