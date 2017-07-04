#ifndef __XRD_SSI_PB_REQUEST_PROC_H
#define __XRD_SSI_PB_REQUEST_PROC_H

#include <XrdSsi/XrdSsiResponder.hh>
#include "XrdSsiException.h"
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

   void ExecuteAction()   { std::cerr << "Called default ExecuteAction()" << std::endl; }
   void ExecuteAlerts()   { std::cerr << "Called default ExecuteAlerts()" << std::endl; }
   void ExecuteMetadata() { std::cerr << "Called default ExecuteMetadata()" << std::endl; }

   RequestType  m_request;
   ResponseType m_response;
   AlertType    m_alert;
   MetadataType m_metadata;
};



template <typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
void RequestProc<RequestType, ResponseType, MetadataType, AlertType>::Execute()
{
   using namespace std;

   cerr << "Execute()" << endl;

   // Unpack the Request buffer into a string object.
   //
   // We need to construct this with an explicit length, as request_buffer is a binary buffer, not a
   // null-terminated string.

   int request_len;
   const char *request_buffer = GetRequest(request_len);
   const std::string request_str(request_buffer, request_len);

   // Deserialize the Request

   if(!m_request.ParseFromString(request_str))
   {
      throw XrdSsiException("request.ParseFromString() failed");
   }

   // Release the request buffer (optional, perhaps it is more efficient to reuse it?)

   ReleaseRequestBuffer();

   // Perform the requested action

   ExecuteAction();

   // Optional: send alerts

   ExecuteAlerts();

   // Optional: prepare to send metadata ahead of the response

   ExecuteMetadata();

   // Serialize the Metadata

   std::string response_str;

   if(!m_metadata.SerializeToString(&response_str))
   {
      throw XrdSsiException("metadata.SerializeToString() failed");
   }

   // Send the Metadata

   if(response_str.size() > 0)
   {
      // Temporary workaround for XrdSsi bug #537:
      response_str = " " + response_str;

      SetMetadata(response_str.c_str(), response_str.size());
   }

   // Serialize the Response

   if(!m_response.SerializeToString(&response_str))
   {
      throw XrdSsiException("response.SerializeToString() failed");
   }

   // Send the response

   if(response_str.size() > 0)
   {
      SetResponse(response_str.c_str(), response_str.size());
   }
}



// Create specialized versions of this method to handle cancellation/cleanup for specific message types

template <typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
void RequestProc<RequestType, ResponseType, MetadataType, AlertType>::Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel)
{
   using namespace std;

   cerr << "Finished()" << endl;

   // Reclaim any allocated resources
}

#endif
