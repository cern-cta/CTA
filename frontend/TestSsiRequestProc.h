#ifndef __TEST_SSI_REQUEST_PROC_H
#define __TEST_SSI_REQUEST_PROC_H

#include <XrdSsi/XrdSsiResponder.hh>
#include "XrdSsiException.h"

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

template <typename RequestType, typename ResponseType>
class RequestProc : public XrdSsiResponder
{
public:
                RequestProc() {}
   virtual     ~RequestProc() {}

           void Execute();
   virtual void Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel=false) override;

private:
   // These methods should be specialized according to the needs of each <RequestType, ResponseType> pair

   void ExecuteAction()   { std::cerr << "Called default ExecuteAction()" << std::endl; }
   void ExecuteAlerts()   { std::cerr << "Called default ExecuteAlerts()" << std::endl; }
   void ExecuteMetadata() { std::cerr << "Called default ExecuteMetadata()" << std::endl; }

   RequestType  request;
   ResponseType response;
};



template <typename RequestType, typename ResponseType>
void RequestProc<RequestType, ResponseType>::Execute()
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

   if(!request.ParseFromString(request_str))
   {
      throw XrdSsiException("ParseFromString() failed");
   }

   // Release the request buffer (optional, perhaps it is more efficient to reuse it?)

   ReleaseRequestBuffer();

   // Perform the requested action

   ExecuteAction();

   // Optional: send alerts

   ExecuteAlerts();

   // Optional: send metadata ahead of the response

   ExecuteMetadata();

   // Serialize the Response

   std::string response_str;

   if(!response.SerializeToString(&response_str))
   {
      throw XrdSsiException("SerializeToString() failed");
   }

   // Send the response

   SetResponse(response_str.c_str(), response_str.length());
}



// Create specialized versions of this method to handle cancellation/cleanup for specific message types

template <typename RequestType, typename ResponseType>
void RequestProc<RequestType, ResponseType>::Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel)
{
   using namespace std;

   cerr << "Finished()" << endl;

   // Reclaim any allocated resources
}

#endif
