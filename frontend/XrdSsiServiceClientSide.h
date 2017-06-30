#ifndef __TEST_SSI_CLIENT_H
#define __TEST_SSI_CLIENT_H

#include <iostream>

#include <XrdSsi/XrdSsiProvider.hh>
#include <XrdSsi/XrdSsiService.hh>
#include "XrdSsiException.h"
#include "TestSsiRequest.h"



// XrdSsiProviderClient is instantiated and managed by the SSI library

extern XrdSsiProvider *XrdSsiProviderClient;



template <typename RequestType, typename ResponseType, typename MetadataType, typename AlertType>
class XrdSsiServiceClientSide
{
public:
   // default constructor to be used on the server side:

   XrdSsiServiceClientSide() = delete;

   // constructor to be used on the client side (to establish a connection to the server):

   XrdSsiServiceClientSide(std::string hostname, int port, std::string _resource, int _timeout = 15) : resource(_resource), timeout(_timeout)
   {
      XrdSsiErrInfo eInfo;

      if(!(serverP = XrdSsiProviderClient->GetService(eInfo, hostname + ":" + std::to_string(port))))
      {
         throw XrdSsiException(eInfo);
      }
   }

   ~XrdSsiServiceClientSide()
   {
      // The XrdSsiService object cannot be explicitly deleted. The Stop() method deletes the object if
      // it is safe to do so. A service object can only be deleted after all requests handed to the object
      // have completed, i.e. Finished() has been called on each request.

      if(serverP->Stop())
      {
         std::cerr << "Stopped SSI service" << std::endl;
      }
      else
      {
         std::cerr << "Failed to stop SSI service" << std::endl;

         // Unpalatable choice here between passing back control to the calling scope with a possible memory leak,
         // or trying to force all requests to Finish with possible blocking behaviour...
      }
   }

   void send(RequestType request)
   {
      // Serialize the request object

      std::string request_str;

      if(!request.SerializeToString(&request_str))
      {
         throw XrdSsiException("request.SerializeToString() failed");
      }

      // Requests are always executed in the context of a service. They need to correspond to what the service allows.

      XrdSsiRequest *requestP = new TestSsiRequest<RequestType, ResponseType, MetadataType, AlertType> (request_str, timeout);

      // Transfer ownership of the request to the service object
      // TestSsiRequest handles deletion of the request buffer, so we can allow the pointer to go out-of-scope

      serverP->ProcessRequest(*requestP, resource);

      // Note: it is safe to delete the XrdSsiResource object after ProcessRequest() returns. I don't delete it because
      // I am assuming I can reuse it, but I need to check if that is a safe assumption. Perhaps I need to create a new
      // resource object for each request?
      //
      // See SSI ref p.10 on configuring a resource to be reusable
      // Do I need more than one resource? I could have a single resource called "/default" with some default options and
      // if necessary I can add other resources with other options later.
      //
      // Possible useful options:
      //
      // For specifying the tapeserver callback:
      // XrdSsiResource::rInfo
      // This option allows you to send additional out-of-band information to the
      // server that will be executing the request. The information should be specified
      // in CGI format (i.e. key=value[&key=value[...]]). This information is supplied
      // to the server-side service in its corresponding request resource object. Note
      // that restrictions apply for reusable resources.
      //
      // XrdSsiResource::rUser
      // This is an arbitrary string that is meant to further identify the request. The
      // SSI framework normally uses this information to tag log messages. It is also
      // supplied to the server-side service in its corresponding request resource
      // object.
   }

private:
   XrdSsiResource resource;          // Requests are bound to this resource

   XrdSsiService *serverP;           // Pointer to XRootD Server object

   int timeout;                      // Server timeout
};

#endif
