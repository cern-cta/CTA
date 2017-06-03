#ifndef __TEST_SSI_SERVICE_H
#define __TEST_SSI_SERVICE_H

#include <iostream>
#include <stdexcept>

#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Probably we want to allow multiple resources, e.g. streaming and non-streaming versions of the service
// Can this be defined in the protobuf definition?

const std::string TestSsiResource("/test");



// Class to convert a XRootD error into a std::exception

class XrdSsiException : public std::exception
{
public:
   XrdSsiException(const XrdSsiErrInfo &eInfo) : error_msg(eInfo.Get()) {}

   const char* what() const noexcept { return error_msg.c_str(); }

private:
   std::string error_msg;
};



// XrdSsiProviderClient is instantiated and managed by the SSI library

extern XrdSsiProvider *XrdSsiProviderClient;



class TestSsiService
{
public:
   TestSsiService() = delete;

   TestSsiService(std::string hostname, int port) : resource(TestSsiResource)
   {
      XrdSsiErrInfo eInfo;

      if(!(serverP = XrdSsiProviderClient->GetService(eInfo, hostname + ":" + std::to_string(port))))
      {
         throw XrdSsiException(eInfo);
      }
   }

   ~TestSsiService()
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

private:
   const XrdSsiResource resource;    // Requests are bound to this resource

   XrdSsiService *serverP;           // Pointer to XRootD Server object
};

#endif
