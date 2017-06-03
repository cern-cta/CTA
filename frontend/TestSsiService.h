#ifndef __TEST_SSI_SERVICE_H
#define __TEST_SSI_SERVICE_H

#include <XrdSsi/XrdSsiService.hh>

#include "TestSsiRequestProc.h"



/*
 * Service Object, obtained using GetService() method of the TestSsiServiceProvider factory
 */

template <typename RequestType, typename ResponseType>
class TestSsiService : public XrdSsiService
{
public:
            TestSsiService() {}
   virtual ~TestSsiService() {}

   // The pure abstract method ProcessRequest() is called when the client calls its ProcessRequest() method to hand off
   // its request and resource objects. The client’s request and resource objects are transmitted to the server and passed
   // into the service’s ProcessRequest() method.

   virtual void ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef) override;

   // Additional virtual methods:
   //
   // Attach(): optimize handling of detached requests
   // Prepare(): perform preauthorization and resource optimization
};



template <typename RequestType, typename ResponseType>
void TestSsiService<RequestType, ResponseType>::ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef)
{
   std::cerr << "Called ProcessRequest()" << std::endl;

   RequestProc<RequestType, ResponseType> processor;

   // Bind the processor to the request. Inherits the BindRequest method from XrdSsiResponder.

   processor.BindRequest(reqRef);

   // Execute the request, upon return the processor is deleted

   processor.Execute();

   // Unbind the request from the responder (required)

   processor.UnBindRequest();

   std::cerr << "ProcessRequest.UnBind()" << std::endl;
}

#endif
