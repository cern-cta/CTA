#include <iostream>

#include "TestSsiService.h"
#include "TestSsiRequestProc.h"



void TestSsiService::ProcessRequest(XrdSsiRequest &reqRef, XrdSsiResource &resRef)
{
   using namespace std;

   cerr << "Called ProcessRequest()" << endl;

   RequestProc theProcessor;

   // Bind the processor to the request. This works because the
   // it inherited the BindRequest method from XrdSsiResponder.

   theProcessor.BindRequest(reqRef);

   // Execute the request, upon return the processor is deleted

   theProcessor.Execute();

   // Unbind the request from the responder (required)

   theProcessor.UnBindRequest();

   cerr << "ProcessRequest.UnBind()" << endl;
}

