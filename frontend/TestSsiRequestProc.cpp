#include <iostream>

#include "TestSsiRequestProc.h"

void RequestProc::Execute()
{
   using namespace std;

   const string metadata("Have some metadata!");
   const string response("Have a response!");

   cerr << "Execute()" << endl;

   // Deserialize the Request

   //int reqLen;
   //const char *reqData = GetRequest(reqLen);

   // Parse the request

   ReleaseRequestBuffer(); // Optional

   // Perform the requested action

   // Optional: send alerts

   // Optional: send metadata ahead of the response

   SetMetadata(metadata.c_str(), metadata.size());

   // Send the response

   SetResponse(response.c_str(), response.size());
}



void RequestProc::Finished(XrdSsiRequest &rqstR, const XrdSsiRespInfo &rInfo, bool cancel)
{
   using namespace std;

   cerr << "Finished()" << endl;

   // Reclaim any allocated resources
}

