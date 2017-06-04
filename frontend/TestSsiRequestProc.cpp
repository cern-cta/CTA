#include <iostream>

#include "XrdSsiException.h"
#include "TestSsiRequestProc.h"
#include "TestSsiProtobuf.h"

// This is for specialized private methods called by RequestProc::Execute to handle actions, alerts
// and metadata

template <>
void RequestProc<xrdssi::test::Request, xrdssi::test::Result>::ExecuteAction()
{
   // Output message in Json format (for debugging)

   std::cerr << "Received message:" << std::endl;
   std::cerr << xrdssi::test::MessageToJsonString(request);

   // Set reply

   response.set_result_code(0);
   response.mutable_response()->set_message_text("This is the reply to " + *request.mutable_message_text());

   // Output message in Json format (for debugging)

   std::cerr << "Sending response:" << std::endl;
   std::cerr << xrdssi::test::MessageToJsonString(response);
}


template <>
void RequestProc<xrdssi::test::Request, xrdssi::test::Result>::ExecuteMetadata()
{
   const std::string metadata("Have some metadata!");
   SetMetadata(metadata.c_str(), metadata.size());
}

