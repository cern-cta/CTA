#include <iostream>
#include "test_util.h"

#include "test.pb.h"
#include "XrdSsiException.h"
#include "XrdSsiPbRequestProc.h"

// This is for specialized private methods called by RequestProc::Execute to handle actions, alerts and metadata

template <>
void RequestProc<xrdssi::test::Request, xrdssi::test::Result, xrdssi::test::Metadata, xrdssi::test::Alert>::ExecuteAction()
{
   // Output message in Json format (for debugging)

   std::cerr << "Received message:" << std::endl;
   std::cerr << MessageToJsonString(m_request);

   // Set reply

   m_response.set_result_code(0);
   m_response.mutable_response()->set_message_text("This is the reply to " + *m_request.mutable_message_text());

   // Output message in Json format (for debugging)

   std::cerr << "Preparing response:" << std::endl;
   std::cerr << MessageToJsonString(m_response);
}



// A metadata-only reply is appropriate when we just need to send a short response/acknowledgement,
// as it has less overhead than a full response. The maximum amount of metadata that may be sent is
// defined by XrdSsiResponder::MaxMetaDataSZ constant member.

template <>
void RequestProc<xrdssi::test::Request, xrdssi::test::Result, xrdssi::test::Metadata, xrdssi::test::Alert>::ExecuteMetadata()
{
   // Set metadata

   m_metadata.set_message_text("Have some metadata");

   // Output message in Json format (for debugging)

   std::cerr << "Preparing metadata..." << std::endl;
   std::cerr << MessageToJsonString(m_metadata);
}



// Alert messages can be anything you want them to be. The SSI framework enforces the following rules:
//
// * alerts are sent in the order posted
// * all outstanding alerts are sent before the final response is sent (i.e. the one posted using a
//   SetResponse() method)
// * once a final response is posted, subsequent alert messages are not sent
// * if a request is cancelled, all pending alerts are discarded

template <>
void RequestProc<xrdssi::test::Request, xrdssi::test::Result, xrdssi::test::Metadata, xrdssi::test::Alert>::ExecuteAlerts()
{
   // Set alert message

   m_alert.set_message_text("Something bad happened");

   // Send the alert message

   Alert(m_alert);
}
