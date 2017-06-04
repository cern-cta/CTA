#include <iostream>

#include "XrdSsiException.h"
#include "TestSsiRequestProc.h"
#include "TestSsiProtobuf.h"

// This is for specialized private methods called by RequestProc::Execute to handle actions, alerts
// and metadata

template <>
void RequestProc<xrdssi::test::Request, xrdssi::test::Result, xrdssi::test::Metadata, xrdssi::test::Alert>::ExecuteAction()
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



// A metadata-only reply is appropriate when we just need to send a short response/acknowledgement,
// as it has less overhead than a full response. The maximum amount of metadata that may be sent is
// defined by XrdSsiResponder::MaxMetaDataSZ constant member.

template <>
void RequestProc<xrdssi::test::Request, xrdssi::test::Result, xrdssi::test::Metadata, xrdssi::test::Alert>::ExecuteMetadata()
{
   std::cerr << "Sending metadata..." << std::endl;
   const std::string metadata("Have some metadata!");
   SetMetadata(metadata.c_str(), metadata.size());
}



/*
 * Alert messages can be anything you want them to be. The SSI framework enforces
 * the following rules:
 *
 * - alerts are sent in the order posted,
 * - all outstanding alerts are sent before the final response is sent (i.e. the one
 * posted using a SetResponse() method),
 * - once a final response is posted, subsequent alert messages are not sent, and
 * - if a request is cancelled, all pending alerts are discarded.
 *
 * In order to send an alert, you must encapsulate your message using an
 * XrdSsiRespInfoMsg object. The object provides synchronization between posting
 * an alert, sending the alert, and releasing storage after the alert was sent. It is defined
 * in the XrdSsiRespInfo.hh header file.
 *
 * The XrdSsiRespInfoMsg object needs to be inherited by whatever class you use to
 * manage your alert message. The pure abstract class, RecycleMsg() must also be
 * implemented. This method is called after the message is sent or when it is discarded.
 * A parameter to the method tells you why it’s being called.
 *
 * See example p.45
 */
