#include "test_util.h"      // for Json output (only needed for debugging)

#include "test_client.h"    // Binds XRootD SSI to Google Protocol buffer

// Define the location of the XRootD SSI Service

const std::string host     = "localhost";
const int         port     = 10400;
const std::string resource = "/test";



int main(int argc, char *argv[])
{
   // Verify that the version of the Google Protocol Buffer library that we linked against is
   // compatible with the version of the headers we compiled against

   GOOGLE_PROTOBUF_VERIFY_VERSION;

   try
   {
      // Obtain a Service Provider

      XrdSsiServiceType test_ssi_service(host, port, resource);

      // Create a Request object

      xrdssi::test::Request request;

      request.set_message_text("Archive some file");

      // Output message in Json format

      std::cout << "Sending message:" << std::endl;
      std::cout << MessageToJsonString(request);

      // Send the Request to the Service

      test_ssi_service.send(request);

      // Wait for the response callback.

      std::cout << "Request sent, going to sleep..." << std::endl;

      // When test_ssi_service goes out-of-scope, the Service will try to shut down, but will wait
      // for outstanding Requests to be processed
   }
   catch (std::exception& e)
   {
      std::cerr << "XrdSsiServiceClient failed with error: " << e.what() << std::endl;

      return 1;
   }

   int wait_secs = 5;

   while(--wait_secs)
   {
      std::cerr << ".";
      sleep(1);
   }

   std::cout << "All done, exiting." << std::endl;

   // Optional: Delete all global objects allocated by libprotobuf

   google::protobuf::ShutdownProtobufLibrary();

   return 0;
}
