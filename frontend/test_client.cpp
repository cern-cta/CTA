#include <unistd.h>         // for sleep
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
      // (When this loop finishes, test_ssi_service will go out-of-scope and the service will be shut down)

      std::cout << "Request sent, going to sleep..." << std::endl;

      int wait_secs = 20;

      while(--wait_secs)
      {
         std::cerr << ".";
         sleep(1);
      }
   }
   catch (std::exception& e)
   {
      std::cerr << "XrdSsiServiceClient failed with error: " << e.what() << std::endl;

      return 1;
   }

   std::cout << "All done, exiting." << std::endl;

   // Optional: Delete all global objects allocated by libprotobuf

   google::protobuf::ShutdownProtobufLibrary();

   return 0;
}
