#include <unistd.h> // for sleep

#include "TestSsiService.h"
#include "TestSsiProtobuf.h"

int main(int argc, char *argv[])
{
   const std::string host = "localhost";
   const int         port = 10400;

   // Verify that the version of the Google Protocol Buffer library that we linked against is
   // compatible with the version of the headers we compiled against

   GOOGLE_PROTOBUF_VERIFY_VERSION;

   try
   {
      // Obtain a Service Provider

      TestSsiService<xrdssi::test::Request, xrdssi::test::Result> test_ssi_service(host, port);

      // Create a Request object

      xrdssi::test::Request request;

      request.set_message_text("Archive some file");

      // Output message in Json format (for debugging only)

      std::cout << "Sending message:" << std::endl;
      std::cout << xrdssi::test::MessageToJsonString(request);

      // Send the Request to the Service

      test_ssi_service.send(request);

      // Wait for the response callback

      std::cout << "Request sent, going to sleep..." << std::endl;

      int wait_secs = 40;

      while(--wait_secs)
      {
         std::cerr << ".";
         sleep(1);
      }

      std::cout << "All done, exiting." << std::endl;
   }
   catch (std::exception& e)
   {
      std::cerr << "TestSsiService failed with error: " << e.what() << std::endl;

      return 1;
   }

   // Optional: Delete all global objects allocated by libprotobuf

   google::protobuf::ShutdownProtobufLibrary();
}
