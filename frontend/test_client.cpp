#include <unistd.h> // for sleep

#include "TestSsiService.h"
#include "TestSsiProtobuf.h"

int main(int argc, char *argv[])
{
   // Verify that the version of the Google Protocol Buffer library that we linked against is
   // compatible with the version of the headers we compiled against

   GOOGLE_PROTOBUF_VERIFY_VERSION;

   // Obtain a Service Provider

   const std::string host = "localhost";
   const int         port = 10400;

   try
   {
      TestSsiService test_ssi_service(host, port);

      // Create a Request object

      xrdssi::test::Request request;

      request.set_message_text("Archive some file");

      // Output message in Json format

      std::cout << "Sending message:" << std::endl;
      std::cout << xrdssi::test::MessageToJsonString(request);


   // Initiate a Request

#if 0
   // Requests are always executed in the context of a service. They need to correspond to what the service allows.

   XrdSsiRequest *theRequest; // Used for demonstration purposes

   // Create a request object (upcast from your implementation)

   int  reqDLen = 1024;
   char reqData[reqDLen];

   theRequest = new MyRequest(reqData, reqDLen, 5);

   // Transfer ownership of the request to the service object

   servP->ProcessRequest(*theRequest, theResource);

   // MyRequest object handles deletion of the data buffer, we shall never mention this pointer again...

   theRequest = NULL;

   // Note: it is safe to delete the XrdSsiResource object after ProcessRequest() returns.
#endif

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
