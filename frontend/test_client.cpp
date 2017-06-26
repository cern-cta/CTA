#include <iostream>

#include <google/protobuf/util/json_util.h> // for Json output

#include "test.pb.h"

int main(int argc, char *argv[])
{
   using namespace std;

   // Verify that the version of the library that we linked against is compatible with the version of
   // the headers we compiled against.

   GOOGLE_PROTOBUF_VERIFY_VERSION;

   eos::wfe::Request request;

   request.set_message_text("Archive some file");

   // Output message in Json format

   google::protobuf::util::JsonPrintOptions options;
   options.add_whitespace = true;
   options.always_print_primitive_fields = true;
   string jsonNotification;
   google::protobuf::util::MessageToJsonString(request, &jsonNotification, options);
   cout << "Sending message:" << endl << jsonNotification;

   // Optional: Delete all global objects allocated by libprotobuf

   google::protobuf::ShutdownProtobufLibrary();

   return 0;
}

