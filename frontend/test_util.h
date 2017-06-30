#ifndef __TEST_UTIL_H
#define __TEST_UTIL_H

#include <google/protobuf/util/json_util.h>

// Helper function for debugging

inline std::string MessageToJsonString(const google::protobuf::Message &message)
{
   using namespace google::protobuf::util;

   std::string output;
   JsonPrintOptions options;

   options.add_whitespace = true;
   options.always_print_primitive_fields = true;

   MessageToJsonString(message, &output, options);    // returns util::Status

   return output;
}

#endif
