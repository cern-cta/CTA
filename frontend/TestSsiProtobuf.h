#ifndef __TEST_SSI_PROTOBUF_H
#define __TEST_SSI_PROTOBUF_H

#include <google/protobuf/util/json_util.h>

#include "test.pb.h"

namespace xrdssi {
namespace test {

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

} // test
} // xrdssi

#endif
