#ifndef __TEST_UTIL_H
#define __TEST_UTIL_H

#include <stdio.h>
#include <iostream>
#include <google/protobuf/util/json_util.h>

// Helper functions for debugging

static void OutputJsonString(const google::protobuf::Message *message)
{
   using namespace google::protobuf::util;

   std::string output;
   JsonPrintOptions options;

   options.add_whitespace = true;
   options.always_print_primitive_fields = true;

   MessageToJsonString(*message, &output, options);

   std::cout << output << std::endl;
}

inline void DumpBuffer(const std::string &buffer)
{
   for(size_t i = 0; i < buffer.size(); ++i)
   {
      fprintf(stderr, "%02X ", buffer[i]);
   }
   fprintf(stderr, "\n");
}

#endif
