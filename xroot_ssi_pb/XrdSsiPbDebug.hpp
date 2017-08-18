/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Helper functions for debugging protocol buffers
 * @copyright      Copyright 2017 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <iostream>
#include <stdio.h>
#include <google/protobuf/util/json_util.h>

namespace XrdSsiPb {

/*!
 * Wrapper around google::protobuf::util::MessageToJsonString() which outputs to a stream
 */

inline void OutputJsonString(std::ostream &os, const google::protobuf::Message *message)
{
   using namespace google::protobuf::util;

   std::string output;
   JsonPrintOptions options;

   options.add_whitespace = true;
   options.always_print_primitive_fields = true;

   MessageToJsonString(*message, &output, options);

   os << output << std::endl;
}



/*!
 * Inspect the contents of a serialized Protocol Buffer
 */

inline void DumpBuffer(const std::string &buffer)
{
   for(size_t i = 0; i < buffer.size(); ++i)
   {
      fprintf(stderr, "%02X ", buffer[i]);
   }
   fprintf(stderr, "\n");
}

} // namespace XrdSsiPb

