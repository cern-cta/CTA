/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <google/protobuf/service.h>

namespace cta { namespace objectstore {
/**
 * This singleton class will check the compatibility of the runtime library
 * with the headers it was compiled against on construction (library load)
 * It will also cleanup 
 */
class ProtobufCleaner {
  ProtobufCleaner() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
  }
  
 virtual ~ProtobufCleaner() {
   // Make protobuf cleanup memory to not trigger memory leaks detectors.
   google::protobuf::ShutdownProtobufLibrary();
 }
} g_protobufCleaner;

}} // end of namespaces
