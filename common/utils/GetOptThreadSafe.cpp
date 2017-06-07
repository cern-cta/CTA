/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "GetOptThreadSafe.hpp"
#include "common/exception/Exception.hpp"
#include "common/threading/MutexLocker.hpp"

#include <memory>

namespace cta { namespace utils {

GetOpThreadSafe::Reply GetOpThreadSafe::getOpt(const Request& request) {
  threading::MutexLocker locker(gMutex);
  // Prepare the classic styled argv.
  std::unique_ptr<char * []>argv(new char *[request.argv.size()]);
  char ** p=argv.get();
  for (auto & a: request.argv) {
    // This is ugly, but getopt's interface takes NON-const char ** for argv.
    *(p++) = const_cast<char *>(a.c_str());
  }
  // Prepare the global variables
  ::optind=0; // optind = 0 allows using GNU extentions on the option string.
  ::opterr=0; // Prevent getopt from printing to stderr.
  // Prepare the return value
  Reply ret;
  // Find the present options
  int longIndex, c;
  while (-1 != (c = ::getopt_long(request.argv.size(), argv.get(), 
                                  request.optstring.c_str(),
                                  request.longopts, &longIndex))) {
    if (!c) {
      // We received a long option.
      ret.options.push_back(FoundOption());
      ret.options.back().option = request.longopts[longIndex].name;
      if (::optarg)
        ret.options.back().parameter = ::optarg;
    } else if (1 == c ) {
      // We received a non option in 
    } else if ('?' == c || ':' == c) {
      // getopt is unhappy
      cta::exception::Exception e("Unexpected option: ");
      e.getMessage() << argv[::optind];
      throw e;
    } else {
      // We received a character option.
      ret.options.push_back(FoundOption());
      ret.options.back().option = " ";
      ret.options.back().option[0] = c;
      if (::optarg)
        ret.options.back().parameter = ::optarg;
    }
  }
  size_t pos = ::optind;
  while (pos < request.argv.size()) {
    ret.remainder.push_back(argv.get()[pos++]);
  }
  return ret;
}

threading::Mutex GetOpThreadSafe::gMutex;

}} // namespace cta::utils
