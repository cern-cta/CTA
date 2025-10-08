/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <memory>

#include "GetOptThreadSafe.hpp"
#include "common/exception/Exception.hpp"
#include "common/threading/MutexLocker.hpp"

namespace cta::utils {

GetOpThreadSafe::Reply GetOpThreadSafe::getOpt(const Request& request) {
  threading::MutexLocker locker(gMutex);
  // Prepare the classic styled argv
  std::unique_ptr<char*[]>argv(new char*[request.argv.size()]);
  char** p = argv.get();
  for(auto& a : request.argv) {
    // This is ugly, but getopt's interface takes NON-const char** for argv
    *(p++) = const_cast<char*>(a.c_str());
  }
  // Prepare the global variables
  ::optind = 0; // optind = 0 allows using GNU extentions on the option string
  ::opterr = 0; // Prevent getopt from printing to stderr

  // Find the present options
  Reply ret;
  int longIndex(0); // getopt_long sets longIndex to the index of the long option relative to longopts
  for(bool is_allOptionsProcessed = false; !is_allOptionsProcessed; ) {
    int c = ::getopt_long(static_cast<int>(request.argv.size()), argv.get(), request.optstring.c_str(), request.longopts, &longIndex);
    switch(c) {
      case -1:
        is_allOptionsProcessed = true;
        break;
      case 0:
        // We received a long option
        ret.options.push_back(FoundOption());
        ret.options.back().option = request.longopts[longIndex].name;
        if (::optarg) ret.options.back().parameter = ::optarg;
        break;
      case 1:
        // We received a non-option
        break;
      case '?':
      case ':': {
        // getopt is unhappy
        cta::exception::Exception e("Unexpected option: ");
        e.getMessage() << argv[::optind];
        throw e;
      }
      default:
        // We received a character option
        ret.options.push_back(FoundOption());
        ret.options.back().option = " ";
        ret.options.back().option[0] = c;
        if(::optarg) ret.options.back().parameter = ::optarg;
    }
  }
  size_t pos = ::optind;
  while(pos < request.argv.size()) {
    ret.remainder.push_back(argv.get()[pos++]);
  }
  return ret;
}

threading::Mutex GetOpThreadSafe::gMutex;

} // namespace cta::utils
