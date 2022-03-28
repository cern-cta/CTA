/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <getopt.h>
#include <string>
#include <iostream>
#include <memory>
#include <fstream>
#include <XrdCl/XrdClFileSystem.hh>
#include <XrdCl/XrdClDefaultEnv.hh>
#include "common/utils/Regex.hpp"
#include "common/exception/XrootCl.hpp"


// No short options.
const char short_options[] = "";  
// We require a handful of long options.

enum class OptionIds: int {
  eos_instance = 1,
  eos_poweruser = 2,
  eos_dir = 3,
  subdir = 4,
  file = 5,
  error_dir = 6
};

const struct ::option long_options[] = {
  { "eos-instance", required_argument, nullptr, (int)OptionIds::eos_instance },
  { "eos-poweruser", required_argument, nullptr, (int)OptionIds::eos_poweruser },
  { "eos-dir", required_argument, nullptr, (int)OptionIds::eos_dir },
  { "subdir", required_argument, nullptr, (int)OptionIds::subdir },
  { "file", required_argument, nullptr, (int)OptionIds::file },
  { "error-dir", required_argument, nullptr, (int)OptionIds::error_dir },
  { nullptr, 0, nullptr, 0 }
};

void help() {
  std::cerr << "Expected parameters are: ";
  const struct ::option * pOpt = long_options;
  while (pOpt->name) {
    std::cerr << "--" << pOpt->name << " ";
    ++pOpt;
  }
  std::cerr << std::endl;
}

// We make these variables global as they will be part of the process's environment.
std::unique_ptr<char[]> envKRB5CCNAME;
char envXrdSecPROTOCOL[] = "XrdSecPROTOCOL=krb5";


int main(int argc, char **argv) { 
  
  struct {
    std::string eos_instance;
    std::string eos_poweruser;
    std::string eos_dir;
    std::string subdir;
    std::string file;
    std::string error_dir;
  } options;
  try {
    int opt_ret;
    while (-1 != (opt_ret = getopt_long(argc, argv, short_options, long_options, nullptr))) {
      switch (opt_ret) {
      case (int)OptionIds::eos_instance:
        options.eos_instance = optarg;
        break;
      case (int)OptionIds::eos_poweruser:
        options.eos_poweruser = optarg;
        break;
      case (int)OptionIds::eos_dir:
        options.eos_dir = optarg;
        break;
      case (int)OptionIds::subdir:
        options.subdir = optarg;
        break;
      case (int)OptionIds::file:
        options.file = optarg;
        break;
      case (int)OptionIds::error_dir:
        options.error_dir = optarg;
        break;
      case '?':
      default:
        std::cerr << "Unexpected option or missing argument." << std::endl;
        exit(EXIT_FAILURE);
        break;
      }
    }

    if (options.eos_instance.empty() || options.eos_poweruser.empty() || options.error_dir.empty() || options.subdir.empty() ||
        options.file.empty() || options.error_dir.empty()) {
      std::cerr << "At least one option missing." << std::endl;
      help();
      exit (EXIT_FAILURE);
    }

  //  std::cout << "To run again: "    << argv[0] 
  //            << " --eos-instance="  << options.eos_instance
  //            << " --eos-poweruser=" << options.eos_poweruser
  //            << " --eos-dir="        << options.eos_dir
  //            << " --subdir="         << options.subdir
  //            << " --file="           << options.file
  //            << " --error_dir="      << options.error_dir << std::endl;

    // Get the extended attribute for the retrieve request id
    std::string retrieveRequestId, fileName(options.eos_dir + "/" + options.subdir + "/" + options.file);
    XrdCl::FileSystem xrdfs(options.eos_instance);
    std::string errFileName;
    try {
      // Prepare Xrootd environment.
      errFileName = options.error_dir + '/' + "XATTRGET2_" + options.subdir + '_' + options.file;
      XrdCl::DefaultEnv::SetLogLevel("Dump");
      XrdCl::DefaultEnv::SetLogFile(errFileName);
      std::string envKRB5CCNAMEvalue = std::string("KRB5CCNAME=/tmp/") + options.eos_poweruser + "/krb5cc_0";
      // We need to copy to an array because of putenv's lack of const correctness.
      envKRB5CCNAME.reset(new char[envKRB5CCNAMEvalue.size() + 1]);
      strncpy(envKRB5CCNAME.get(), envKRB5CCNAMEvalue.c_str(), envKRB5CCNAMEvalue.size() + 1);
      putenv(envKRB5CCNAME.get());
      putenv(envXrdSecPROTOCOL);

      std::string query = fileName + "?mgm.pcmd=xattr&mgm.subcmd=get&mgm.xattrname=sys.retrieve.req_id";
      auto qcOpaque = XrdCl::QueryCode::OpaqueFile;
      XrdCl::Buffer xrdArg;
      xrdArg.FromString(query);
      XrdCl::Buffer *respPtr = nullptr;
      auto status = xrdfs.Query(qcOpaque, xrdArg, respPtr, (uint16_t)0 /*timeout=default*/);
      // Ensure proper memory management for the response buffer (it is our responsilibity to free it, we delegate to the unique_ptr).
      std::unique_ptr<XrdCl::Buffer> respUP(respPtr);
      respPtr = nullptr;
      cta::exception::XrootCl::throwOnError(status, "Error during XrdCl::Query");
      cta::utils::Regex re("value=(.*)");
      std::string respStr(respUP->GetBuffer(), respUP->GetSize());
      auto reResult = re.exec(respStr);
      if (reResult.size() != 2) {
        // We did not receive the expected structure
        throw cta::exception::Exception(std::string("Unexpected result from xattr query: ") + respStr);
      }
      retrieveRequestId = reResult[1];
      unlink(errFileName.c_str());
    } catch (cta::exception::Exception & ex) {
      std::cerr << "ERROR: failed to get request Id for file " << fileName << " full logs in " << errFileName << std::endl;
      std::ofstream errFile(errFileName, std::ios::out | std::ios::app);
      errFile << ex.what();
      return 1;
    } catch (std::exception & ex) {
      std::cerr << "ERROR: a standard exception occurred" << " full logs in " << errFileName << std::endl;
      std::ofstream errFile(errFileName, std::ios::out | std::ios::app);
      errFile << ex.what();
      return 1;
    }
    try {
      // Prepare Xrootd environment.
      errFileName = options.error_dir + '/' + "PREPAREABORT_" + options.subdir + '_' + options.file;
      XrdCl::DefaultEnv::SetLogLevel("Dump");
      XrdCl::DefaultEnv::SetLogFile(errFileName);
      std::vector<std::string> files = { retrieveRequestId, fileName };
      XrdCl::PrepareFlags::Flags flags = XrdCl::PrepareFlags::Cancel;
      XrdCl::Buffer *respPtr = nullptr;
      auto abortStatus = xrdfs.Prepare(files, flags, 0, respPtr, 0 /* timeout */);
      // Ensure proper memory management for the response buffer (it is our responsilibity to free it, we delegate to the unique_ptr).
      std::unique_ptr<XrdCl::Buffer> respUP(respPtr);
      respPtr = nullptr;
      cta::exception::XrootCl::throwOnError(abortStatus, "Error during XrdCl::Prepare");
      unlink(errFileName.c_str());
    } catch (cta::exception::Exception & ex) {
      std::cerr << "ERROR: failed to get request Id for file " << fileName << " full logs in " << errFileName << std::endl;
      std::ofstream errFile(errFileName, std::ios::out | std::ios::app);
      errFile << ex.what();
      return 1;
    } catch (std::exception & ex) {
      std::cerr << "ERROR: a standard exception occurred" << " full logs in " << errFileName << std::endl;
      std::ofstream errFile(errFileName, std::ios::out | std::ios::app);
      errFile << ex.what();
      return 1;
    }
    return 0;
  } catch (std::exception &ex) {
    std::cerr << "ERROR: a standard exception occurred " << ex.what() << std::endl;
    return 1;
  } catch (...) {
    std::cerr << "ERROR: an unknown general exception occurred " << std::endl;
    return 1;
  }
}
