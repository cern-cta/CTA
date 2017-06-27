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

#include "cmdline/Configuration.hpp"
#include "common/Configuration.hpp"
#include "common/dataStructures/FrontendReturnCode.hpp"

#include "XrdCl/XrdClCopyProcess.hh"
#include "XrdCl/XrdClEnv.hh"
#include "XrdCl/XrdClDefaultEnv.hh"

#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>

#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdexcept>
#include <xrootd/XrdCl/XrdClFile.hh>

/**
 * Returns true if --stderr is on the command-line.
 *
 * @param argc The number of command-line arguments.
 * @param argv The command-line arguments.
 */
static bool stderrIsOnTheCmdLine(const int argc, const char *const *const argv) {
  for(int i = 1; i < argc; i++) {
    const std::string arg = argv[i];

    if(arg == "--stderr") {
      return true;
    }
  }

  return false;
}

/**
 * Replaces all occurrences in a string "str" of a substring "from" with the string "to"
 * 
 * @param str  The original string
 * @param from The substring to replace
 * @param to   The replacement string
 */
void replaceAll(std::string& str, const std::string& from, const std::string& to){
  if(from.empty() || str.empty())
    return;
  size_t start_pos = 0;
  while((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
}

/**
 * Encodes a string in base 64 and replaces slashes ('/') in the result
 * with underscores ('_').
 * 
 * @param msg string to encode
 * @return encoded string
 */
std::string encode(const std::string msg) {
  std::string ret;
  const bool noNewLineInBase64Output = false;
  CryptoPP::StringSource ss1(msg, true, new CryptoPP::Base64Encoder(new CryptoPP::StringSink(ret), noNewLineInBase64Output));

  // need to replace slashes ('/') with underscores ('_') because xroot removes
  // consecutive slashes, and the cryptopp base64 algorithm may produce
  // consecutive slashes. This is solved in cryptopp-5.6.3 (using
  // Base64URLEncoder instead of Base64Encoder) but we currently have
  // cryptopp-5.6.2. To be changed in the future...
  replaceAll(ret, "/", "_");

  return ret;
}

/**
 * Formats the command path string
 *
 * @param argc The number of command-line arguments.
 * @param argv The command-line arguments. 
 * @return the command string
 */
std::string formatCommandPath(const int argc, const char **argv) {
  cta::cmdline::Configuration cliConf("/etc/cta/cta-cli.conf");
  std::string cmdPath = "root://"+cliConf.getFrontendHostAndPort()+"//";

  for(int i=0; i<argc; i++) {
    if(i) cmdPath += "&";
    cmdPath += encode(std::string(argv[i]));
  }
  return cmdPath;
}

/**
 * Sends the command and waits for the reply
 *
 * @param argc The number of command-line arguments.
 * @param argv The command-line arguments. 
 * @return the return code
 */
int sendCommand(const int argc, const char **argv) {
  int rc = 0;
  const bool writeToStderr = stderrIsOnTheCmdLine(argc, argv);
  const std::string cmdPath = formatCommandPath(argc, argv);
  XrdCl::File xrootFile;

  // Open the xroot file reprsenting the execution of the command
  {
    const XrdCl::Access::Mode openMode = XrdCl::Access::None;
    const uint16_t openTimeout = 15; // Timeout in seconds that is rounded up to the nearest 15 seconds
    const XrdCl::XRootDStatus openStatus = xrootFile.Open(cmdPath, XrdCl::OpenFlags::Read, openMode, openTimeout);
    if(!openStatus.IsOK()) {
      throw std::runtime_error(std::string("Failed to open ") + cmdPath + ": " + openStatus.ToStr());
    }
  }

  // The cta frontend return code is the first char of the answer
  {
    uint64_t readOffset = 0;
    uint32_t bytesRead = 0;
    char rc_char = '0';
    const XrdCl::XRootDStatus readStatus = xrootFile.Read(readOffset, 1, &rc_char, bytesRead);
    if(!readStatus.IsOK()) {
      throw std::runtime_error(std::string("Failed to read first byte from ") + cmdPath + ": " +
        readStatus.ToStr());
    }
    if(bytesRead != 1) {
      throw std::runtime_error(std::string("Failed to read first byte from ") + cmdPath +
        ": Expected to read exactly 1 byte, actually read " +
        std::to_string((long long unsigned int)bytesRead) + " bytes");
    }
    rc = rc_char - '0';
  }

  // Read and print the command result
  {
    uint64_t readOffset = 1; // The first character at offset 0 has already been read
    uint32_t bytesRead = 0;
    do {
      bytesRead = 0;
      char buf[1024];
      memset(buf, 0, sizeof(buf));
      const XrdCl::XRootDStatus readStatus = xrootFile.Read(readOffset, sizeof(buf) - 1, buf, bytesRead);
      if(!readStatus.IsOK()) {
        throw std::runtime_error(std::string("Failed to read ") + cmdPath + ": " + readStatus.ToStr());
      }

      if(bytesRead > 0) {
        std::cout << buf;

        if(writeToStderr) {
          std::cerr<<buf;
        }
      }

      readOffset += bytesRead;
    } while(bytesRead > 0);
  }

  // Close the xroot file reprsenting the execution of the command
  {
    const XrdCl::XRootDStatus closeStatus = xrootFile.Close();
    if(!closeStatus.IsOK()) {
      throw std::runtime_error(std::string("Failed to close ") + cmdPath + ": " + closeStatus.ToStr());
    }
  }

  return rc;
}

/**
 * The entry function of the command.
 *
 * @param argc The number of command-line arguments.
 * @param argv The command-line arguments.
 */
int main(const int argc, const char **argv) {
  try {    
    return sendCommand(argc, argv);
  } catch (std::exception &ex) {
    std::cerr << "Failed to execute the command. Reason: " << ex.what() << std::endl;
    return cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry;
  } catch (...) {
    std::cerr << "Failed to execute the command for an unknown reason" << std::endl;
    return cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry;
  }
}
