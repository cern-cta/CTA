/*!
 *
 * @project        The CERN Tape Archive (CTA)
 * @brief          Command-line tool to test EOS-CTA interface
 * @description    Proof-of-concept stub to combine Google Protocol Buffers and XRootD SSI transport
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

#include <iostream>
#include <stdexcept>

#include <google/protobuf/util/json_util.h>
#include "EosCtaApi.h"

#include "common/dataStructures/FrontendReturnCode.hpp"



const std::runtime_error Usage("Usage: eoscta_stub archive|retrieve|delete [options] [--stderr]");



/*!
 * Convert protocol buffer to JSON (for debug output)
 *
 * @param     message    A Google protocol buffer
 * @returns   The PB converted to JSON format
 */

static std::string MessageToJsonString(const google::protobuf::Message &message)
{
   using namespace google::protobuf::util;

   std::string output;
   JsonPrintOptions options;

   options.add_whitespace = true;
   options.always_print_primitive_fields = true;

   MessageToJsonString(message, &output, options);    // returns util::Status

   return output;
}



/*!
 * Returns true if --stderr is on the command-line.
 *
 * @param argc The number of command-line arguments.
 * @param argv The command-line arguments.
 */

static bool stderrIsOnTheCmdLine(int argc, const char *const *const argv)
{
  for(int i = 1; i < argc; i++)
  {
    const std::string arg(argv[i]);

    if(arg == "--stderr") return true;
  }

  return false;
}



#if 0
#include "cmdline/Configuration.hpp"
#include "common/Configuration.hpp"

#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <xrootd/XrdCl/XrdClFile.hh>

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
    const size_t bufSize = 20480;
    std::unique_ptr<char []> buf(new char[bufSize]);
    do {
      bytesRead = 0;
      memset(buf.get(), 0, bufSize);
      const XrdCl::XRootDStatus readStatus = xrootFile.Read(readOffset, bufSize - 1, buf.get(), bytesRead);
      if(!readStatus.IsOK()) {
        throw std::runtime_error(std::string("Failed to read ") + cmdPath + ": " + readStatus.ToStr());
      }

      if(bytesRead > 0) {
        std::cout << buf.get();

        if(writeToStderr) {
          std::cerr << buf.get();
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
#endif

int exceptionThrowingMain(int argc, const char *const *const argv)
{
   // Verify that the version of the Google Protocol Buffer library that we linked against is
   // compatible with the version of the headers we compiled against

   GOOGLE_PROTOBUF_VERIFY_VERSION;

   // Parse which workflow action to execute

   if(argc < 2) throw Usage;

   const std::string wf_command(argv[1]);

   if(wf_command == "retrieve" || wf_command == "delete") throw std::runtime_error(wf_command + " is not implemented yet.");

   if(wf_command != "archive") throw Usage;

   // Fill the protocol buffer from the command line arguments

   eos::wfe::Notification notification;

   // Output the protocol buffer as a JSON object (for debugging)

   std::cout << MessageToJsonString(notification);

#if 0
      // Obtain a Service Provider

      XrdSsiPbServiceType test_ssi_service(host, port, resource);

      // Create a Request object

      xrdssi::test::Request request;

      request.set_message_text("Archive some file");

      // Output message in Json format

      std::cout << "Sending message:" << std::endl;
      std::cout << MessageToJsonString(request);

      // Send the Request to the Service

      test_ssi_service.send(request);

      // Wait for the response callback.

      std::cout << "Request sent, going to sleep..." << std::endl;

      // When test_ssi_service goes out-of-scope, the Service will try to shut down, but will wait
      // for outstanding Requests to be processed

   int wait_secs = 5;

   while(--wait_secs)
   {
      std::cerr << ".";
      sleep(1);
   }

   std::cout << "All done, exiting." << std::endl;
#endif

   // Send output to stdout or stderr?

   std::ostream &myout = stderrIsOnTheCmdLine(argc, argv) ? std::cerr : std::cout;

   myout << "Hello, world" << std::endl;

   // Optional: Delete all global objects allocated by libprotobuf

   google::protobuf::ShutdownProtobufLibrary();

   return 0;
}



/*!
 * Start here
 *
 * @param argc The number of command-line arguments
 * @param argv The command-line arguments
 */

int main(int argc, const char **argv)
{
   try {    
      return exceptionThrowingMain(argc, argv);
   } catch (std::exception &ex) {
      std::cerr << "Failed to execute the command. Reason: " << ex.what() << std::endl;
      return cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry;
   } catch (...) {
      std::cerr << "Failed to execute the command for an unknown reason" << std::endl;
      return cta::common::dataStructures::FrontendReturnCode::ctaErrorNoRetry;
   }
}

