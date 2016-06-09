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

#include "common/Configuration.hpp"
#include "common/exception/Exception.hpp"

#include "XrdCl/XrdClCopyProcess.hh"

#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>

#include <iostream>
#include <future>
#include <chrono>

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
 * Encodes a string in base 64 and replaces slatches ('/') in the result
 * with underscores ('_').
 *
 * Need to replace slashes ('/') with underscores ('_') because xroot removes
 * consecutive slashes, and the cryptopp base64 algorithm may produce
 * consecutive slashes. This is solved in cryptopp-5.6.3 (using
 * Base64URLEncoder instead of Base64Encoder) but we currently have
 * cryptopp-5.6.2. To be changed in the future...
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
  cta::common::Configuration ctaConf("/etc/cta/cta-frontend.conf");  
  std::string cmdPath = "root://"+ctaConf.getConfEntString("Frontend", "HostAndPort", NULL)+"//";
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
  XrdCl::PropertyList properties;
  properties.Set("source", formatCommandPath(argc, argv));
  properties.Set("target", "-"); //destination is stdout
  XrdCl::PropertyList results;
  XrdCl::CopyProcess copyProcess;
  
  XrdCl::XRootDStatus status = copyProcess.AddJob(properties, &results);
  if(!status.IsOK())
  {
    throw cta::exception::Exception(status.ToStr());
  }
  
  status = copyProcess.Prepare();
  if(!status.IsOK())
  {
    throw cta::exception::Exception(status.ToStr());
  }
  
  XrdCl::CopyProgressHandler copyProgressHandler;
  status = copyProcess.Run(&copyProgressHandler);
  if(!status.IsOK())
  {
    throw cta::exception::Exception(status.ToStr());
  }
  return 0;
}

/**
 * The entry function of the command.
 *
 * @param argc The number of command-line arguments.
 * @param argv The command-line arguments.
 */
int main(const int argc, const char **argv) {
  try {
    std::chrono::system_clock::time_point five_secs_passed = std::chrono::system_clock::now() + std::chrono::seconds(5);
    // call function asynchronously:
    std::future<int> fut = std::async(std::launch::async, sendCommand, argc, argv);
    if(std::future_status::ready == fut.wait_until(five_secs_passed)) {
      return fut.get();
    }
    else {
      std::cout<<"Timeout!\n";      
      exit(1);
    }
  } catch(cta::exception::Exception &ex) {
    std::cerr << "Failed to execute the command. Reason: " << ex.getMessageValue() << std::endl;
    return 1;
  } catch (std::exception &ex) {
    std::cerr << "Failed to execute the command. Reason: " << ex.what() << std::endl;
    return 1;
  } catch (...) {
    std::cerr << "Failed to execute the command for an unknown reason" << std::endl;
    return 1;
  }
}
