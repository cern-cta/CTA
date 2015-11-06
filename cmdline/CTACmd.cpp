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

#include "castor/common/CastorConfiguration.hpp"
#include "cmdline/CTACmd.hpp"
#include "common/exception/Exception.hpp"

#include "XrdCl/XrdClCopyProcess.hh"

#include <cryptopp/base64.h>
#include <cryptopp/osrng.h>

#include <iostream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CTACopyCmd::CTACopyCmd() throw() {
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int CTACopyCmd::main(const int argc, const char **argv) const throw() {
  try {
    return sendCommand(argc, argv);
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

//------------------------------------------------------------------------------
// sendCommand
//------------------------------------------------------------------------------
int CTACopyCmd::sendCommand(const int argc, const char **argv) const {
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

//------------------------------------------------------------------------------
// formatCommandPath
//------------------------------------------------------------------------------
std::string CTACopyCmd::formatCommandPath(const int argc, const char **argv) const {
  std::string cmdPath = "root://"+castor::common::CastorConfiguration::getConfig().getConfEntString("TapeServer", "CTAFrontendHostAndPort")+"//";
  for(int i=0; i<argc; i++) {
    if(i) cmdPath += "&";
    cmdPath += encode(std::string(argv[i]));
  }
  return cmdPath;
}

//------------------------------------------------------------------------------
// replaceAll
//------------------------------------------------------------------------------
void CTACopyCmd::replaceAll(std::string& str, const std::string& from, const std::string& to) const {
  if(from.empty() || str.empty())
    return;
  size_t start_pos = 0;
  while((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length();
  }
}

//------------------------------------------------------------------------------
// encode
//------------------------------------------------------------------------------
std::string CTACopyCmd::encode(const std::string msg) const {
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
