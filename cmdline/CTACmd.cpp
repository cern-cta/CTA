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

#include "cmdline/CTACmd.hpp"
#include "common/exception/Exception.hpp"

#include "XrdCl/XrdClCopyProcess.hh"

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
  
  int rc = 1;
  
  // Execute the command
  try {
    rc = sendCommand(argc, argv);
  } catch(std::exception &ex) {
    std::cerr << std::endl << "Failed to execute the command. Reason: " << ex.what() << std::endl;
    return 1;
  }

  return rc;
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
    throw cta::exception::Exception("Job adding error");
  }
  
  status = copyProcess.Prepare();
  if(!status.IsOK())
  {
    throw cta::exception::Exception("Job preparing error");
  }
  
  XrdCl::CopyProgressHandler copyProgressHandler;
  status = copyProcess.Run(&copyProgressHandler);
  if(!status.IsOK())
  {
    throw cta::exception::Exception("Copy process run error");
  }
  return 0;
}

//------------------------------------------------------------------------------
// formatCommandPath
//------------------------------------------------------------------------------
std::string CTACopyCmd::formatCommandPath(const int argc, const char **argv) const {
  std::string cmdPath = "root://localhost//";
  std::string arg = argv[0];
  replaceAll(arg, "&", "_#_and_#_");
  cmdPath += arg;
  for(int i=1; i<argc; i++) {
    std::string arg = argv[i];
    replaceAll(arg, "&", "_#_and_#_");
    cmdPath += "&";
    cmdPath += arg;
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