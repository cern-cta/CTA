/**
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

#include "CTACopyCmd.hpp"

#include "XrdCl/XrdClFileSystem.hh"
#include "XrdCl/XrdClCopyProcess.hh"
#include "XrdOuc/XrdOucString.hh"

#include <getopt.h>
#include <iostream>
#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CTACopyCmd::CTACopyCmd() throw(): m_programName("CTA_copycmd") {
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void CTACopyCmd::usage(std::ostream &os) const throw() {
  os <<
    "Usage:\n"
    "\t" << m_programName << " ls <directory_name>\n";
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int CTACopyCmd::main(const int argc, char **argv) throw() {
//  if(argc < 2) {
//    usage(std::cerr);
//    return 1;
//  }
  int rc = 1;
  
  // Execute the command
  try {
    rc = executeCommand(argc, argv);
  } catch(std::exception &ex) {
    std::cerr << std::endl << "Failed to execute the archive command:\n\n" << ex.what() << std::endl;
    return 1;
  }

  return rc;
}

//------------------------------------------------------------------------------
// executeCommand
//------------------------------------------------------------------------------
int CTACopyCmd::executeCommand(const int argc, char **argv)  {
  XrdCl::PropertyList properties;
  properties.Set("source", "root://localhost//himama");
  properties.Set("target", "/afs/cern.ch/user/d/dkruse/lola.txt"); 
  properties.Set("force", true);
  XrdCl::PropertyList results;
  XrdCl::CopyProcess copyProcess;
  
  XrdCl::XRootDStatus status = copyProcess.AddJob(properties, &results);
  if(status.IsOK())
  {
    std::cout << "Job added" << std::endl;
  }
  else
  {
    std::cout << "Job adding error" << std::endl;
  }
  
  status = copyProcess.Prepare();
  if(status.IsOK())
  {
    std::cout << "Job prepared" << std::endl;
  }
  else
  {
    std::cout << "Job preparing error" << std::endl;
  }
  
  XrdCl::CopyProgressHandler copyProgressHandler;
  status = copyProcess.Run(&copyProgressHandler);
  if(status.IsOK())
  {
    std::cout << "Copy OK" << std::endl;
  }
  else
  {
    std::cout << "Copy error" << std::endl;
  }
  return 0;
}
