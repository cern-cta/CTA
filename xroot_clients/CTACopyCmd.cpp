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
  properties.Set("source", "/afs/cern.ch/user/d/dkruse/vmgrlisttape.txt2");
  properties.Set("target", "/afs/cern.ch/user/d/dkruse/vmgrlisttape.txt3");  
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
