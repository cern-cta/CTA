#include "CTACmd.hpp"

#include "XrdCl/XrdClFileSystem.hh"
#include "XrdOuc/XrdOucString.hh"

#include <getopt.h>
#include <iostream>
#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CTACmd::CTACmd() throw(): m_programName("CTA_cmd") {
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void CTACmd::usage(std::ostream &os) const throw() {
  os <<
    "Usage:\n"
    "\t" << m_programName << " archive <source_file1> [<source_file2> [<source_file3> [...]]] <destination_path>\n";
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int CTACmd::main(const int argc, char **argv) throw() {
  if(argc < 3) {
    usage(std::cerr);
    return 1;
  }
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
int CTACmd::executeCommand(const int argc, char **argv)  {
  
  XrdCl::FileSystem fs(XrdCl::URL("localhost"));
  std::string queryString = "/";
  queryString += argv[1];
  queryString += "?";  
  for(int i=2; i<argc-1; i++) {
    queryString += argv[i];
    queryString += "+";
  }  
  queryString += argv[argc-1];
          
  XrdCl::Buffer arg;
  arg.FromString(queryString.c_str());
  XrdCl::Buffer* response = 0;
  XrdCl::XRootDStatus status = fs.Query(XrdCl::QueryCode::Opaque, arg, response);
  if(status.IsOK())
  {
    std::string sresult = response->GetBuffer();
    std::cout << "Result string: " << sresult << std::endl;
  }
  else
  {
    std::cout << "Query error" << std::endl;
  }
  return 0;
}
