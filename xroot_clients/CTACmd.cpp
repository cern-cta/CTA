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
    "\t" << m_programName << " archive <source_file1> [<source_file2> [<source_file3> [...]]] <destination_path>\n"
    "\t" << m_programName << " create-storage-class <storage_class_name> <number_of_tape_copies>\n"
    "\t" << m_programName << " change-storage-class <directory_name> <storage_class_name>\n"
    "\t" << m_programName << " delete-storage-class <storage_class_name>\n"
    "\t" << m_programName << " list-storage-class\n"
    "\t" << m_programName << " mkdir <directory_name>\n"
    "\t" << m_programName << " rmdir <directory_name>\n";
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int CTACmd::main(const int argc, char **argv) throw() {
  if(argc < 2) {
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
    std::cout << response->GetBuffer() << std::endl;
  }
  else
  {
    std::cout << "Query error" << std::endl;
  }
  return 0;
}
