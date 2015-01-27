#include "ArchiveCmd.hpp"

#include "XrdCl/XrdClFileSystem.hh"
#include "XrdOuc/XrdOucString.hh"

#include <getopt.h>
#include <iostream>
#include <string.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ArchiveCmd::ArchiveCmd() throw(): m_programName("xrd_pro_client") {
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void ArchiveCmd::parseCommandLine(const int argc, char **argv)  {
  if(argc < 3) {
    throw wrongArgumentsException();
  }
  m_cmdLine.dstPath = argv[argc-1];
  for(int i=1; i<argc-1; i++) {
    m_cmdLine.srcFiles.push_back(argv[i]);
  }
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void ArchiveCmd::usage(std::ostream &os) const throw() {
  os <<
    "Usage:\n"
    "\t" << m_programName << " <source_file> <destination_file>\n"
    "Or:\n"
    "\t" << m_programName << " <source_file1> [<source_file2> [<source_file3> [...]]] <destination_dir>\n"
    "Where:\n"
    "\t<source_fileX>      Full path of the source file(s)\n"
    "\t<destination_file>  Full path of the destination file\n"
    "\t<destination_dir>   Full path of the destination directory (must end with a /)\n";
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int ArchiveCmd::main(const int argc, char **argv) throw() {

  // Parse the command line
  try {
    parseCommandLine(argc, argv);
  } catch (std::exception &ex) {
    std::cerr
      << std::endl
      << "Failed to parse the command-line:\n\n"
      << ex.what() << std::endl
      << std::endl;
    usage(std::cerr);
    std::cerr << std::endl;
    return 1;
  }

  int rc = 1;
  
  // Execute the command
  try {
    rc = executeCommand();
  } catch(std::exception &ex) {
    std::cerr
      << std::endl
      << "Failed to execute the archive command:\n\n"
      << ex.what() << std::endl
      << std::endl;
    return 1;
  }

  return rc;
}

//------------------------------------------------------------------------------
// executeCommand
//------------------------------------------------------------------------------
int ArchiveCmd::executeCommand()  {
  
  XrdCl::FileSystem fs(XrdCl::URL("localhost"));
  std::string queryString = "/archive?";
    
  for(std::list<std::string>::iterator it = m_cmdLine.srcFiles.begin(); it != m_cmdLine.srcFiles.end(); it++) {
    queryString += *it;
    queryString += "+";
  }  
  queryString += m_cmdLine.dstPath;
          
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
