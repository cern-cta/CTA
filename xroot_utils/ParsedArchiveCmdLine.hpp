#pragma once

#include "XrdOuc/XrdOucString.hh"

#include <list>
#include <string>

/**
 * Data type used to store the results of parsing the archive command-line.
 */
struct ParsedArchiveCmdLine {

  /**
   * The source files to be archived
   */
  std::list<std::string> srcFiles;
  
  /**
   * The destination path (in the tape namespace)
   */
  std::string dstPath;
  
  /**
   * Constructor.
   */
  ParsedArchiveCmdLine() throw();
  
}; // struct ParsedArchiveCmdLine

