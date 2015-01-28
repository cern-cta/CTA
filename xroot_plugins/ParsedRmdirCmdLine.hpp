#pragma once

#include "XrdOuc/XrdOucString.hh"

#include <list>
#include <string>

/**
 * Data type used to store the results of parsing the rmdir command-line.
 */
struct ParsedRmdirCmdLine {

  /**
   * The directory name
   */
  std::string dirName;
  
  /**
   * Constructor.
   */
  ParsedRmdirCmdLine() throw();
  
}; // struct ParsedRmdirCmdLine

