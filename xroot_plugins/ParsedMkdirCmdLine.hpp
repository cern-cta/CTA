#pragma once

#include "XrdOuc/XrdOucString.hh"

#include <list>
#include <string>

/**
 * Data type used to store the results of parsing the mkdir command-line.
 */
struct ParsedMkdirCmdLine {

  /**
   * The directory name
   */
  std::string dirName;
  
  /**
   * Constructor.
   */
  ParsedMkdirCmdLine() throw();
  
}; // struct ParsedMkdirCmdLine

