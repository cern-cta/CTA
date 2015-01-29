#pragma once

#include "XrdOuc/XrdOucString.hh"

#include <vector>
#include <string>

/**
 * Data type used to store the results of parsing the request.
 */
struct ParsedRequest {
  
  /**
   * The command name
   */
  std::string cmd;

  /**
   * The command arguments
   */
  std::vector<std::string> args;
  
  /**
   * Constructor.
   */
  ParsedRequest() throw();
  
}; // struct ParsedRequest

