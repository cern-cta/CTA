#pragma once

#include "XrdOuc/XrdOucString.hh"

#include <list>
#include <string>

/**
 * Data type used to store the results of parsing the delete-storage-class command-line.
 */
struct ParsedDeleteStorageClassCmdLine {

  /**
   * The storage class name
   */
  std::string storageClassName;
  
  /**
   * Constructor.
   */
  ParsedDeleteStorageClassCmdLine() throw();
  
}; // struct ParsedDeleteStorageClassCmdLine

