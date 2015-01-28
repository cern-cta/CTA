#pragma once

#include "XrdOuc/XrdOucString.hh"

#include <list>
#include <string>

/**
 * Data type used to store the results of parsing the create-storage-class command-line.
 */
struct ParsedChangeStorageClassCmdLine {

  /**
   * The directory name
   */
  std::string dirName;
  
  /**
   * The storage class name
   */
  std::string storageClassName;
  
  /**
   * Constructor.
   */
  ParsedChangeStorageClassCmdLine() throw();
  
}; // struct ParsedChangeStorageClassCmdLine

