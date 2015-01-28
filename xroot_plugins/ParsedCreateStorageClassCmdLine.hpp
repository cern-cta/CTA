#pragma once

#include "XrdOuc/XrdOucString.hh"

#include <list>
#include <string>

/**
 * Data type used to store the results of parsing the create-storage-class command-line.
 */
struct ParsedCreateStorageClassCmdLine {

  /**
   * The storage class name
   */
  std::string storageClassName;
  
  /**
   * The number of copies on tape
   */
  int numberOfCopies;
  
  /**
   * Constructor.
   */
  ParsedCreateStorageClassCmdLine() throw();
  
}; // struct ParsedCreateStorageClassCmdLine

