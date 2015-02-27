#pragma once

#include "ObjectOps.hpp"
#include "Agent.hpp"
#include "exception/Exception.hpp"
#include <cxxabi.h>

namespace cta { namespace objectstore {

class FIFO: public ObjectOps<serializers::FIFO> {
public:
  FIFO(const std::string & name, Backend & os):
  ObjectOps<serializers::FIFO>(os, name) {}
 
  void initialize();
  
  class FIFOEmpty: public cta::exception::Exception {
  public:
    FIFOEmpty(const std::string & context): cta::exception::Exception(context) {}
  };
  
  std::string peek(); 

  void pop();
  
  void push(std::string name);
  
  void pushIfNotPresent (std::string name);
  
  std::string dump();
  
  uint64_t size();
  
  std::list<std::string> getContent();

private:
  
  void compact();
  static const size_t c_compactionSize;
};

}}