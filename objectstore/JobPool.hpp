#pragma once

#include "ObjectOps.hpp"
#include "FIFO.hpp"

class JobPool: private ObjectOps<cta::objectstore::jobPool> {
public:
  JobPool(ObjectStore & os, const std::string & name, ContextHandle & context):
  ObjectOps<cta::objectstore::jobPool>(os, name) {
    cta::objectstore::jobPool jps;
    updateFromObjectStore(jps, context);
  }
  
  void PostRecallJob (const std::string & string, ContextHandle & context) {
    cta::objectstore::jobPool jps;
    lockExclusiveAndRead(jps, context);
  }
  
private:
  // The following functions are hidden from the user in order to provide
  // higher level functionnality
  //std::string get
  
};