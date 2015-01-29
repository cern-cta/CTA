#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta { namespace objectstore {

  
  class AgentVisitor: private ObjectOps<serializers::Agent> {
    
  };
  
}}