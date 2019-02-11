#include "RepackQueueAlgorithms.hpp"

namespace cta { namespace objectstore {
  template<>
  const std::string ContainerTraits<RepackQueue,RepackQueueToExpand>::c_containerTypeName = "RepackQueueToExpand";
  
  template<>
  const std::string ContainerTraits<RepackQueue,RepackQueueToExpand>::c_identifierType = "uniqueQueue";

  template<>
  auto ContainerTraits<RepackQueue,RepackQueueToExpand>::getContainerSummary(Container &cont) -> ContainerSummary
  {
    ContainerSummary ret;
    ret.requests = cont.getRequestsSummary().requests;
    return ret;
  }
  
}}