#include "ArchiveQueue.hpp"

namespace cta::objectstore {

  void ArchiveQueueToReportForUser::setContainerId(const std::string &name) {
    checkPayloadWritable();
    m_payload.set_vid(name);
  };

  std::string ArchiveQueueToReportForUser::setContainerId() {
    checkPayloadReadable();
    return m_payload.vid();
  };

} // namespace cta::objectstore
