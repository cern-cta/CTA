#include "ArchiveQueue.hpp"

class ArchiveQueueToReportForUser: public ArchiveQueue {
  using ArchiveQueue::ArchiveQueue;

  void setContainerId(const std::string &name) {
    checkPayloadWritable();
    m_payload.set_vid(name);
  };

  std::string getContainerId() {
    checkPayloadReadable();
    return m_payload.vid();
  };

};
