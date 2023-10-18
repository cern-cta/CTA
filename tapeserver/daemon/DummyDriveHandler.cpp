#include "DummyDriveHandler.hpp"

namespace cta::tape::daemon::tests {

void DummyDriveHandler::requestKill() {
  m_processingStatus.killRequested = true;
}

void DummyDriveHandler::requestShutdown() {
  m_processingStatus.shutdownRequested = true;
}

void DummyDriveHandler::setProcessingStatus(
    SubprocessHandler::ProcessingStatus newProcStatus) {
  m_processingStatus.shutdownRequested = newProcStatus.shutdownRequested;
  m_processingStatus.shutdownComplete = newProcStatus.shutdownComplete;
  m_processingStatus.killRequested = newProcStatus.killRequested;
  m_processingStatus.forkRequested = newProcStatus.forkRequested;
  m_processingStatus.sigChild = newProcStatus.sigChild;
  m_processingStatus.forkState = newProcStatus.forkState;
}
} // namespace cta::tape::daemon::tests
