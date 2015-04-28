#pragma once

namespace cta {

/**
 * Class representing the API for archiving files.
 */
class ArchivalRequestHandler {
public:
  /**
   * Creates and queues a request to archive one or files.
   */
  void createArchivalRequest();
}; // class ArchivalRequestHandler

} // namespace cta
