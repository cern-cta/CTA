#pragma once

namespace cta {

/**
 * The possible states of an archival job.
 */
class ArchivalJobState {
public:

  /**
   * Enumeration of the possible states of an archival job.
   */
  enum Enum {
    NONE,
    PENDING,
    SUCCESS,
    ERROR};

  /**
   * Thread safe method that returns the string representation of the
   * enumeration value.
   *
   * @param enumValue The enumeration value.
   * @return The string representation.
   */
  static const char *toStr(const Enum enumValue) throw();

}; // class ArchivalJobState

} // namespace cta
