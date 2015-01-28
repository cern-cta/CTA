#pragma once

#include <string>

namespace cta {

/**
 * Class representing a file atrribute.
 */
struct FileAtribute {

  /**
   * The name of the attribute.
   */
  std::string name;

  /**
   * The value of the attribute.
   */
  std::string value;

  /**
   * Constructor.
   */
  FileAtribute();

  /**
   * Constructor.
   *
   * @param name The name of the attribute.
   * @param value value of the attribute.
   */
  FileAtribute(const std::string &name, const std::string &value);

}; // struct FileAtribute

} // namespace cta
