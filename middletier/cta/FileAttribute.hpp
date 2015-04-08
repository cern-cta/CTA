#pragma once

#include <string>

namespace cta {

/**
 * Class representing a file atrribute.
 */
struct FileAttribute {

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
  FileAttribute();

  /**
   * Constructor.
   *
   * @param name The name of the attribute.
   * @param value value of the attribute.
   */
  FileAttribute(const std::string &name, const std::string &value);

}; // struct FileAttribute

} // namespace cta
