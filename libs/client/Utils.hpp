#pragma once

#include <string>
#include <vector>

namespace cta {

/**
 * Class of utility methods.
 */
class Utils {
public:

  /**
   * Throws an exception if the specified absolute path constains a
   * syntax error.
   *
   * @param path The Absolute path.
   */
  static void checkAbsolutePathSyntax(const std::string &path);

  /**
   * Returns the path of the enclosing directory of the specified path.
   *
   * For example:
   *
   * * path="/grandparent/parent/child" would return "/grandparent/parent"
   * * path="/grandparent" would return "/grandparent"
   * * path="/" would return "" where empty string means no enclosing directoyr
   *
   * @param path The path.
   * @return The path of the enclosing directory.
   */
  static std::string getEnclosingDirPath(const std::string &path);

  /**
   * Returns the name of the enclosed file or directory of the specified path.
   *
   * @param path The path.
   * @return The name of the enclosed file or directory.
   */
  static std::string getEnclosedName(const std::string &path);

  /**
   * Returns the result of trimming both left and right slashes from the
   * specified string.
   *
   * @param s The string to be trimmed.
   * @return The result of trimming the string.
   */
  static std::string trimSlashes(const std::string &s);

  /**
   * Splits the specified string into a vector of strings using the specified
   * separator.
   *
   * Please note that the string to be split is NOT modified.
   *
   * @param str The string to be split.
   * @param separator The separator to be used to split the specified string.
   * @param result The vector when the result of spliting the string will be
   * stored.
   */
  static void splitString(const std::string &str, const char separator,
    std::vector<std::string> &result);

protected:

  /**
   * Throws an exception if the specified path does not start with a slash.
   *
   * @param path The path.
   */
  static void checkPathStartsWithASlash(const std::string &path);

  /**
   * Throws an exception if the specified path does not contain valid
   * characters.
   *
   * @param path The path.
   */
  static void checkPathContainsValidChars(const std::string &path);

  /**
   * Throws an exception if the specified character cannot be used within a
   * path.
   *
   * @param c The character to be tested.
   */
  static void checkValidPathChar(const char c);

  /**
   * Returns true of the specified character can be used within a path.
   */
  static bool isValidPathChar(const char c);

  /**
   * Throws an exception if the specified path contains consective slashes.  For
   * example the path "/just_before_consectuive_slashes//file" would cause this
   * method to throw an exception.
   *
   * @param path The path.
   */
  static void checkPathDoesContainConsecutiveSlashes(const std::string &path);

}; // class Utils

} // namespace cta
