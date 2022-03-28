/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <iostream>
#include <XrdCl/XrdClFile.hh>

namespace cta {

/**
 * Command-line tool that modifies a file using the XRootD client API.
 */
class ImmutableFileTest {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   */
  ImmutableFileTest(std::istream &inStream, std::ostream &outStream, std::ostream &errStream);

  /**
   * The object's implementation of main() that should be called from the main()
   * of the program.
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int main(const int argc, char *const *const argv);

private:

  /**
   * Standard input stream.
   */
  std::istream &m_in;

  /**
   * Standard output stream.
   */
  std::ostream &m_out;

  /**
   * Standard error stream.
   */
  std::ostream &m_err;

  /**
   * An exception throwing version of main().
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv);

  /**
   * Asks the user to confirm that they want to destroy the specified file.
   *
   * @param fileUrl The URL of the file.
   * @return True if the user confirmed.
   */
  bool userConfirmsDestroyFile(const std::string &fileUrl) const;

  /**
   * @return True if the specified file exists
   * @param url The XRootD URL of the file to be tested.
   */
  bool fileExists(const XrdCl::URL &url);

  /**
   * Deletes the specified file.
   *
   * @param url The XRootD URL of the file to be deleted.
   */
  void deleteFile(const XrdCl::URL &url);

  /**
   * Tests the opening and closing of the specified file.
   *
   * WARNING: The specified file can be destroyed depending on the flags used to
   * open it.
   *
   * @param url The XRootD URL of the file to be tested.
   * @param openFlags The XRootD flags to be used when opening the file.
   * @param openMode The XRootD mode permissions to be used when opening the
   * file.
   * @param contents Optional contents to be written to the file.  An empty
   * string means don't write anything.
   * @param expectecErrNo The expected errNo result of opening the file.
   */
  void testFile(const XrdCl::URL &url, const XrdCl::OpenFlags::Flags openFlags, const XrdCl::Access::Mode openMode,
    const std::string &contents, const uint32_t expectedOpenErrNo);

  /**
   * @return The string representation of the specified XRootD "open file"
   * flags.
   * @param flags The XRootD "open file" flags.
   */
  std::string openFlagsToString(XrdCl::OpenFlags::Flags flags);

  /**
   * @return The string representation of the specified XRootD "open file"
   * mode permissions.
   * @param mode The XRootD "open file" mode permissions.
   */
  std::string openModeToString(XrdCl::Access::Mode mode);

  /**
   * @return The string representation of the specified XErrorCode.
   * @param code The XErrorCode.
   */
  std::string xErrorCodeToString(uint32_t code);

}; // class ImmutableFileTest

} // namespace cta
