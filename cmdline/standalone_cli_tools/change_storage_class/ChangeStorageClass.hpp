/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaFrontendApi.hpp"
#include "cmdline/standalone_cli_tools/common/CmdLineTool.hpp"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace eos::client {
class EndpointMap;
}

namespace cta::log {
class StdoutLogger;
}

namespace cta::cliTool {
class CmdLineArgs;

class ChangeStorageClass final : public CmdLineTool {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param log The object representing the API of the CTA logging system.
   * @param argc
   * @param argv
   */
  ChangeStorageClass(std::istream& inStream,
                     std::ostream& outStream,
                     std::ostream& errStream,
                     cta::log::StdoutLogger& log);

  /*
   * Destructor
   */
  ~ChangeStorageClass() override;

private:
  /**
   * The object representing the API of the CTA logging system.
   */
  cta::log::StdoutLogger& m_log;

  /**
   * Archive file ids of the files to change
   */
  std::vector<std::pair<std::string, std::string>> m_archiveFileIdsAndStorageClasses;

  /**
   * Archive file id of the files to change
   */
  std::string m_storageClassName;

  /**
   * CTA Frontend service provider
   */
  std::unique_ptr<XrdSsiPbServiceType> m_serviceProviderPtr;

  /**
   * Creates a set of the provided storage classes and checks that they exist
  */
  void storageClassesExist() const;

  /**
  * Checks if the storage classes provided to the tool is defined,
  * and throws an exception::UserError if it is not found
  * @param storageClass The new storage class
  */
  void storageClassExists(const std::string& storageClass) const;

  /**
  * Sets up the xrootd configuration
  * @param storageClass The new storage class
  */
  std::unique_ptr<XrdSsiPbServiceType> setXrootdConfiguration(bool debug = false) const;

  /**
  * Updates the storage class name in the catalogue
  * @param archiveFileId the archive file id to check
  * @param storageClass The new storage class
  */
  void updateStorageClassInCatalogue(const std::string& archiveFileId, const std::string& storageClass) const;

  /**
  * Fills the member variables with data, based on the arguments that were provided
  */
  void handleArguments(const CmdLineArgs& cmdLineArgs);

  /**
  * Checks that the provided archive id is related to the correct disk file id and disk instance
  * @param archiveFileId the archive file id to check
  * @param operatorProvidedFid The disk file id provided by the operator, either from a json file or an command line argument
  * @param operatorProvidedInstance The disk instance provided by the operator, either from a json file or an command line argument
  */
  bool validateUserInputFileMetadata(const std::string& archiveFileId,
                                     const std::string& operatorProvidedFid,
                                     const std::string& operatorProvidedInstance);

  /**
   * An exception throwing version of main().
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char* const* const argv) override;

};  // class CtaChangeStorageClass

}  // namespace cta::cliTool
