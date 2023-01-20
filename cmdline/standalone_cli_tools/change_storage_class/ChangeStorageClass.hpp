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

#include "cmdline/standalone_cli_tools/common/CmdLineTool.hpp"
#include "eos_grpc_client/GrpcEndpoint.hpp"

#include <optional>
#include <memory>
#include <vector>

#include "CtaFrontendApi.hpp"

namespace cta {

namespace log {
class StdoutLogger;
}

namespace cliTool {
class CmdLineArgs;

class ChangeStorageClass: public CmdLineTool {
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
  ChangeStorageClass(
    std::istream &inStream,
    std::ostream &outStream,
    std::ostream &errStream,
    cta::log::StdoutLogger &log);

private:

  /**
   * The object representing the API of the CTA logging system.
   */
  cta::log::StdoutLogger &m_log;

  /**
   * Archive file ids of the files to change
   */
  std::vector<std::string> m_archiveFileIds;

  /**
   * Archive file id of the files to change
   */
  std::string m_storageClassName;

  /**
   * CTA Frontend service provider
   */
  std::unique_ptr<XrdSsiPbServiceType> m_serviceProviderPtr;

  /**
   * Endpoint map for interaction with EOS
   */
  std::unique_ptr<::eos::client::EndpointMap> m_endpointMapPtr;

  /**
   * Endpoint map for interaction with EOS
   */
  std::vector<std::string> m_archiveIdsNotUpdatedInEos;

  /**
   * Endpoint map for interaction with EOS
   */
  std::vector<std::string> m_archiveIdsUpdatedInEos;

  /**
   * Endpoint map for interaction with EOS
   */
  uint64_t m_eosUpdateFrequency;

  /**
   *  Updates the storage class name in the EOS namespace
  */
  void updateStorageClassInEosNamespace();

  /**
   *  Checks if the storage class provided to the tool is defined,
   * and throws an exception::UserError if it is not found
  */
  void storageClassExists() const;

  /**
  * Updates the storage class name in the catalogue
  */
  void updateStorageClassInCatalogue() const;

  /**
  * Writes the skipped archive ids to file
  */
  void writeSkippedArchiveIdsToFile() const;

  /**
   * Checks if a file is in flight
  */
  bool fileInFlight(const google::protobuf::RepeatedField<unsigned int> &locations) const;

  /**
  * Fills the member variables with data, based on the arguments that were provided
  */
  void handleArguments(const CmdLineArgs &cmdLineArgs);

  /**
   * An exception throwing version of main().
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv) override;


} ; // class CtaChangeStorageClass

} // namespace admin
} // namespace cta
