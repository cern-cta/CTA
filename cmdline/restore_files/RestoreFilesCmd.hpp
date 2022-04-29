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

#include "cmdline/restore_files/CmdLineTool.hpp"
#include "cmdline/restore_files/RestoreFilesCmdLineArgs.hpp"
#include "eos_grpc_client/GrpcEndpoint.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/log/StdoutLogger.hpp"

#include <optional>
#include <memory>

#include "CtaFrontendApi.hpp"

namespace cta {
namespace admin {

class RestoreFilesCmd: public CmdLineTool {
public:
  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param log The object representing the API of the CTA logging system.
   */
  RestoreFilesCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, cta::log::StdoutLogger &log);

  /**
   * An exception throwing version of main().
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv) override;

  /**
   * Sets internal configuration parameters to be used for reading.
   * It reads cta frontend parameters from /etc/cta/cta-cli.conf
   *
   * @param username The name of the user running the command-line tool.
   * @param cmdLineArgs The arguments parsed from the command line.
   */
  void readAndSetConfiguration(const std::string &userName, const RestoreFilesCmdLineArgs &cmdLineArgs);

  /**
   * Populate the namespace endpoint configuration from a keytab file
   */
  void setNamespaceMap(const std::string &keytab_file);

  /**
   * Restores the specified deleted files in the cta catalogue
   */
  void listDeletedFilesCta() const;

  /**
   * Queries the eos mgm for the current eos file and container id
   * Must be called before any other call to EOS, to initialize the grpc
   * client cid and fid
   * @param diskInstance the disk instance of the eos instance
   */
  void getCurrentEosIds(const std::string &diskInstance) const;

  /**
   * Restores the deleted file present in the cta catalogue recycle bin
   * @param file the deleted tape file
   */
  void restoreDeletedFileCopyCta(const RecycleTapeFileLsItem &file) const;

  /**
   * Adds a container in the eos namespace, if it does not exist
   * @param diskInstance eos disk instance
   * @param path the path of the container
   * @param sc the storage class of the container
   * @returns the container id of the container identified by path
   */
  int addContainerEos(const std::string &diskInstance, const std::string &path, const std::string &sc) const;

  /**
   * Returns true (i.e. not zero) if a container exists in the eos namespace
   * @param diskInstance eos disk instance
   * @param path the path of the container
   */
  int containerExistsEos(const std::string &diskInstance, const std::string &path) const;

  /**
   * Returns true (i.e. not zero) if a file with given id exists in the eos namespace
   * @param diskInstance eos disk instance
   * @param diskFileId the eos file id to check
   */
  bool fileExistsEos(const std::string &diskInstance, const std::string &diskFileId) const;

  /**
   * Returns true (i.e. not zero) if a file was deleted using cta-admin tf rm
   * @param diskInstance eos disk instance
   * @param diskFileId the eos file id to check
   */
  bool fileWasDeletedByRM(const RecycleTapeFileLsItem &file) const;

  /**
   * Returns true (i.e. not zero) if an archive file with given id exists in the cta catalogue
   * @param archiveFileId the archive file id to check
   */
  bool archiveFileExistsCTA(const uint64_t &archiveFileId) const;

  /**
   * Returns the id of a given file in eos or zero if the files does not exist
   * @param diskInstance eos disk instance
   * @param path the path to check
   */
  int getFileIdEos(const std::string &diskInstance, const std::string &path) const;

  /**
   * Restores the deleted file present in the eos namespace
   * @param file the deleted tape file
   */
  uint64_t restoreDeletedFileEos(const RecycleTapeFileLsItem &file) const;

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  void printUsage(std::ostream &os) override;

private:
  /**
   * The object representing the API of the CTA logging system.
   */
  cta::log::StdoutLogger &m_log;

  /**
   * True if the command should just restore deleted tape file copies
   */
  bool m_restoreCopies;

  /**
   * True if the command should just restore deleted archive files
   */
  bool m_restoreFiles;

  /**
   * Archive file id of the files to restore
   */
  std::optional<uint64_t> m_archiveFileId;

  /**
   * Disk instance of the files to restore
   */
  std::optional<std::string> m_diskInstance;

  /**
   * Fxids of the files to restore
   */
  std::optional<std::list<std::string>> m_eosFxids;

  /**
   * Vid of the tape of the files to restore
   */
  std::optional<std::string> m_vid;

  /**
   *Copy number of the files to restore
   */
  std::optional<uint64_t> m_copyNumber;

  /**
   *Default file layout for restored files in EOS
   */
  int m_defaultFileLayout;

  /**
   * CTA Frontend service provider
   */
  std::unique_ptr<XrdSsiPbServiceType> m_serviceProviderPtr;

  std::unique_ptr<::eos::client::EndpointMap> m_endpointMapPtr;

   const std::string StreamBufferSize      = "1024";                  //!< Buffer size for Data/Stream Responses
   const std::string DefaultRequestTimeout = "10";                    //!< Default Request Timeout. Can be overridden by
                                                                      //!< XRD_REQUESTTIMEOUT environment variable.

   const std::string m_ctaVersion = CTA_VERSION;                      //!< The version of CTA
   const std::string m_xrootdSsiProtobufInterfaceVersion =            //!< The xrootd-ssi-protobuf-interface version (=tag)
      XROOTD_SSI_PROTOBUF_INTERFACE_VERSION;


} ; // class RestoreFilesCmd

} // namespace admin
} // namespace cta
