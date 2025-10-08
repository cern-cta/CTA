/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <filesystem>
#include <vector>

#include "cmdline/standalone_cli_tools/common/CmdLineTool.hpp"
#include "cmdline/standalone_cli_tools/eos_namespace_injection/MetaData.hpp"
#include "xrootd-ssi-protobuf-interface/eos_cta/include/CtaFrontendApi.hpp"

namespace {
using cid = uint64_t;
using uid = uint64_t;
using gid = uint64_t;

using ArchiveId = uint64_t;
using Checksum = std::string;
}

namespace eos::client { class EndpointMap;  }
namespace cta::log    { class StdoutLogger; }

namespace cta::cliTool {
class CmdLineArgs;

class EosNamespaceInjection final: public CmdLineTool {
  public:
    /**
    * Constructor.
    *
    * @param inStream Standard input stream.
    * @param outStream Standard output stream.
    * @param errStream Standard error stream.
    * @param log The object representing the API of the CTA logging system.
    */
    EosNamespaceInjection(std::istream &inStream, std::ostream &outStream,
      std::ostream &errStream, cta::log::StdoutLogger &log);

    /**
    * Destructor
    */
    ~EosNamespaceInjection() override;

    /**
    * An exception throwing version of main().
    *
    * @param argc The number of command-line arguments including the program name.
    * @param argv The command-line arguments.
    * @return The exit value of the program.
    */
    int exceptionThrowingMain(const int argc, char *const *const argv) override;

  private:
    /**
    * Returns the meta data from the catalogue
    * @param archiveId the archive file id to check
    */
    bool getMetaDataFromCatalogue(const uint64_t &archiveId) const;

    /**
    * Updates the disk file ID and the disk instance in the catalogue.
    * @param archiveId the archive file id to check
    * @param diskFileId The new diskFileID
    * @param diskInstance The new disk instance
    */
    void updateDiskFileIdAndDiskInstanceInCatalogue(const std::string &archiveId, const std::string &diskFileId, const std::string &diskInstance) const;

    /**
    * Compares the meta data from the provided json and from the Catalogue
    * @param metaDataJson Meta data from the provided json file
    * @param metaDataCatalogue Meta data from the catalogue
    */
    void compareJsonAndCtaMetaData(const MetaDataObject &metaDataJson, const MetaDataObject &metaDataCatalogue) const;

    /**
    * Returns cid, uid and gid
    * @param diskInstance the disk instance to check
    * @param path the path to check
    */
    std::tuple<cid, uid, gid> getContainerIdsEos(const std::string &diskInstance, const std::string &path) const;

    /**
    * Creates a new file in eos
    * @param metaDataObject metaData for the eos file
    * @param parentId the id of the parent container
    * @return the new fid
    */
    uint64_t createFileInEos(const MetaDataObject &metaDataObject, const uint64_t &parentId, const uint64_t uid, const uint64_t gid) const;

    /**
    * Returns the id of a given file in eos or zero if the files does not exist
    * @param diskInstance eos disk instance
    * @param path the path to check
    */
    uint64_t getFileIdEos(const std::string &diskInstance, const std::string &path) const;

    /**
    * Gets the archive file id and checksum from eos
    * @param diskInstance The eos disk instance
    * @param diskFileId The eos disk file id
    */
    std::pair<ArchiveId, Checksum> getArchiveFileIdAndChecksumFromEOS(const std::string& diskInstance, const std::string& diskFileId) const;

    /**
    * Validates the command line arguments
    * @param argc The number of command-line arguments including the program name.
    * @param argv The command-line arguments.
    */
    void setCmdLineArguments(const int argc, char *const *const argv);

    /**
    * Checks if path exists in EOS
    * @param fid EOS file id
    */
    bool pathExists(const uint64_t fid) const;

    /**
    * Checks consistency between EOS and CTA
    * @param archiveId CTA archive file id
    * @param fxId The eos file id
    * @param metaDataFromUser metaData for the eos file
    */
    bool checkEosCtaConsistency(const uint64_t& archiveId, const std::string& newFxIdEos, const MetaDataObject &metaDataFromUser);

    /**
    * Checks if file was created in EOS
    * @param newFid EOS file id
    */
    void checkFileCreated(const uint64_t newFid);

    /**
    * Checks if parent container exists in EOS
    * @param parentId The id of the parent container in EOS
    * @param enclosingPath The full EOS path of the parent container
    */
    void checkParentContainerExists(const uint64_t parentId, const std::string& enclosingPath) const;

    /**
    * Checks if archive id exists
    * @param CTA archive file id
    */
    void checkArchiveIdExistsInCatalogue(const uint64_t &archiveId) const;

    /**
    * Throws error if existing path has invalid metadata
    * @param archiveId CTA archive file id
    * @param fid The eos file id
    * @param metaDataFromUser metaData for the eos file
    */
    void checkExistingPathHasInvalidMetadata(const uint64_t &archiveId, const uint64_t& fid, const MetaDataObject& metaDataFromUser);

    /**
    * Writes the skipped metadata for file to txt file
    */
    void createTxtFileWithSkippedMetadata() const;

    /**
    * Meta data from CTA catalogue
    */
    MetaDataObject m_metaDataObjectCatalogue;

    /**
    *Default file layout for restored files in EOS
    */
    int m_defaultFileLayout;

    /**
    * Path to the json file with eos meta data
    */
    std::filesystem::path m_jsonPath;

    /**
    * Disk instance of the destination path
    */
    std::string m_diskInstance;

    /**
    * The object representing the API of the CTA logging system.
    */
    cta::log::StdoutLogger &m_log;

    /**
    * When a file is skipped due to inconsistent meta data between EOS and CTA,
    * they are added to this vector
    */
    std::vector<MetaDataObject> m_inconsistentMetadata;

    /**
    * CTA Frontend service provider
    */
    std::unique_ptr<XrdSsiPbServiceType> m_serviceProviderPtr;

    std::unique_ptr<::eos::client::EndpointMap> m_endpointMapPtr;

};
} // namespace cta::cliTool
