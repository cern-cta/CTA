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

#include <XrdSsi/XrdSsiEntity.hh>

#include "XrdSsiCtaServiceProvider.hpp"
#include "cta_frontend.pb.h"
#include "common/utils/utils.hpp"
#include "Versions.hpp"


namespace cta { namespace xrd {

/*!
 * CTA Frontend Request Message class
 */
class RequestMessage
{
public:
  RequestMessage(const XrdSsiEntity &client, const XrdSsiCtaServiceProvider *service) :
    m_service(*service),
    m_catalogue(service->getCatalogue()),
    m_scheduler(service->getScheduler()),
    m_archiveFileMaxSize(service->getArchiveFileMaxSize()),
    m_repackBufferURL(service->getRepackBufferURL()),
    m_verificationMountPolicy(service->getVerificationMountPolicy()),
    m_namespaceMap(service->getNamespaceMap()),
    m_lc(service->getLogContext()),
    m_catalogue_conn_string(service->getCatalogueConnectionString())
    {
      m_cliIdentity.username   = client.name;
      m_cliIdentity.host       = cta::utils::getShortHostname(); // Host should be of the machine that executes the command
      m_cliIdentity.clientHost = client.host;

      m_lc.pushOrReplace({"user", m_cliIdentity.username + "@" + m_cliIdentity.host});

      // Map the client protocol string to an enum value
      auto proto_it = m_protomap.find(client.prot);
      m_protocol = proto_it != m_protomap.end() ? proto_it->second : Protocol::OTHER;
    }

  /*!
   * Process a Notification request or an Admin command request
   *
   * @param[in]     request
   * @param[out]    response        Response protocol buffer
   * @param[out]    stream          Reference to Response stream pointer
   */
  void process(const cta::xrd::Request &request, cta::xrd::Response &response, XrdSsiStream* &stream);

  /*!
   * Get a required option
   */
  const std::string &getRequired(cta::admin::OptionString::Key key) const {
    return m_option_str.at(key);
  }
  const std::vector<std::string> &getRequired(cta::admin::OptionStrList::Key key) const {
    return m_option_str_list.at(key);
  }
  const uint64_t &getRequired(cta::admin::OptionUInt64::Key key) const {
    return m_option_uint64.at(key);
  }
  const bool &getRequired(cta::admin::OptionBoolean::Key key) const {
    return m_option_bool.at(key);
  }
  const Versions &getClientVersions() const {
    return m_client_versions;
  }
  const std::string &getClientXrdSsiProtoIntVersion() const {
    return m_client_xrd_ssi_proto_int_version;
  }

  /*!
   * Get an optional option
   *
   * The has_option parameter is set if the option exists and left unmodified if the option does not
   * exist. This is provided as a convenience to monitor whether at least one option was set from a
   * list of optional options.
   *
   * @param[in]     key           option key to look up in options
   * @param[out]    has_option    Set to true if the option exists, unmodified if it does not
   *
   * @returns       value of the option if it exists, an object of type std::nullopt_t if it does not
   */
  template<typename K, typename V>
  std::optional<V> getOptional(K key, const std::map<K,V> &options, bool *has_option) const
  {
    auto it = options.find(key);

    if(it != options.end()) {
      if(has_option != nullptr) *has_option = true;
      return it->second;
    } else {
      return std::nullopt;
    }
  }

  /*!
   * Overloaded versions of getOptional
   *
   * These map the key type to the template specialization of <key,value> pairs
   */
  std::optional<std::string> getOptional(cta::admin::OptionString::Key key, bool *has_option = nullptr) const {
    return getOptional(key, m_option_str, has_option);
  }
  std::optional<std::vector<std::string>> getOptional(cta::admin::OptionStrList::Key key, bool *has_option = nullptr) const {
    return getOptional(key, m_option_str_list, has_option);
  }
  std::optional<uint64_t> getOptional(cta::admin::OptionUInt64::Key key, bool *has_option = nullptr) const {
    return getOptional(key, m_option_uint64, has_option);
  }
  std::optional<bool> getOptional(cta::admin::OptionBoolean::Key key, bool *has_option = nullptr) const {
    return getOptional(key, m_option_bool, has_option);
  }

  /*!
   * Check if an optional flag has been set
   *
   * This is a simpler version of getOptional for checking flags which are either present
   * or not. In the case of flags, they should always have the value true if the flag is
   * present, but we do a redundant check anyway.
   *
   * @param[in] option    Optional command line option
   *
   * @retval    true      The flag is present in the options map, and its value is true
   * @retval    false     The flag is either not present or is present and set to false
   */
  bool has_flag(cta::admin::OptionBoolean::Key option) const {
    auto opt_it = m_option_bool.find(option);
    return opt_it != m_option_bool.end() && opt_it->second;
  }

private:
  /*!
   * Process Notification events
   *
   * @param[in]     notification    Notification request message from EOS WFE
   * @param[out]    response        Response message to return to EOS
   */
  void processOPENW        (const cta::eos::Notification &notification, cta::xrd::Response &response);    //!< Ignore OPENW event
  void processCREATE       (const cta::eos::Notification &notification, cta::xrd::Response &response);    //!< New archive file ID event
  void processCLOSEW       (const cta::eos::Notification &notification, cta::xrd::Response &response);    //!< Archive file event
  void processPREPARE      (const cta::eos::Notification &notification, cta::xrd::Response &response);    //!< Retrieve file event
  void processABORT_PREPARE(const cta::eos::Notification &notification, cta::xrd::Response &response);    //!< Abort retrieve file event
  void processDELETE       (const cta::eos::Notification &notification, cta::xrd::Response &response);    //!< Delete file event
  void processUPDATE_FID   (const cta::eos::Notification &notification, cta::xrd::Response &response);    //!< Disk file ID update event

  /*!
   * Process AdminCmd events
   *
   * @param[out]    response        CTA Admin command response message
   */
  void processAdmin_Add             (cta::xrd::Response &response);
  void processAdmin_Ch              (cta::xrd::Response &response);
  void processAdmin_Rm              (cta::xrd::Response &response);
  void processArchiveRoute_Add      (cta::xrd::Response &response);
  void processArchiveRoute_Ch       (cta::xrd::Response &response);
  void processArchiveRoute_Rm       (cta::xrd::Response &response);
  void processDrive_Up              (cta::xrd::Response &response);
  void processDrive_Down            (cta::xrd::Response &response);
  void processDrive_Ch              (cta::xrd::Response &response);
  void processDrive_Rm              (cta::xrd::Response &response);
  void processFailedRequest_Rm      (cta::xrd::Response &response);
  void processGroupMountRule_Add    (cta::xrd::Response &response);
  void processGroupMountRule_Ch     (cta::xrd::Response &response);
  void processGroupMountRule_Rm     (cta::xrd::Response &response);
  void processLogicalLibrary_Add    (cta::xrd::Response &response);
  void processLogicalLibrary_Ch     (cta::xrd::Response &response);
  void processLogicalLibrary_Rm     (cta::xrd::Response &response);
  void processMediaType_Add         (cta::xrd::Response &response);
  void processMediaType_Ch          (cta::xrd::Response &response);
  void processMediaType_Rm          (cta::xrd::Response &response);
  void processMountPolicy_Add       (cta::xrd::Response &response);
  void processMountPolicy_Ch        (cta::xrd::Response &response);
  void processMountPolicy_Rm        (cta::xrd::Response &response);
  void processRepack_Add            (cta::xrd::Response &response);
  void processRepack_Rm             (cta::xrd::Response &response);
  void processRepack_Err            (cta::xrd::Response &response);
  void processRequesterMountRule_Add(cta::xrd::Response &response);
  void processRequesterMountRule_Ch (cta::xrd::Response &response);
  void processRequesterMountRule_Rm (cta::xrd::Response &response);
  void processActivityMountRule_Add (cta::xrd::Response &response);
  void processActivityMountRule_Ch  (cta::xrd::Response &response);
  void processActivityMountRule_Rm  (cta::xrd::Response &response);
  void processStorageClass_Add      (cta::xrd::Response &response);
  void processStorageClass_Ch       (cta::xrd::Response &response);
  void processStorageClass_Rm       (cta::xrd::Response &response);
  void processTape_Add              (cta::xrd::Response &response);
  void processTape_Ch               (cta::xrd::Response &response);
  void processTape_Rm               (cta::xrd::Response &response);
  void processTape_Reclaim          (cta::xrd::Response &response);
  void processTape_Label            (cta::xrd::Response &response);
  void processTapeFile_Ch           (cta::xrd::Response &response);
  void processTapeFile_Rm           (cta::xrd::Response &response);
  void processTapePool_Add          (cta::xrd::Response &response);
  void processTapePool_Ch           (cta::xrd::Response &response);
  void processTapePool_Rm           (cta::xrd::Response &response);
  void processDiskSystem_Add        (cta::xrd::Response &response);
  void processDiskSystem_Ch         (cta::xrd::Response &response);
  void processDiskSystem_Rm         (cta::xrd::Response &response);
  void processDiskInstance_Add      (cta::xrd::Response &response);
  void processDiskInstance_Ch       (cta::xrd::Response &response);
  void processDiskInstance_Rm       (cta::xrd::Response &response);
  void processDiskInstanceSpace_Add (cta::xrd::Response &response);
  void processDiskInstanceSpace_Ch  (cta::xrd::Response &response);
  void processDiskInstanceSpace_Rm  (cta::xrd::Response &response);
  void processVirtualOrganization_Add(cta::xrd::Response &response);
  void processVirtualOrganization_Ch(cta::xrd::Response &response);
  void processVirtualOrganization_Rm(cta::xrd::Response &response);
  void processRecycleTapeFile_Restore(cta::xrd::Response &response);
  void processChangeStorageClass    (cta::xrd::Response &response);
  
  /*!
   * Process AdminCmd events which can return a stream response
   *
   * @param[out]    response    Response protocol buffer message. This is used for response
   *                            headers or for summary responses.
   * @param[out]    stream      Reference to Response stream message pointer
   */
  typedef void admincmdstream_t(cta::xrd::Response &response, XrdSsiStream* &stream);

  admincmdstream_t processAdmin_Ls;
  admincmdstream_t processArchiveRoute_Ls;
  admincmdstream_t processDrive_Ls;
  admincmdstream_t processFailedRequest_Ls;
  admincmdstream_t processGroupMountRule_Ls;
  admincmdstream_t processActivityMountRule_Ls;
  admincmdstream_t processLogicalLibrary_Ls;
  admincmdstream_t processMediaType_Ls;
  admincmdstream_t processMountPolicy_Ls;
  admincmdstream_t processRequesterMountRule_Ls;
  admincmdstream_t processShowQueues;
  admincmdstream_t processStorageClass_Ls;
  admincmdstream_t processTapePool_Ls;
  admincmdstream_t processTape_Ls;
  admincmdstream_t processTapeFile_Ls;
  admincmdstream_t processRepack_Ls;
  admincmdstream_t processDiskSystem_Ls;
  admincmdstream_t processDiskInstance_Ls;
  admincmdstream_t processDiskInstanceSpace_Ls;
  admincmdstream_t processVirtualOrganization_Ls;
  admincmdstream_t processVersion;
  admincmdstream_t processRecycleTapeFile_Ls;

  /*!
   * Log an admin command
   *
   * @param[in]    admincmd    CTA Admin command request message
   * @param[in]    t           CTA Catalogue timer
   */
  void logAdminCmd(const std::string &function, const std::string &status, const std::string &reason, 
    const cta::admin::AdminCmd &admincmd, cta::utils::Timer &t);

  /*!
   * Drive state enum
   */
  enum DriveState { Up, Down };

  /*!
   * Changes state for the drives by a given regex.
   *
   * @param[in]     regex          The regex to match drive name(s) to change
   * @param[in]     desiredDriveState The object holding desired drive state informations (up, forceDown, reason, comment)
   * 
   * @returns       The result of the operation, to return to the client
   */
  std::string setDriveState(const std::string &regex, const cta::common::dataStructures::DesiredDriveState & desiredDriveState);

  /*!
   * Import Google Protobuf option fields into maps
   *
   * @param[in]     admincmd        CTA Admin command request message
   */
  void importOptions(const cta::admin::AdminCmd &admincmd);

  /*!
   * Throw an exception for empty protocol buffer strings
   */
  void checkIsNotEmptyString(const std::string &value, const std::string &error_txt) {
    if(value.empty()) throw XrdSsiPb::PbException("Protocol buffer field " + error_txt + " is an empty string.");
  }

  // Security protocol used to connect

  enum class Protocol { SSS, KRB5, OTHER };

  const std::map<std::string, Protocol> m_protomap = {
    { "sss",  Protocol::SSS  },
    { "krb5", Protocol::KRB5 },
  };

  // Member variables

  Protocol                                              m_protocol;                   //!< The protocol the client used to connect
  cta::common::dataStructures::SecurityIdentity         m_cliIdentity;                //!< Client identity: username/host
  const XrdSsiCtaServiceProvider                       &m_service;                    //!< Const reference to the XRootD SSI Service
  cta::catalogue::Catalogue                            &m_catalogue;                  //!< Reference to CTA Catalogue
  cta::Scheduler                                       &m_scheduler;                  //!< Reference to CTA Scheduler
  uint64_t                                              m_archiveFileMaxSize;         //!< Maximum allowed file size for archive requests
  std::optional<std::string>				                    m_repackBufferURL;            //!< Repack buffer URL
  std::optional<std::string>				                    m_verificationMountPolicy;    //!< Repack buffer URL
  NamespaceMap_t                                        m_namespaceMap;               //!< Identifiers for namespace queries
  cta::log::LogContext                                  m_lc;                         //!< CTA Log Context
  std::map<cta::admin::OptionBoolean::Key, bool>        m_option_bool;                //!< Boolean options
  std::map<cta::admin::OptionUInt64::Key, uint64_t>     m_option_uint64;              //!< UInt64 options
  std::map<cta::admin::OptionString::Key, std::string>  m_option_str;                 //!< String options
  std::map<cta::admin::OptionStrList::Key,
    std::vector<std::string>>                           m_option_str_list;            //!< String List options
  Versions                                              m_client_versions;            //!< Client CTA and xrootd-ssi-proto version(tag)
  std::string m_client_cta_version;                                                   //!< Client CTA Version
  std::string m_client_xrd_ssi_proto_int_version;                                     //!< Client xrootd-ssi-protobuf-interface version (tag)  
  std::string m_catalogue_conn_string;                                                //!< Server catalogue connection string
};

}} // namespace cta::xrd
