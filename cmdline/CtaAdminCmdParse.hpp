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

#include <map>
#include <set>
#include <string>
#include "common/dataStructures/Tape.hpp"

#include "CtaFrontendApi.hpp"

namespace cta::admin {

using namespace common::dataStructures;

/*!
 * Command line option class
 */
class Option
{
public:
   enum option_t { OPT_CMD, OPT_FLAG, OPT_BOOL, OPT_UINT, OPT_STR, OPT_STR_LIST };

   /*!
    * Constructor
    */
   Option(option_t type, const std::string &long_opt, const std::string &short_opt,
          const std::string &help_txt, const std::string &alias = "") :
      m_type(type),
      m_lookup_key((alias.size() == 0) ? long_opt : alias),
      m_long_opt(long_opt),
      m_short_opt(short_opt),
      m_help_txt(help_txt),
      m_is_optional(false) {
   }

   /*!
    * Copy-construct an optional version of this option
    */
   Option optional() const {
      Option option(*this);
      option.m_is_optional = true;
      return option;
   }

   /*!
    * Check if the supplied key matches the option
    */
   bool operator==(const std::string &option) const {
      return option == m_short_opt || option == m_long_opt;
   }

   /*!
    * Check if the supplied option matches the option
    */
   bool operator==(const Option &option) const {
      return option == m_short_opt || option == m_long_opt;
   }

   /*!
    * Return the type of this option
    */
   option_t get_type() const { return m_type; }

   /*!
    * Return the number of parameters expected after this option
    */
   int num_params() const { return m_type == OPT_CMD || m_type == OPT_FLAG ? 0 : 1; }

   /*!
    * Return the key for this option
    */
   const std::string &get_key() const { return m_lookup_key; }

   /*!
    * Return whether the option is optional
    */
   bool is_optional() const { return m_is_optional; }

   /*!
    * Validate the command protocol buffer against this option
    *
    * If the option is compulsory and is not present, throws an exception
    */
   void validateCmd(const cta::admin::AdminCmd &admincmd) const;

   /*!
    * Return the help text for this option
    */
   std::string_view get_help_text() const { return m_help_txt; }

   /*!
    * Return per-option help string
    */
   std::string help() const {
      std::string help = m_is_optional ? " [" : " ";
      help += (m_type == OPT_CMD) ? "" : m_long_opt + '/' + m_short_opt;
      help += m_help_txt;
      help += m_is_optional ? "]" : "";
      return help;
   }

private:
   option_t    m_type;          //!< Option type
   std::string m_lookup_key;    //!< Key to map option string to Protocol Buffer enum
   std::string m_long_opt;      //!< Long command option
   std::string m_short_opt;     //!< Short command option
   std::string m_help_txt;      //!< Option help text
   bool        m_is_optional;   //!< Option is optional or compulsory
};



/*
 * Type aliases
 */
using cmdLookup_t    = std::map<std::string, AdminCmd::Cmd>;
using subcmdLookup_t = std::map<std::string, AdminCmd::SubCmd>;
using cmd_key_t      = std::pair<AdminCmd::Cmd, AdminCmd::SubCmd>;
using cmd_val_t      = std::vector<Option>;



/*!
 * Command/subcommand help class
 */
class CmdHelp
{
public:
   /*!
    * Constructor
    */
   CmdHelp(const std::string &cmd_long, const std::string &cmd_short,
           const std::vector<std::string> &sub_cmd, const std::string &help_txt = "") :
      m_cmd_long(cmd_long),
      m_cmd_short(cmd_short),
      m_sub_cmd(sub_cmd),
      m_help_extra(help_txt) {}

   /*!
    * Can we parse subcommands for this command?
    */
   bool has_subcommand() const { return m_sub_cmd.size() > 0; }

   /*!
    * Return the short help message
    */
   std::string short_help() const;

   /*!
    * Return the detailed help message
    */
   std::string help() const;

private:
   /*!
    * Called by help() to add command line options to the full help text
    */
   void add_options(std::string &cmd_line, cmd_key_t &key, unsigned int indent) const;

   const unsigned int INDENT      = 4;      //!< Number of spaces to indent when listing subcommands
   const unsigned int WRAP_MARGIN = 80;     //!< Number of characters per line before word wrapping

   std::string              m_cmd_long;     //!< Command string (long version)
   std::string              m_cmd_short;    //!< Command string (short version)
   std::vector<std::string> m_sub_cmd;      //!< Subcommands which are valid for this command, in the
                                            //!< same order that they should be displayed in the help
   std::string              m_help_extra;   //!< Optional extra help text above the options
   mutable std::string      m_help_full;    //!< The full text of the detailed help for this command.
                                            //!< Mutable because it is lazy evaluated when we call help()
};



/*!
 * Map short and long command names to Protocol Buffer enum values
 */
const cmdLookup_t cmdLookup = {
   { "admin",                   AdminCmd::CMD_ADMIN },
   { "ad",                      AdminCmd::CMD_ADMIN },
   { "archiveroute",            AdminCmd::CMD_ARCHIVEROUTE },
   { "ar",                      AdminCmd::CMD_ARCHIVEROUTE },
   { "drive",                   AdminCmd::CMD_DRIVE },
   { "dr",                      AdminCmd::CMD_DRIVE },
   { "failedrequest",           AdminCmd::CMD_FAILEDREQUEST },
   { "fr",                      AdminCmd::CMD_FAILEDREQUEST },
   { "groupmountrule",          AdminCmd::CMD_GROUPMOUNTRULE },
   { "gmr",                     AdminCmd::CMD_GROUPMOUNTRULE },
   { "logicallibrary",          AdminCmd::CMD_LOGICALLIBRARY },
   { "ll",                      AdminCmd::CMD_LOGICALLIBRARY },
   { "mediatype",               AdminCmd::CMD_MEDIATYPE },
   { "mt",                      AdminCmd::CMD_MEDIATYPE },
   { "mountpolicy",             AdminCmd::CMD_MOUNTPOLICY },
   { "mp",                      AdminCmd::CMD_MOUNTPOLICY },
   { "repack",                  AdminCmd::CMD_REPACK },
   { "re",                      AdminCmd::CMD_REPACK },
   { "requestermountrule",      AdminCmd::CMD_REQUESTERMOUNTRULE },
   { "rmr",                     AdminCmd::CMD_REQUESTERMOUNTRULE },
   { "showqueues",              AdminCmd::CMD_SHOWQUEUES },
   { "sq",                      AdminCmd::CMD_SHOWQUEUES },
   { "storageclass",            AdminCmd::CMD_STORAGECLASS },
   { "sc",                      AdminCmd::CMD_STORAGECLASS },
   { "tape",                    AdminCmd::CMD_TAPE },
   { "ta",                      AdminCmd::CMD_TAPE },
   { "tapefile",                AdminCmd::CMD_TAPEFILE },
   { "tf",                      AdminCmd::CMD_TAPEFILE },
   { "tapepool",                AdminCmd::CMD_TAPEPOOL },
   { "tp",                      AdminCmd::CMD_TAPEPOOL },
   { "disksystem",              AdminCmd::CMD_DISKSYSTEM },
   { "ds",                      AdminCmd::CMD_DISKSYSTEM },
   { "virtualorganization",     AdminCmd::CMD_VIRTUALORGANIZATION },
   { "vo",                      AdminCmd::CMD_VIRTUALORGANIZATION },
   { "version",                 AdminCmd::CMD_VERSION},
   { "v",                       AdminCmd::CMD_VERSION},
   { "recycletf",               AdminCmd::CMD_RECYCLETAPEFILE},
   { "rtf",                     AdminCmd::CMD_RECYCLETAPEFILE},
   { "activitymountrule",       AdminCmd::CMD_ACTIVITYMOUNTRULE },
   { "amr",                     AdminCmd::CMD_ACTIVITYMOUNTRULE },
   { "diskinstance",            AdminCmd::CMD_DISKINSTANCE },
   { "di",                      AdminCmd::CMD_DISKINSTANCE }, 
   { "diskinstancespace",       AdminCmd::CMD_DISKINSTANCESPACE },
   { "dis",                     AdminCmd::CMD_DISKINSTANCESPACE },
   { "physicallibrary",         AdminCmd::CMD_PHYSICALLIBRARY },
   { "pl",                      AdminCmd::CMD_PHYSICALLIBRARY }
};



/*!
 * Map subcommand names to Protocol Buffer enum values
 */
const subcmdLookup_t subcmdLookup = {
   { "add",                     AdminCmd::SUBCMD_ADD },
   { "ch",                      AdminCmd::SUBCMD_CH },
   { "err",                     AdminCmd::SUBCMD_ERR },
   { "ls",                      AdminCmd::SUBCMD_LS },
   { "reclaim",                 AdminCmd::SUBCMD_RECLAIM },
   { "retry",                   AdminCmd::SUBCMD_RETRY },
   { "rm",                      AdminCmd::SUBCMD_RM },
   { "up",                      AdminCmd::SUBCMD_UP },
   { "down",                    AdminCmd::SUBCMD_DOWN },
};



/*!
 * Map boolean options to Protocol Buffer enum values
 */
const std::map<std::string, OptionBoolean::Key> boolOptions = {
   // Boolean options
   { "--all",                   OptionBoolean::ALL },
   { "--disabled",              OptionBoolean::DISABLED },
   { "--encrypted",             OptionBoolean::ENCRYPTED },
   { "--force",                 OptionBoolean::FORCE },
   { "--full",                  OptionBoolean::FULL },
   {"--fromcastor",             OptionBoolean::FROM_CASTOR },

   // hasOption options
   { "--disabledtape",          OptionBoolean::DISABLED },
   { "--justarchive",           OptionBoolean::JUSTARCHIVE },
   { "--justmove",              OptionBoolean::JUSTMOVE },
   { "--justaddcopies",         OptionBoolean::JUSTADDCOPIES },
   { "--justretrieve",          OptionBoolean::JUSTRETRIEVE },
   { "--log",                   OptionBoolean::SHOW_LOG_ENTRIES },
   { "--lookupnamespace",       OptionBoolean::LOOKUP_NAMESPACE },
   { "--summary",               OptionBoolean::SUMMARY },
   { "--no-recall",             OptionBoolean::NO_RECALL },
   { "--dirtybit",              OptionBoolean::DIRTY_BIT },
   { "--isrepackvo",            OptionBoolean::IS_REPACK_VO }
};



/*!
 * Map integer options to Protocol Buffer enum values
 */
const std::map<std::string, OptionUInt64::Key> uint64Options = {
   { "--archivepriority",           OptionUInt64::ARCHIVE_PRIORITY },
   { "--capacity",                  OptionUInt64::CAPACITY },
   { "--copynb",                    OptionUInt64::COPY_NUMBER },
   { "--id",                        OptionUInt64::ARCHIVE_FILE_ID },
   { "--maxfilesize",               OptionUInt64::MAX_FILE_SIZE},
   { "--maxlpos",                   OptionUInt64::MAX_LPOS },
   { "--minarchiverequestage",      OptionUInt64::MIN_ARCHIVE_REQUEST_AGE },
   { "--minlpos",                   OptionUInt64::MIN_LPOS },
   { "--minretrieverequestage",     OptionUInt64::MIN_RETRIEVE_REQUEST_AGE },
   { "--nbwraps",                   OptionUInt64::NUMBER_OF_WRAPS },
   { "--partialtapesnumber",        OptionUInt64::PARTIAL_TAPES_NUMBER },
   { "--primarydensitycode",        OptionUInt64::PRIMARY_DENSITY_CODE },
   { "--retrievepriority",          OptionUInt64::RETRIEVE_PRIORITY },
   { "--secondarydensitycode",      OptionUInt64::SECONDARY_DENSITY_CODE },
   { "--refreshinterval",           OptionUInt64::REFRESH_INTERVAL },
   { "--targetedfreespace",         OptionUInt64::TARGETED_FREE_SPACE },
   { "--sleeptime",                 OptionUInt64::SLEEP_TIME },
   { "--readmaxdrives",             OptionUInt64::READ_MAX_DRIVES },
   { "--writemaxdrives",            OptionUInt64::WRITE_MAX_DRIVES },
   { "--nbphysicalcartridgeslots",  OptionUInt64::NB_PHYSICAL_CARTRIDGE_SLOTS },
   { "--nbavailablecartridgeslots", OptionUInt64::NB_AVAILABLE_CARTRIDGE_SLOTS },
   { "--nbphysicaldriveslots",      OptionUInt64::NB_PHYSICAL_DRIVE_SLOTS },
   { "--maxfilestoselect",          OptionUInt64::MAX_FILES_TO_EXPAND },
   { "--logunixtimemin",            OptionUInt64::LOG_UNIXTIME_MIN },
   { "--logunixtimemax",            OptionUInt64::LOG_UNIXTIME_MAX },
};



/*!
 * Map string options to Protocol Buffer enum values
 */
const std::map<std::string, OptionString::Key> strOptions = {
   { "--bufferurl",             OptionString::BUFFERURL },
   { "--cartridge",             OptionString::CARTRIDGE },
   { "--comment",               OptionString::COMMENT },
   { "--drive",                 OptionString::DRIVE },
   { "--encryptionkeyname",     OptionString::ENCRYPTION_KEY_NAME },
   { "--fxid",                  OptionString::FXID },
   { "--instance",              OptionString::INSTANCE },
   { "--logicallibrary",        OptionString::LOGICAL_LIBRARY },
   { "--mediatype",             OptionString::MEDIA_TYPE },
   { "--mountpolicy",           OptionString::MOUNT_POLICY },
   { "--objectid",              OptionString::OBJECTID },
   { "--storageclass",          OptionString::STORAGE_CLASS },
   { "--supply",                OptionString::SUPPLY },
   { "--tapepool",              OptionString::TAPE_POOL },
   { "--username",              OptionString::USERNAME },
   { "--vendor",                OptionString::VENDOR },
   { "--vid",                   OptionString::VID },
   { "--virtualorganisation",   OptionString::VO },
   { "--disksystem",            OptionString::DISK_SYSTEM },
   { "--fileregexp",            OptionString::FILE_REGEXP },
   { "--freespacequeryurl",     OptionString::FREE_SPACE_QUERY_URL },
   { "--reason",                OptionString::REASON },
   { "--state",                 OptionString::STATE },
   {  "--activityregex",        OptionString::ACTIVITY_REGEX},
   { "--diskinstance",          OptionString::DISK_INSTANCE },
   { "--diskinstancespace",     OptionString::DISK_INSTANCE_SPACE },
   { "--verificationstatus",    OptionString::VERIFICATION_STATUS },
   { "--disabledreason",        OptionString::DISABLED_REASON },
   { "--purchaseorder",         OptionString::MEDIA_PURCHASE_ORDER_NUMBER },
   { "--physicallibrary",       OptionString::PHYSICAL_LIBRARY },
   { "--manufacturer",          OptionString::MANUFACTURER },
   { "--model",                 OptionString::LIBRARY_MODEL },
   { "--type",                  OptionString::LIBRARY_TYPE },
   { "--guiurl",                OptionString::GUI_URL },
   { "--webcamurl",             OptionString::WEBCAM_URL },
   { "--location",              OptionString::LIBRARY_LOCATION }
};

/*!
 * Map string list options to Protocol Buffer enum values
 */
const std::map<std::string, OptionStrList::Key> strListOptions = {
  {"--fxidfile", OptionStrList::FILE_ID},
  {"--vidfile",  OptionStrList::VID    },
  {"--id",       OptionStrList::FILE_ID}
};

/**
 * Specify the help text for commands
 */
const std::map<AdminCmd::Cmd, CmdHelp> cmdHelp = {
  {AdminCmd::CMD_ACTIVITYMOUNTRULE,   {"activitymountrule", "amr", {"add", "ch", "rm", "ls"}} },
  {AdminCmd::CMD_ADMIN,               {"admin", "ad", {"add", "ch", "rm", "ls"}}              },
  {AdminCmd::CMD_ARCHIVEROUTE,        {"archiveroute", "ar", {"add", "ch", "rm", "ls"}}       },
  {AdminCmd::CMD_DISKINSTANCE,        {"diskinstance", "di", {"add", "ch", "rm", "ls"}}       },
  {AdminCmd::CMD_DISKINSTANCESPACE,   {"diskinstancespace", "dis", {"add", "ch", "rm", "ls"}} },
  {AdminCmd::CMD_DISKSYSTEM,          {"disksystem", "ds", {"add", "ch", "rm", "ls"}}         },
  {AdminCmd::CMD_DRIVE,               {"drive", "dr", {"up", "down", "ls", "ch", "rm"}}       },
  {AdminCmd::CMD_FAILEDREQUEST,       {"failedrequest", "fr", {"ls", "rm"}}                   },
  {AdminCmd::CMD_GROUPMOUNTRULE,      {"groupmountrule", "gmr", {"add", "ch", "rm", "ls"}}    },
  {AdminCmd::CMD_LOGICALLIBRARY,      {"logicallibrary", "ll", {"add", "ch", "rm", "ls"}}     },
  {AdminCmd::CMD_MEDIATYPE,           {"mediatype", "mt", {"add", "ch", "rm", "ls"}}          },
  {AdminCmd::CMD_MOUNTPOLICY,         {"mountpolicy", "mp", {"add", "ch", "rm", "ls"}}        },
  {AdminCmd::CMD_PHYSICALLIBRARY,     {"physicallibrary", "pl", {"add", "ch", "rm", "ls"}}    },
  {AdminCmd::CMD_RECYCLETAPEFILE,     {"recycletf", "rtf", {"ls"}}                            },
  {AdminCmd::CMD_REPACK,              {"repack", "re", {"add", "rm", "ls", "err"}}            },
  {AdminCmd::CMD_REQUESTERMOUNTRULE,  {"requestermountrule", "rmr", {"add", "ch", "rm", "ls"}}},
  {AdminCmd::CMD_SHOWQUEUES,          {"showqueues", "sq", {}}                                },
  {AdminCmd::CMD_STORAGECLASS,        {"storageclass", "sc", {"add", "ch", "rm", "ls"}}       },
  {AdminCmd::CMD_TAPE,                {"tape", "ta", {"add", "ch", "rm", "reclaim", "ls"}}    },
  {AdminCmd::CMD_TAPEFILE,            {"tapefile", "tf", {"ls", "rm"}}                        },
  {AdminCmd::CMD_TAPEPOOL,            {"tapepool", "tp", {"add", "ch", "rm", "ls"}}           },
  {AdminCmd::CMD_VERSION,             {"version", "v", {}}                                    },
  {AdminCmd::CMD_VIRTUALORGANIZATION, {"virtualorganization", "vo", {"add", "ch", "rm", "ls"}}},
};

/*
 * Enumerate options
 */
const Option opt_all {Option::OPT_FLAG, "--all", "-a", ""};
const Option opt_archivefileid {Option::OPT_UINT, "--id", "-I", " <archive_file_id>"};
const Option opt_archivepriority {Option::OPT_UINT, "--archivepriority", "--ap", " <priority_value>"};
const Option opt_bufferurl {Option::OPT_STR, "--bufferurl", "-b", " <buffer URL>"};
const Option opt_capacity {Option::OPT_UINT, "--capacity", "-c", " <capacity_in_bytes>"};
const Option opt_cartridge {Option::OPT_STR, "--cartridge", "-t", " <cartridge>"};
const Option opt_comment {Option::OPT_STR, "--comment", "-m", " <\"comment\">"};
const Option opt_copynb {Option::OPT_UINT, "--copynb", "-c", " <copy_number>"};
const Option opt_copynb_alias {Option::OPT_UINT, "--numberofcopies", "-c", " <number_of_copies>", "--copynb"};
const Option opt_disabled {Option::OPT_BOOL, "--disabled", "-d", " <\"true\" or \"false\">"};
const Option opt_drivename_cmd {Option::OPT_CMD, "--drive", "", "<drive_name>"};
const Option opt_encrypted {Option::OPT_BOOL, "--encrypted", "-e", " <\"true\" or \"false\">"};
const Option opt_encryptionkeyname {Option::OPT_STR, "--encryptionkeyname", "-k", " <encryption_key_name>"};
const Option opt_fid {Option::OPT_STR, "--fxid", "-f", " <eos_fxid>"};
const Option opt_fidfile {Option::OPT_STR_LIST, "--fxidfile", "-F", " <filename>"};
const Option opt_filename {Option::OPT_STR, "--file", "-f", " <filename>"};
const Option opt_force {Option::OPT_BOOL, "--force", "-f", " <\"true\" or \"false\">"};
const Option opt_force_flag {Option::OPT_FLAG, "--force", "-f", ""};
const Option opt_fromcastor {Option::OPT_BOOL, "--fromcastor", "--fc", " <\"true\" or \"false\">"};
const Option opt_dirtybit {Option::OPT_BOOL, "--dirtybit", "--db", " <\"true\" or \"false\">"};
const Option opt_instance {Option::OPT_STR, "--instance", "-i", " <disk_instance>"};
const Option opt_justarchive {Option::OPT_FLAG, "--justarchive", "-a", ""};
const Option opt_justmove {Option::OPT_FLAG, "--justmove", "-m", ""};
const Option opt_justaddcopies {Option::OPT_FLAG, "--justaddcopies", "-a", ""};
const Option opt_justretrieve {Option::OPT_FLAG, "--justretrieve", "-r", ""};
const Option opt_log {Option::OPT_FLAG, "--log", "-l", ""};
const Option opt_logicallibrary {Option::OPT_STR, "--logicallibrary", "-l", " <logical_library_name>"};
const Option opt_logicallibrary_alias {Option::OPT_STR, "--name", "-n", " <logical_library_name>", "--logicallibrary"};
const Option opt_logicallibrary_disabled {Option::OPT_BOOL, "--disabled", "-d", " <\"true\" or \"false\">"};
const Option opt_maxfilesize {Option::OPT_UINT, "--maxfilesize", "--mfs", " <maximum_file_size>"};
const Option opt_maxlpos {Option::OPT_UINT, "--maxlpos", "--maxl", " <maximum_longitudinal_position>"};
const Option opt_mediatype {Option::OPT_STR, "--mediatype", "--mt", " <media_type_name>"};
const Option opt_mediatype_alias {Option::OPT_STR, "--name", "-n", " <media_type_name>", "--mediatype"};
const Option opt_minarchiverequestage {Option::OPT_UINT, "--minarchiverequestage", "--aa", " <min_request_age>"};
const Option opt_minlpos {Option::OPT_UINT, "--minlpos", "--minl", " <minimum_longitudinal_position>"};
const Option opt_minretrieverequestage {Option::OPT_UINT, "--minretrieverequestage", "--ra", " <min_request_age>"};
const Option opt_mountpolicy {Option::OPT_STR, "--mountpolicy", "-u", " <mount_policy_name>"};
const Option opt_mountpolicy_alias {Option::OPT_STR, "--name", "-n", " <mount_policy_name>", "--mountpolicy"};
const Option opt_number_of_wraps {Option::OPT_UINT, "--nbwraps", "-w", " <number_of_wraps>"};
const Option opt_partialfiles {Option::OPT_UINT, "--partial", "-p", " <number_of_files_per_tape>"};
const Option opt_partialtapes {Option::OPT_UINT, "--partialtapesnumber", "-p", " <number_of_partial_tapes>"};
const Option opt_primarydensitycode {Option::OPT_UINT, "--primarydensitycode", "-p", " <primary_density_code>"};
const Option opt_retrievepriority {Option::OPT_UINT, "--retrievepriority", "--rp", " <priority_value>"};
const Option opt_secondarydensitycode {Option::OPT_UINT, "--secondarydensitycode", "-s", " <secondary_density_code>"};
const Option opt_storageclass {Option::OPT_STR, "--storageclass", "-s", " <storage_class_name>"};
const Option opt_storageclass_alias {Option::OPT_STR, "--name", "-n", " <storage_class_name>", "--storageclass"};
const Option opt_summary {Option::OPT_FLAG, "--summary", "-S", ""};
const Option opt_supply {Option::OPT_STR, "--supply", "-s", " <supply_value>"};
const Option opt_tapepool {Option::OPT_STR, "--tapepool", "-t", " <tapepool_name>"};
const Option opt_tapepool_alias {Option::OPT_STR, "--name", "-n", " <tapepool_name>", "--tapepool"};
const Option opt_username {Option::OPT_STR, "--username", "-u", " <user_name>"};
const Option opt_username_alias {Option::OPT_STR, "--name", "-n", " <user_name>", "--username"};
const Option opt_groupname_alias {Option::OPT_STR, "--name", "-n", " <group_name>", "--username"};
const Option opt_vendor {Option::OPT_STR, "--vendor", "--ve", " <vendor>"};
const Option opt_vid {Option::OPT_STR, "--vid", "-v", " <vid>"};
const Option opt_purchase_order {Option::OPT_STR, "--purchaseorder", "-p", " <purchase_order>"};
const Option opt_vo {Option::OPT_STR, "--virtualorganisation", "--vo", " <virtual_organisation>"};
const Option opt_vidfile {Option::OPT_STR_LIST, "--vidfile", "-f", " <filename>"};
const Option opt_full {Option::OPT_BOOL, "--full", "-f", " <\"true\" or \"false\">"};
const Option opt_disksystem {Option::OPT_STR, "--disksystem", "-n", " <disk_system_name>"};
const Option opt_file_regexp {Option::OPT_STR, "--fileregexp", "-r", " <file_regexp>"};
const Option opt_free_space_query_url {Option::OPT_STR, "--freespacequeryurl", "-u", " <free_space_query_url>"};
const Option opt_refresh_interval {Option::OPT_UINT, "--refreshinterval", "-i", " <refresh_intreval>"};
const Option opt_targeted_free_space {Option::OPT_UINT, "--targetedfreespace", "-f", " <targeted_free_space>"};
const Option opt_sleep_time {Option::OPT_UINT, "--sleeptime", "-s", " <sleep time in s>"};
const Option opt_reason {Option::OPT_STR, "--reason", "-r", " <reason_status_change>"};
const Option opt_no_recall {Option::OPT_FLAG, "--no-recall", "--nr", ""};
const Option opt_object_id {Option::OPT_STR, "--objectid", "-o", " <objectId>"};
const Option opt_read_max_drives {Option::OPT_UINT, "--readmaxdrives", "--rmd", " <read_max_drives>"};
const Option opt_write_max_drives {Option::OPT_UINT, "--writemaxdrives", "--wmd", " <write_max_drives>"};
const Option opt_state {
  Option::OPT_STR, "--state", "-s",
  std::string(" <\"") + Tape::stateToString(Tape::ACTIVE) + "\"" + " or \"" + Tape::stateToString(Tape::DISABLED) +
    "\" or \"" + Tape::stateToString(Tape::BROKEN) + "\" or \"" + Tape::stateToString(Tape::EXPORTED) + "\" or \"" +
    Tape::stateToString(Tape::REPACKING) + "\" or \"" + Tape::stateToString(Tape::REPACKING_DISABLED) + "\">"};
const Option opt_activityregex {Option::OPT_STR, "--activityregex", "--ar", " <activity_regex>"};
const Option opt_diskinstance {Option::OPT_STR, "--diskinstance", "--di", " <disk_instance_name>"};
const Option opt_diskinstance_alias {Option::OPT_STR, "--name", "-n", " <disk_instance_name>", "--diskinstance"};
const Option opt_diskinstancespace {Option::OPT_STR, "--diskinstancespace", "--dis", " <disk_instance_space_name>"};
const Option opt_diskinstancespace_alias {Option::OPT_STR, "--name", "-n", " <disk_instance_space_name>",
                                          "--diskinstancespace"};
const Option opt_verificationstatus {Option::OPT_STR, "--verificationstatus", "--vs", " <verification_status>"};
const Option opt_disabledreason {Option::OPT_STR, "--disabledreason", "--dr", " <disabled_reason>"};
const Option opt_archive_file_ids {Option::OPT_STR_LIST, "--id", "-I", " <archive_file_id>"};
const Option opt_physical_library_alias {Option::OPT_STR, "--name", "-n", " <physical_library_name>",
                                         "--physicallibrary"};
const Option opt_physical_library {Option::OPT_STR, "--physicallibrary", "--pl", " <physical_library_name>",
                                   "--physicallibrary"};
const Option opt_manufacturer {Option::OPT_STR, "--manufacturer", "--ma", " <manufacturer>"};
const Option opt_model {Option::OPT_STR, "--model", "--mo", " <model>"};
const Option opt_type {Option::OPT_STR, "--type", "-t", " <type>"};
const Option opt_gui_url {Option::OPT_STR, "--guiurl", "-g", " <gui_url>"};
const Option opt_webcam_url {Option::OPT_STR, "--webcamurl", "-w", " <webcam_url>"};
const Option opt_location {Option::OPT_STR, "--location", "-l", " <location>"};
const Option opt_nb_physical_cartridge_slots {Option::OPT_UINT, "--nbphysicalcartridgeslots", "--npcs",
                                              " <nb_physical_cartridge_slots>"};
const Option opt_nb_available_cartridge_slots {Option::OPT_UINT, "--nbavailablecartridgeslots", "--nacs",
                                               " <nb_available_cartridge_slots>"};
const Option opt_nb_physical_drive_slots {Option::OPT_UINT, "--nbphysicaldriveslots", "--npds",
                                          " <nb_physical_drive_slots >"};
const Option opt_isrepackvo {Option::OPT_BOOL, "--isrepackvo", "--irvo", " <\"true\" or \"false\">"};
const Option opt_max_files_to_select {Option::OPT_UINT, "--maxfilestoselect", "--mfts", " <max_files_to_select>"};
const Option opt_log_unixtime_min {Option::OPT_UINT, "--logunixtimemin", "--ltmin", " <min_recycle_log_unixtime>"};
const Option opt_log_unixtime_max {Option::OPT_UINT, "--logunixtimemax", "--ltmax", " <max_recycle_log_unixtime>"};

/*!
 * Subset of commands that return streaming output
 */
const std::set<cmd_key_t> streamCmds = {
  {AdminCmd::CMD_ACTIVITYMOUNTRULE,   AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_ADMIN,               AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_ARCHIVEROUTE,        AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_DISKINSTANCE,        AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_DISKINSTANCESPACE,   AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_DISKSYSTEM,          AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_DRIVE,               AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_FAILEDREQUEST,       AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_GROUPMOUNTRULE,      AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_LOGICALLIBRARY,      AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_MEDIATYPE,           AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_MOUNTPOLICY,         AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_RECYCLETAPEFILE,     AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_REPACK,              AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_REQUESTERMOUNTRULE,  AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_SHOWQUEUES,          AdminCmd::SUBCMD_NONE},
  {AdminCmd::CMD_STORAGECLASS,        AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_TAPE,                AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_TAPEFILE,            AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_TAPEPOOL,            AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_VERSION,             AdminCmd::SUBCMD_NONE},
  {AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_LS  },
  {AdminCmd::CMD_PHYSICALLIBRARY,     AdminCmd::SUBCMD_LS  },
};

/**
 * Specify the options for each (command, subcommand) pair
 *
 * The options specified here are checked for validity by cta-admin AND by the CTA Frontend.
 *
 * NOTE: Comment blocks beginning **md are Markdown documentation blocks. These comments will be
 *       included verbatim into the CTA documentation and man pages.
 *
 *       When adding or modifying a command option, take care to include the modifications into
 *       the documentation at the same time. Note that the documentation comments follow a specific
 *       format and indentation is significant.
 *
 *       After modifying this source file, run the following script to generate the man page:
 *
 *           ./compile_man_md.py cta-admin.1cta.md.in
 *
 *       This will generate "cta-admin.1cta.md", which should be added to the git commit.
 */
const std::map<cmd_key_t, cmd_val_t> cmdOptions = {
  /**md
activitymountrule (amr)

:   Add, change, remove or list the activity mount rules. This is provided as an alternative to
    requester mount rules and group mount rules. Activity mount rules allow the scheduling priority
    to be set based on metadata sent by the client rather than the authenticated identity of the
    requestor.
  */
  {{AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_ADD},
   {opt_instance, opt_username_alias, opt_activityregex, opt_mountpolicy, opt_comment}                                       },
  {{AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_CH},
   {opt_instance, opt_username_alias, opt_activityregex, opt_mountpolicy.optional(), opt_comment.optional()}                 },
  {{AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_RM},    {opt_instance, opt_username_alias, opt_activityregex}          },
  {{AdminCmd::CMD_ACTIVITYMOUNTRULE, AdminCmd::SUBCMD_LS},    {}                                                             },

 /**md
admin (ad)

:   Add, change, remove or list the administrators of the system. In order to use **cta-admin**,
    users must be included in the list of administrators, in addition to being authenticated.
  */
  {{AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_ADD},               {opt_username, opt_comment}                                    },
  {{AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_CH},                {opt_username, opt_comment}                                    },
  {{AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_RM},                {opt_username}                                                 },
  {{AdminCmd::CMD_ADMIN, AdminCmd::SUBCMD_LS},                {}                                                             },

 /**md
archiveroute (ar)

:   Add, change, remove or list the archive routes. Archive routes are the policies linking namespace
    entries to tape pools.
  */
  {{AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_ADD},        {opt_storageclass, opt_copynb, opt_tapepool, opt_comment}      },
  {{AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_CH},
   {opt_storageclass, opt_copynb, opt_tapepool.optional(), opt_comment.optional()}                                           },
  {{AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_RM},         {opt_storageclass, opt_copynb}                                 },
  {{AdminCmd::CMD_ARCHIVEROUTE, AdminCmd::SUBCMD_LS},         {}                                                             },

 /**md
diskinstance (di)

:   Add, change, remove or list the disk instances. A disk instance is a separate namespace. A CTA
    installation has one or more disk instances. Multiple disk instances can be configured if it is
    desired to have a separate namespace for each Virtual Organization (VO).

    **\-\-name** specifies the disk instance name, which is the unique identifier of the disk
    instance and cannot be changed.
  */
  {{AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_ADD},        {opt_diskinstance_alias, opt_comment}                          },
  {{AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_CH},         {opt_diskinstance_alias, opt_comment.optional()}               },
  {{AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_RM},         {opt_diskinstance_alias}                                       },
  {{AdminCmd::CMD_DISKINSTANCE, AdminCmd::SUBCMD_LS},         {}                                                             },

 /**md
diskinstancespace (dis)

:   Add, change, remove or list the disk instance spaces. A disk instance can contain zero or more
    disk instance spaces. A disk instance space is a partition of the disk.

    A typical use case for disk instance spaces is to configure separate spaces for archival and
    retrieval operations on each instance.

    **\-\-name** specifies the disk instance space name. The disk instance name (**\-\-diskinstance**)
    and disk instance space name form a pair which is the unique identifier for the disk instance space.

    **\-\-diskinstance** specifies the name of the disk instance to query, e.g. **eosctapublic**

    **\-\-freespacequeryurl** specifies the URL to query the free disk space on this disk instance
    space. It should be specified in the following format:

        <query protocol>:<name of disk space>

    Example:

        eosSpace:retrieve

    **\-\-refreshinterval** specifies how long (in seconds) the cached value of the free space query
    will be used before performing a new query.
  */
  {{AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_ADD},
   {opt_diskinstancespace_alias, opt_diskinstance, opt_free_space_query_url, opt_refresh_interval, opt_comment}              },
  {{AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_CH},
   {opt_diskinstancespace_alias, opt_diskinstance, opt_comment.optional(), opt_free_space_query_url.optional(),
    opt_refresh_interval.optional()}                                                                                         },
  {{AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_RM},    {opt_diskinstancespace_alias, opt_diskinstance}                },
  {{AdminCmd::CMD_DISKINSTANCESPACE, AdminCmd::SUBCMD_LS},    {}                                                             },

 /**md
disksystem (ds)

:   Add, change, remove or list the disk systems. The disk system defines the disk buffer to be used
    for CTA archive and retrieval operations for each VO. It corresponds to a specific directory tree
    on a disk instance space.

    **\-\-disksystem** specifies the unique identifier of the disk system.

    **\-\-diskinstance** and **\-\-diskinstancespace** form a pair which specifies the disk instance
    and partition where this disk system is physically located.

    **\-\-fileregexp** specifies the regular expression to match filenames (from *destinationURL*) to disk systems.

    Example:

        destinationURL root://eos_instance//eos/cta/myfile?eos.lfn=fxid:7&eos.space=spinners

    will match the regular expression:

        ^root://eos_instance//eos/cta(.*)eos.space=spinners

    The two options below are provided to configure backpressure for retrieve operations. Backpressure
    is a mechanism to postpone retrieval of files from tape to the disk buffer when there is insufficient
    disk space. It can be configured separately for each disk system.
    
    Before a retrieve mount, the destination URL of each file is pattern-matched to identify the disk
    system. The corresponding disk instance space is queried to determine if there is sufficient free
    space to perform the mount. If there is insufficient space, the tape server sleeps for the specified
    interval.

    **\-\-targetedfreespace** specifies how much free space should be available before processing a
    batch of retrieve requests. It should be calculated based on the free space update latency (based
    on *diskinstancespace* parameters) and the expected bandwidth for transfers to the external Storage
    Element.

    **\-\-sleeptime** specifies how long (in seconds) to sleep when the disk system has insufficient
    space, before retrying the retrieve mount.
  */
  {{AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_ADD},
   {opt_disksystem, opt_file_regexp, opt_diskinstance, opt_diskinstancespace, opt_targeted_free_space, opt_sleep_time,
    opt_comment}                                                                                                             },
  {{AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_CH},
   {opt_disksystem, opt_file_regexp.optional(), opt_targeted_free_space.optional(), opt_sleep_time.optional(),
    opt_comment.optional()}                                                                                                  },
  {{AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_RM},           {opt_disksystem}                                               },
  {{AdminCmd::CMD_DISKSYSTEM, AdminCmd::SUBCMD_LS},           {}                                                             },

 /**md
drive (dr)

:   Bring tape drives up or down, list tape drives or remove tape drives from the CTA system.

    This is a synchronous command to set and read back the state of one or more tape drives. The
    *drive_name* option accepts a regular expression. If the *drive_name* option is set to **first**,
    the **up**, **down**, **ls** and **ch** commands will scan the local configuration directory
    *\/etc\/cta* and use the drive from the first tape server configuration file found. This does not
    guarantee that the same drive will be used every time.

    **up** puts a drive into active state, able to perform an archive or retrieve mount.

    **down** puts a drive into inactive state, unable to mount. Drives will complete any running session
    and dismount the tape before changing state.

    **\-\-reason** specifies the reason (free text) why the drive is being put down. The reason will
    be displayed in the output of **ls**.

    **ls** lists the drives matching *drive_name*, or all drives by default. An exclamation mark (**!**)
    is displayed in front of the drive name for drives in disabled libraries.

    **rm** deletes the drive definition. Drives must be in the **down** state before deleting.
    (Override with **\-\-force**).
  */
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_UP},                {opt_drivename_cmd, opt_reason.optional()}                     },
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_DOWN},              {opt_drivename_cmd, opt_reason, opt_force_flag.optional()}     },
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_LS},                {opt_drivename_cmd.optional()}                                 },
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_RM},                {opt_drivename_cmd, opt_force_flag.optional()}                 },
  {{AdminCmd::CMD_DRIVE, AdminCmd::SUBCMD_CH},                {opt_drivename_cmd, opt_comment}                               },

 /**md
failedrequest (fr)

:   List and remove requests which failed and for which all retry attempts failed.
  */
  {{AdminCmd::CMD_FAILEDREQUEST, AdminCmd::SUBCMD_LS},
   {opt_justarchive.optional(), opt_justretrieve.optional(), opt_tapepool.optional(), opt_vid.optional(),
    opt_log.optional(), opt_summary.optional()}                                                                              },
  {{AdminCmd::CMD_FAILEDREQUEST, AdminCmd::SUBCMD_RM},        {opt_object_id}                                                },

 /**md
groupmountrule (gmr)

:   Add, change, remove or list the group mount rules.
  */
  {{AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_ADD},
   {opt_instance, opt_groupname_alias, opt_mountpolicy, opt_comment}                                                         },
  {{AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_CH},
   {opt_instance, opt_groupname_alias, opt_mountpolicy.optional(), opt_comment.optional()}                                   },
  {{AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_RM},       {opt_instance, opt_groupname_alias}                            },
  {{AdminCmd::CMD_GROUPMOUNTRULE, AdminCmd::SUBCMD_LS},       {}                                                             },

 /**md
logicallibrary (ll)

:   Add, change, remove or list the logical libraries. Logical libraries are logical groupings of
    tapes and drives based on physical location and tape drive capabilities.

    A tape can be accessed by a drive if it is in the same physical library and if the drive is
    capable of reading or writing the tape. In this case, that tape and that drive should normally
    be in the same logical library.
  */
  {{AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_ADD},
   {opt_logicallibrary_alias, opt_disabled.optional(), opt_physical_library.optional(), opt_comment}                         },
  {{AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_CH},
   {opt_logicallibrary_alias, opt_disabled.optional(), opt_physical_library.optional(), opt_comment.optional(),
    opt_disabledreason.optional()}                                                                                           },
  {{AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_RM},       {opt_logicallibrary_alias}                                     },
  {{AdminCmd::CMD_LOGICALLIBRARY, AdminCmd::SUBCMD_LS},       {opt_logicallibrary_disabled.optional()}                       },

 /**md
mediatype (mt)

:   Add, change, remove or list the tape cartridge media types. This command is used to specify the
    nominal capacity of each media type, which is used to estimate the total capacity of tape pools.

    Optionally, specify the parameters for software Recommended Access Order (LTO-8 or older tape
    technology only). See **cta-taped(1cta)** for details.
  */
  {{AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_ADD},
   {opt_mediatype_alias, opt_cartridge, opt_capacity, opt_primarydensitycode.optional(),
    opt_secondarydensitycode.optional(), opt_number_of_wraps.optional(), opt_minlpos.optional(), opt_maxlpos.optional(),
    opt_comment}                                                                                                             },
  {{AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_CH},
   {opt_mediatype_alias, opt_cartridge.optional(), opt_primarydensitycode.optional(),
    opt_secondarydensitycode.optional(), opt_number_of_wraps.optional(), opt_minlpos.optional(), opt_maxlpos.optional(),
    opt_comment.optional()}                                                                                                  },
  {{AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_RM},            {opt_mediatype_alias}                                          },
  {{AdminCmd::CMD_MEDIATYPE, AdminCmd::SUBCMD_LS},            {}                                                             },

 /**md
mountpolicy (mp)

:   Add, change, remove or list the mount policies.
  */
  {{AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_ADD},
   {opt_mountpolicy_alias, opt_archivepriority, opt_minarchiverequestage, opt_retrievepriority,
    opt_minretrieverequestage, opt_comment}                                                                                  },
  {{AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_CH},
   {opt_mountpolicy_alias, opt_archivepriority.optional(), opt_minarchiverequestage.optional(),
    opt_retrievepriority.optional(), opt_minretrieverequestage.optional(), opt_comment.optional()}                           },
  {{AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_RM},          {opt_mountpolicy_alias}                                        },
  {{AdminCmd::CMD_MOUNTPOLICY, AdminCmd::SUBCMD_LS},          {}                                                             },

 /**md
physicallibrary (pl)

:   Add, change, remove or list the physical tape libraries.
  */
  {{AdminCmd::CMD_PHYSICALLIBRARY, AdminCmd::SUBCMD_ADD},
   {opt_physical_library_alias, opt_manufacturer, opt_model, opt_type.optional(), opt_gui_url.optional(),
    opt_webcam_url.optional(), opt_location.optional(), opt_nb_physical_cartridge_slots,
    opt_nb_available_cartridge_slots.optional(), opt_nb_physical_drive_slots, opt_comment.optional()}                        },
  {{AdminCmd::CMD_PHYSICALLIBRARY, AdminCmd::SUBCMD_CH},
   {opt_physical_library_alias, opt_gui_url.optional(), opt_webcam_url.optional(), opt_location.optional(),
    opt_nb_physical_cartridge_slots.optional(), opt_nb_available_cartridge_slots.optional(),
    opt_nb_physical_drive_slots.optional(), opt_comment.optional()}                                                          },
  {{AdminCmd::CMD_PHYSICALLIBRARY, AdminCmd::SUBCMD_RM},      {opt_physical_library_alias}                                   },
  {{AdminCmd::CMD_PHYSICALLIBRARY, AdminCmd::SUBCMD_LS},      {}                                                             },

 /**md
recycletf (rtf)

:   List tape files in the recycle log.

    Tape files in the recycle log can be listed by VID, EOS disk file ID, EOS disk instance,
    ArchiveFileId or copy number. Disk file IDs should be provided in hexadecimal format (fxid).
  */
  {{AdminCmd::CMD_RECYCLETAPEFILE, AdminCmd::SUBCMD_LS},
   {opt_vid.optional(), opt_fid.optional(), opt_fidfile.optional(), opt_copynb.optional(), opt_archivefileid.optional(),
    opt_instance.optional(), opt_log_unixtime_min.optional(), opt_log_unixtime_max.optional(), opt_vo.optional()}            },

 /**md
repack (re)

:   Add or remove a request to repack one or more tapes, list repack requests in progress, display
    repack errors.

    **add** adds one or more tapes to the active repack requests:

    **\-\-mountpolicy** specifies the mount policy that will be applied to the repack subrequests
    (the retrieve and archive requests).

    **\-\-vid** specifies a single tape to repack.

    **\-\-vidfile** specifies the filename of a text file containing a list of tapes to repack.

    **\-\-bufferurl** optionally specifies the buffer to use in place of the default repack buffer
    URL (specified in the CTA Frontend configuration). It should follow this format:

        root://eos_instance//path/to/repack/buffer

    **\-\-maxfilestoselect** optionally limits the the number of files to be repacked to the specified
    value, overriding the default value (specified in the CTA Frontend configuration). Set the value
    to zero to force all files to be selected.

    **\-\-no-recall** inhibits the retrieve mount. Only files that are already located in the disk
    buffer will be considered for archival.

    By default, CTA will migrate files onto a new tape (or multiple tapes) AND add new (or missing)
    copies of the file. The expected number of copies is defined by the storage class of the file.

    **\-\-justmove** means that the files located on the tape to repack will be migrated onto new
    tape(s), without creating any additional copies.

    **\-\-justaddcopies** means that new (or missing) copies of the files located on the tape to
    repack will be created on the new tape(s), but the source tape file will not be migrated.

    **rm** removes a tape from the list of tapes to repack.

    **ls** lists repack requests in progress. Rows marked with the **\*** flag indicate that not all
    files were selected for repack. The number of files to repack at once can be restricted to avoid
    overloading the Scheduler Database. To repack the remaining files, repeat the operation.

    **err** displays any repack errors.
  */
  {{AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ADD},
   {opt_mountpolicy, opt_vid.optional(), opt_vidfile.optional(), opt_bufferurl.optional(),
    opt_max_files_to_select.optional(), opt_no_recall.optional(), opt_justmove.optional(),
    opt_justaddcopies.optional()}                                                                                            },
  {{AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_RM},               {opt_vid}                                                      },
  {{AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_LS},               {opt_vid.optional()}                                           },
  {{AdminCmd::CMD_REPACK, AdminCmd::SUBCMD_ERR},              {opt_vid}                                                      },

 /**md
requestermountrule (rmr)

:   Add, change, remove or list the requester mount rules.
  */
  {{AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_ADD},
   {opt_instance, opt_username_alias, opt_mountpolicy, opt_comment}                                                          },
  {{AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_CH},
   {opt_instance, opt_username_alias, opt_mountpolicy.optional(), opt_comment.optional()}                                    },
  {{AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_RM},   {opt_instance, opt_username_alias}                             },
  {{AdminCmd::CMD_REQUESTERMOUNTRULE, AdminCmd::SUBCMD_LS},   {}                                                             },

 /**md
showqueues (sq)

:   Show the status of all active queues. The bottom section shows requests already being serviced by
    tape servers, which is why the values of some fields are at zero.
  */
  {{AdminCmd::CMD_SHOWQUEUES, AdminCmd::SUBCMD_NONE},         {}                                                             },

 /**md
storageclass (sc)

:   Add, change, remove or list the storage classes. The storage class of a file specifies its
    expected number of tape copies, and the corresponding tape pool that each copy should be archived
    to.

    In EOS, the storage class is specified as an extended attribute of the directory, which is
    inherited as an extended attribute of the file at creation time.
  */
  {{AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_ADD},        {opt_storageclass_alias, opt_copynb_alias, opt_vo, opt_comment}},
  {{AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_CH},
   {opt_storageclass_alias, opt_copynb_alias.optional(), opt_vo.optional(), opt_comment.optional()}                          },
  {{AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_RM},         {opt_storageclass_alias}                                       },
  {{AdminCmd::CMD_STORAGECLASS, AdminCmd::SUBCMD_LS},         {opt_storageclass_alias.optional()}                            },

 /**md
tape (ta)

:   Add, change, remove, reclaim, list or label tapes. This command is used to manage the physical
    tape cartridges in each library.
  */
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_ADD},
   {opt_vid, opt_mediatype, opt_vendor, opt_logicallibrary, opt_tapepool, opt_full, opt_state.optional(),
    opt_purchase_order.optional(), opt_reason.optional(), opt_comment.optional()}                                            },
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_CH},
   {opt_vid, opt_mediatype.optional(), opt_vendor.optional(), opt_logicallibrary.optional(), opt_tapepool.optional(),
    opt_encryptionkeyname.optional(), opt_full.optional(), opt_verificationstatus.optional(), opt_state.optional(),
    opt_purchase_order.optional(), opt_reason.optional(), opt_comment.optional(), opt_dirtybit.optional()}                   },
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RM},                 {opt_vid}                                                      },
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_RECLAIM},            {opt_vid}                                                      },
  {{AdminCmd::CMD_TAPE, AdminCmd::SUBCMD_LS},
   {opt_vid.optional(), opt_mediatype.optional(), opt_vendor.optional(), opt_logicallibrary.optional(),
    opt_tapepool.optional(), opt_vo.optional(), opt_capacity.optional(), opt_full.optional(), opt_fidfile.optional(),
    opt_all.optional(), opt_state.optional(), opt_fromcastor.optional(), opt_purchase_order.optional(),
    opt_physical_library.optional()}                                                                                         },

 /**md
tapefile (tf)

:   List files on a specified tape or delete a tape file.

    **ls** lists tape files. Which files to list can be specified by VID, the (disk instance,
    disk file ID) pair, or the archive file ID.

    **\-\-vid** specifies the volume identifier (VID) of a tape.

    **\-\-fxid** specifies the disk file ID. EOS disk file IDs should be provided in hexadecimal
    format.

    **\-\-instance** specifies on which disk instance the disk file ID is located.

    **\-\-fxidfile** specifies the filename of a text file containing a list of disk file IDs
    in hexadecimal formal. The format of this file is the same as the output of
    **eos find --fid <path>**.

    **\-\-id** specifies an archive file ID.

    **rm** deletes a single tape file. Note that some disk files have more than one associated tape file.
  */
  {{AdminCmd::CMD_TAPEFILE, AdminCmd::SUBCMD_LS},
   {opt_vid.optional(), opt_instance.optional(), opt_fid.optional(), opt_fidfile.optional(),
    opt_archivefileid.optional()}                                                                                            },
  {{AdminCmd::CMD_TAPEFILE, AdminCmd::SUBCMD_RM},
   {opt_vid, opt_instance.optional(), opt_fid.optional(), opt_archivefileid.optional(), opt_reason}                          },

 /**md
tapepool (tp)

:   Add, change, remove or list tape pools. Tape pools are logical sets of tapes which are used to
    manage the tape lifecycle:

        label → supply pool → user pool → erase → label

    **add** creates a new tapepool:

    **\-\-partialtapesnumber** specifies the number of tapes which should be made available for
    writing in this tape pool. If the number of tapes eligible for writing falls below this number
    (for example, because a tape becomes full), a new tape will be added from a supply pool.

    **\-\-supply** specifies a comma-separated list of tape pools to use as supply pools, when
    adding new (empty) tapes to this tape pool.

    **ls** shows statistics such as the total number of tapes in the pool and number of free tapes.
  */
  {{AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_ADD},
   {opt_tapepool_alias, opt_vo, opt_partialtapes, opt_encrypted, opt_supply.optional(), opt_comment}                         },
  {{AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_CH},
   {opt_tapepool_alias, opt_vo.optional(), opt_partialtapes.optional(), opt_encrypted.optional(), opt_supply.optional(),
    opt_comment.optional()}                                                                                                  },
  {{AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_RM},             {opt_tapepool_alias}                                           },
  {{AdminCmd::CMD_TAPEPOOL, AdminCmd::SUBCMD_LS},
   {opt_tapepool_alias.optional(), opt_vo.optional(), opt_encrypted.optional()}                                              },

 /**md
version (v)

:   Display the version of **cta-admin**, the CTA Frontend, the protocol buffer used for client/server
    communication, and the CTA Catalogue schema.
  */
  {{AdminCmd::CMD_VERSION, AdminCmd::SUBCMD_NONE},            {}                                                             },

 /**md
virtualorganization (vo)

:   Add, change, remove or list the Virtual Organisations (VOs). A VO corresponds to an entity whose
    data transfers and storage should be managed independently of the others, for example an experimental
    collaboration.

    **\-\-virtualorganisation** specifies the name of the virtual organisation. It must be unique.

    **\-\-readmaxdrives** specifies the maximum number of drives the virtual organisation is allowed
    to use for reading>

    **\-\-writemaxdrives** specifies the maximum number of drives the virtual organisation is allowed
    to use for writing.

    **\-\-maxfilesize** specifies the maximum file size for this virtual organisation. Default is 0,
    which means no limit.

    **\-\-isrepackvo** if set to **true**, normal data archival and retrieval is inhibited for this VO;
    it can only be used for repack operations.
  */
  {{AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_ADD},
   {opt_vo, opt_read_max_drives, opt_write_max_drives, opt_comment, opt_diskinstance, opt_maxfilesize.optional(),
    opt_isrepackvo.optional()}                                                                                               },
  {{AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_CH},
   {opt_vo, opt_comment.optional(), opt_read_max_drives.optional(), opt_write_max_drives.optional(),
    opt_maxfilesize.optional(), opt_diskinstance.optional(), opt_isrepackvo.optional()}                                      },
  {{AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_RM},  {opt_vo}                                                       },
  {{AdminCmd::CMD_VIRTUALORGANIZATION, AdminCmd::SUBCMD_LS},  {}                                                             },

 /*-------------------------------------------------------------------------------------------------------------------------
   COMMANDS DEFINED IN CTA FRONTEND BUT NOT AVAILABLE TO CTA-ADMIN
   -------------------------------------------------------------------------------------------------------------------------*/

  // Used by cta-change-storageclass and cta-eos-namespace-inject
  {{AdminCmd::CMD_ARCHIVEFILE, AdminCmd::SUBCMD_CH},
   {opt_storageclass.optional(), opt_archive_file_ids, opt_fid.optional(), opt_diskinstance.optional()}                      },
 // Used by cta-restore-deleted-files
  {{AdminCmd::CMD_RECYCLETAPEFILE, AdminCmd::SUBCMD_RESTORE},
   {opt_vid.optional(), opt_fid, opt_copynb.optional(), opt_archivefileid.optional(), opt_instance.optional()}               },
 /*-------------------------------------------------------------------------------------------------------------------------*/
};

/*!
 * Validate that all required command line options are present
 *
 * Throws a std::runtime_error if the command is invalid
 * This function is used on the server side
 */
void validateCmd(const cta::admin::AdminCmd &admincmd);
} // namespace cta::admin
