/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Definitions for parsing the options of the CTA Admin command-line tool
 * @description    CTA Admin command using Google Protocol Buffers and XRootD SSI transport
 * @copyright      Copyright 2017 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <map>
#include <set>
#include <string>

#include "CtaFrontendApi.hpp"

namespace cta {
namespace admin {

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
      m_long_opt(long_opt),
      m_short_opt(short_opt),
      m_help_txt(help_txt),
      m_is_optional(false) {
        m_lookup_key = (alias.size() == 0) ? long_opt : alias;
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
   { "archivefile",             AdminCmd::CMD_ARCHIVEFILE },
   { "af",                      AdminCmd::CMD_ARCHIVEFILE },
   { "archiveroute",            AdminCmd::CMD_ARCHIVEROUTE },
   { "ar",                      AdminCmd::CMD_ARCHIVEROUTE },
   { "drive",                   AdminCmd::CMD_DRIVE },
   { "dr",                      AdminCmd::CMD_DRIVE },
   { "failedrequest",           AdminCmd::CMD_FAILEDREQUEST },
   { "fr",                      AdminCmd::CMD_FAILEDREQUEST },
   { "groupmountrule",          AdminCmd::CMD_GROUPMOUNTRULE },
   { "gmr",                     AdminCmd::CMD_GROUPMOUNTRULE },
   { "listpendingarchives",     AdminCmd::CMD_LISTPENDINGARCHIVES },
   { "lpa",                     AdminCmd::CMD_LISTPENDINGARCHIVES },
   { "listpendingretrieves",    AdminCmd::CMD_LISTPENDINGRETRIEVES },
   { "lpr",                     AdminCmd::CMD_LISTPENDINGRETRIEVES },
   { "logicallibrary",          AdminCmd::CMD_LOGICALLIBRARY },
   { "ll",                      AdminCmd::CMD_LOGICALLIBRARY },
   { "mountpolicy",             AdminCmd::CMD_MOUNTPOLICY },
   { "mp",                      AdminCmd::CMD_MOUNTPOLICY },
   { "repack",                  AdminCmd::CMD_REPACK },
   { "re",                      AdminCmd::CMD_REPACK },
   { "requestermountrule",      AdminCmd::CMD_REQUESTERMOUNTRULE },
   { "rmr",                     AdminCmd::CMD_REQUESTERMOUNTRULE },
   { "showqueues",              AdminCmd::CMD_SHOWQUEUES },
   { "sq",                      AdminCmd::CMD_SHOWQUEUES },
   { "shrink",                  AdminCmd::CMD_SHRINK },
   { "sh",                      AdminCmd::CMD_SHRINK },
   { "storageclass",            AdminCmd::CMD_STORAGECLASS },
   { "sc",                      AdminCmd::CMD_STORAGECLASS },
   { "tape",                    AdminCmd::CMD_TAPE },
   { "ta",                      AdminCmd::CMD_TAPE },
   { "tapepool",                AdminCmd::CMD_TAPEPOOL },
   { "tp",                      AdminCmd::CMD_TAPEPOOL },
   { "test",                    AdminCmd::CMD_TEST },
   { "te",                      AdminCmd::CMD_TEST },
   { "verify",                  AdminCmd::CMD_VERIFY },
   { "ve",                      AdminCmd::CMD_VERIFY }
};



/*!
 * Map subcommand names to Protocol Buffer enum values
 */
const subcmdLookup_t subcmdLookup = {
   { "add",                     AdminCmd::SUBCMD_ADD },
   { "ch",                      AdminCmd::SUBCMD_CH },
   { "err",                     AdminCmd::SUBCMD_ERR },
   { "label",                   AdminCmd::SUBCMD_LABEL },
   { "ls",                      AdminCmd::SUBCMD_LS },
   { "reclaim",                 AdminCmd::SUBCMD_RECLAIM },
   { "retry",                   AdminCmd::SUBCMD_RETRY },
   { "rm",                      AdminCmd::SUBCMD_RM },
   { "show",                    AdminCmd::SUBCMD_SHOW },
   { "up",                      AdminCmd::SUBCMD_UP },
   { "down",                    AdminCmd::SUBCMD_DOWN },
   { "read",                    AdminCmd::SUBCMD_READ },
   { "write",                   AdminCmd::SUBCMD_WRITE },
   { "write_auto",              AdminCmd::SUBCMD_WRITE_AUTO }
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
   { "--lbp",                   OptionBoolean::LBP },

   // hasOption options
   { "--checkchecksum",         OptionBoolean::CHECK_CHECKSUM },
   { "--extended",              OptionBoolean::EXTENDED },
   { "--header",                OptionBoolean::SHOW_HEADER },
   { "--justarchive",           OptionBoolean::JUSTARCHIVE },
   { "--justrepack",            OptionBoolean::JUSTREPACK },
   { "--justexpand",            OptionBoolean::JUSTEXPAND },
   { "--justretrieve",          OptionBoolean::JUSTRETRIEVE },
   { "--log",                   OptionBoolean::SHOW_LOG_ENTRIES },
   { "--summary",               OptionBoolean::SUMMARY }
};



/*!
 * Map integer options to Protocol Buffer enum values
 */
const std::map<std::string, OptionUInt64::Key> uint64Options = {
   { "--archivepriority",       OptionUInt64::ARCHIVE_PRIORITY },
   { "--capacity",              OptionUInt64::CAPACITY },
   { "--copynb",                OptionUInt64::COPY_NUMBER },
   { "--firstfseq",             OptionUInt64::FIRST_FSEQ },
   { "--id",                    OptionUInt64::ARCHIVE_FILE_ID },
   { "--lastfseq",              OptionUInt64::LAST_FSEQ },
   { "--maxdrivesallowed",      OptionUInt64::MAX_DRIVES_ALLOWED },
   { "--minarchiverequestage",  OptionUInt64::MIN_ARCHIVE_REQUEST_AGE },
   { "--minretrieverequestage", OptionUInt64::MIN_RETRIEVE_REQUEST_AGE },
   { "--nbfiles",               OptionUInt64::NUMBER_OF_FILES },
   { "--partial",               OptionUInt64::PARTIAL }, 
   { "--partialtapesnumber",    OptionUInt64::PARTIAL_TAPES_NUMBER },
   { "--retrievepriority",      OptionUInt64::RETRIEVE_PRIORITY },
   { "--size",                  OptionUInt64::FILE_SIZE }                  
};



/*!
 * Map string options to Protocol Buffer enum values
 */
const std::map<std::string, OptionString::Key> strOptions = {
   { "--bufferurl",             OptionString::BUFFERURL }, 
   { "--comment",               OptionString::COMMENT },
   { "--diskid",                OptionString::DISKID },
   { "--drive",                 OptionString::DRIVE },
   { "--encryptionkey",         OptionString::ENCRYPTION_KEY },
   { "--file",                  OptionString::FILENAME },
   { "--group",                 OptionString::GROUP },
   { "--hostname",              OptionString::HOSTNAME },
   { "--input",                 OptionString::INPUT },
   { "--instance",              OptionString::INSTANCE },
   { "--logicallibrary",        OptionString::LOGICAL_LIBRARY },
   { "--mediatype",             OptionString::MEDIA_TYPE },
   { "--mountpolicy",           OptionString::MOUNT_POLICY },
   { "--output",                OptionString::OUTPUT },
   { "--owner",                 OptionString::OWNER },
   { "--path",                  OptionString::PATH },
   { "--storageclass",          OptionString::STORAGE_CLASS },
   { "--tapepool",              OptionString::TAPE_POOL },
   { "--username",              OptionString::USERNAME },
   { "--vendor",                OptionString::VENDOR },
   { "--vid",                   OptionString::VID },
   { "--vo",                    OptionString::VO }
};



/*!
 * Map string list options to Protocol Buffer enum values
 */
const std::map<std::string, OptionStrList::Key> strListOptions = {
   { "--vidfile",               OptionStrList::VID }
};



/*!
 * Specify the help text for commands
 */
const std::map<AdminCmd::Cmd, CmdHelp> cmdHelp = {
   { AdminCmd::CMD_ADMIN,                { "admin",                "ad",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_ARCHIVEFILE,          { "archivefile",          "af",  { "ls" } }},
   { AdminCmd::CMD_ARCHIVEROUTE,         { "archiveroute",         "ar",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_DRIVE,                { "drive",                "dr",  { "up", "down", "ls", "rm" },
                        "\n  This is a synchronous command that sets and reads back the state of one or\n"
                          "  more drives. The <drive_name> option accepts a regular expression. If the\n"
                          "  --force option is not set, the drives will complete any running mount and\n"
                          "  drives must be in the down state before deleting.\n\n"
                                         }},
   { AdminCmd::CMD_FAILEDREQUEST,        { "failedrequest",        "fr",  { "ls", "show", "retry", "rm" } }},
   { AdminCmd::CMD_GROUPMOUNTRULE,       { "groupmountrule",       "gmr", { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_LISTPENDINGARCHIVES,  { "listpendingarchives",  "lpa", { } }},
   { AdminCmd::CMD_LISTPENDINGRETRIEVES, { "listpendingretrieves", "lpr", { } }},
   { AdminCmd::CMD_LOGICALLIBRARY,       { "logicallibrary",       "ll",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_MOUNTPOLICY,          { "mountpolicy",          "mp",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_REPACK,               { "repack",               "re",  { "add", "rm", "ls", "err" } }},
   { AdminCmd::CMD_REQUESTERMOUNTRULE,   { "requestermountrule",   "rmr", { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_SHOWQUEUES,           { "showqueues",           "sq",  { } }},
   { AdminCmd::CMD_SHRINK,               { "shrink",               "sh",  { } }},
   { AdminCmd::CMD_STORAGECLASS,         { "storageclass",         "sc",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_TAPE,                 { "tape",                 "ta",  { "add", "ch", "rm", "reclaim", "ls", "label" } }},
   { AdminCmd::CMD_TAPEPOOL,             { "tapepool",             "tp",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_TEST,                 { "test",                 "te",  { "read", "write", "write_auto" },
                        "\n  This is a synchronous command that returns performance stats and errors.\n"
                          "  It should be run on an empty self-dedicated drive. All locations are local\n"
                          "  to the tapeserver.\n\n"
                                         }},
   { AdminCmd::CMD_VERIFY,               { "verify",               "ve",  { "add", "rm", "ls", "err" } }}
};



/*
 * Enumerate options
 */
const Option opt_all                  { Option::OPT_FLAG, "--all",                   "-a",   "" };
const Option opt_archivefileid        { Option::OPT_UINT, "--id",                    "-I",   " <archive_file_id>" };
const Option opt_archivepriority      { Option::OPT_UINT, "--archivepriority",       "--ap", " <priority_value>" };
const Option opt_bufferurl            { Option::OPT_STR,  "--bufferurl",             "-b",   " <buffer URL>" };
const Option opt_capacity             { Option::OPT_UINT, "--capacity",              "-c",   " <capacity_in_bytes>" };
const Option opt_checkchecksum        { Option::OPT_FLAG, "--checkchecksum",         "-c",   "" };
const Option opt_comment              { Option::OPT_STR,  "--comment",               "-m",   " <\"comment\">" };
const Option opt_copynb               { Option::OPT_UINT, "--copynb",                "-c",   " <copy_number>" };
const Option opt_disabled             { Option::OPT_BOOL, "--disabled",              "-d",   " <\"true\" or \"false\">" };
const Option opt_diskid               { Option::OPT_STR,  "--diskid",                "-d",   " <disk_id>" };
const Option opt_drivename            { Option::OPT_STR,  "--drive",                 "-d",   " <drive_name>" };
const Option opt_drivename_cmd        { Option::OPT_CMD,  "--drive",                 "",     "<drive_name>" };
const Option opt_encrypted            { Option::OPT_BOOL, "--encrypted",             "-e",   " <\"true\" or \"false\">" };
const Option opt_encryptionkey        { Option::OPT_STR,  "--encryptionkey",         "-k",   " <encryption_key>" };
const Option opt_extended             { Option::OPT_FLAG, "--extended",              "-x",   "" };
const Option opt_filename             { Option::OPT_STR,  "--file",                  "-f",   " <filename>" };
const Option opt_firstfseq            { Option::OPT_UINT, "--firstfseq",             "-f",   " <first_fseq>" };
const Option opt_force                { Option::OPT_BOOL, "--force",                 "-f",   " <\"true\" or \"false\">" };
const Option opt_force_flag           { Option::OPT_FLAG, "--force",                 "-f",   "" };
const Option opt_group                { Option::OPT_STR,  "--group",                 "-g",   " <group>" };
const Option opt_header               { Option::OPT_FLAG, "--header",                "-h",   "" };
const Option opt_hostname_alias       { Option::OPT_STR,  "--name",                  "-n",   " <host_name>",
                                        "--hostname" };
const Option opt_input                { Option::OPT_STR,  "--input",                 "-i",   " <\"zero\" or \"urandom\">" };
const Option opt_instance             { Option::OPT_STR,  "--instance",              "-i",   " <instance_name>" };
const Option opt_justarchive          { Option::OPT_FLAG, "--justarchive",           "-a",   "" };
const Option opt_justrepack           { Option::OPT_FLAG, "--justrepack",            "-r",   "" };
const Option opt_justexpand           { Option::OPT_FLAG, "--justexpand",            "-e",   "" };
const Option opt_justretrieve         { Option::OPT_FLAG, "--justretrieve",          "-r",   "" };
const Option opt_lastfseq             { Option::OPT_UINT, "--lastfseq",              "-l",   " <last_fseq>" };
const Option opt_lbp                  { Option::OPT_BOOL, "--lbp",                   "-p",   " <\"true\" or \"false\">" };
const Option opt_log                  { Option::OPT_FLAG, "--log",                   "-l",   "" };
const Option opt_logicallibrary       { Option::OPT_STR,  "--logicallibrary",        "-l",   " <logical_library_name>" };
const Option opt_logicallibrary_alias { Option::OPT_STR,  "--name",                  "-n",   " <logical_library_name>",
                                        "--logicallibrary" };
const Option opt_maxdrivesallowed     { Option::OPT_UINT, "--maxdrivesallowed",      "-d",   " <max_drives_allowed>" };
const Option opt_mediatype            { Option::OPT_STR,  "--mediatype",             "--mt", " <media_type>" };
const Option opt_minarchiverequestage { Option::OPT_UINT, "--minarchiverequestage",  "--aa", " <min_request_age>" };
const Option opt_minretrieverequestage{ Option::OPT_UINT, "--minretrieverequestage", "--ra", " <min_request_age>" };
const Option opt_mountpolicy          { Option::OPT_STR,  "--mountpolicy",           "-u",   " <mount_policy_name>" };
const Option opt_mountpolicy_alias    { Option::OPT_STR,  "--name",                  "-n",   " <mount_policy_name>",
                                        "--mountpolicy" };
const Option opt_number_of_files      { Option::OPT_UINT, "--nbfiles",               "-n",   " <number_of_files_per_tape>" };
const Option opt_number_of_files_alias{ Option::OPT_UINT, "--number",                "-n",   " <number_of_files>",
                                        "--nbfiles" };
const Option opt_output               { Option::OPT_STR,  "--output",                "-o",   " <\"null\" or output_dir>" };
const Option opt_owner                { Option::OPT_STR,  "--owner",                 "-o",   " <owner>" };
const Option opt_partialfiles         { Option::OPT_UINT, "--partial",               "-p",   " <number_of_files_per_tape>" };
const Option opt_partialtapes         { Option::OPT_UINT, "--partialtapesnumber",    "-p",   " <number_of_partial_tapes>" };
const Option opt_path                 { Option::OPT_STR,  "--path",                  "-p",   " <full_path>" };
const Option opt_retrievepriority     { Option::OPT_UINT, "--retrievepriority",      "--rp", " <priority_value>" };
const Option opt_size                 { Option::OPT_UINT, "--size",                  "-s",   " <file_size>" };
const Option opt_storageclass         { Option::OPT_STR,  "--storageclass",          "-s",   " <storage_class_name>" };
const Option opt_storageclass_alias   { Option::OPT_STR,  "--name",                  "-n",   " <storage_class_name>",
                                        "--storageclass" };
const Option opt_summary              { Option::OPT_FLAG, "--summary",               "-S",   "" };
const Option opt_tapepool             { Option::OPT_STR,  "--tapepool",              "-t",   " <tapepool_name>" };
const Option opt_tapepool_alias       { Option::OPT_STR,  "--name",                  "-n",   " <tapepool_name>",
                                        "--tapepool" };
const Option opt_username             { Option::OPT_STR,  "--username",              "-u",   " <user_name>" };
const Option opt_username_alias       { Option::OPT_STR,  "--name",                  "-n",   " <user_name>",
                                        "--username" };
const Option opt_vendor               { Option::OPT_STR,  "--vendor",                "--ve", " <vendor>" };
const Option opt_vid                  { Option::OPT_STR,  "--vid",                   "-v",   " <vid>" };
const Option opt_vo                   { Option::OPT_STR,  "--vo",                    "--vo", " <vo>" };
const Option opt_vidfile              { Option::OPT_STR_LIST, "--vidfile",           "-f",   " <filename>" };
const Option opt_full                 { Option::OPT_BOOL, "--full",                  "-f",   " <\"true\" or \"false\">" };



/*!
 * Map valid options to commands
 */
const std::map<cmd_key_t, cmd_val_t> cmdOptions = {
   {{ AdminCmd::CMD_ADMIN,                AdminCmd::SUBCMD_ADD   }, { opt_username, opt_comment }},
   {{ AdminCmd::CMD_ADMIN,                AdminCmd::SUBCMD_CH    }, { opt_username, opt_comment }},
   {{ AdminCmd::CMD_ADMIN,                AdminCmd::SUBCMD_RM    }, { opt_username }},
   {{ AdminCmd::CMD_ADMIN,                AdminCmd::SUBCMD_LS    }, { opt_header.optional() }},
   {{ AdminCmd::CMD_ARCHIVEFILE,          AdminCmd::SUBCMD_LS    },
      { opt_header.optional(), opt_archivefileid.optional(), opt_diskid.optional(), opt_copynb.optional(),
        opt_vid.optional(), opt_tapepool.optional(), opt_owner.optional(), opt_group.optional(),
        opt_storageclass.optional(), opt_path.optional(), opt_instance.optional(), opt_all.optional(),
        opt_summary.optional() }},
   {{ AdminCmd::CMD_ARCHIVEROUTE,         AdminCmd::SUBCMD_ADD   },
      { opt_instance, opt_storageclass, opt_copynb, opt_tapepool, opt_comment }},
   {{ AdminCmd::CMD_ARCHIVEROUTE,         AdminCmd::SUBCMD_CH    },
      { opt_instance, opt_storageclass, opt_copynb, opt_tapepool.optional(), opt_comment.optional()
                                                                    }},
   {{ AdminCmd::CMD_ARCHIVEROUTE,         AdminCmd::SUBCMD_RM    }, { opt_instance, opt_storageclass, opt_copynb }},
   {{ AdminCmd::CMD_ARCHIVEROUTE,         AdminCmd::SUBCMD_LS    }, { opt_header.optional() }},
   {{ AdminCmd::CMD_DRIVE,                AdminCmd::SUBCMD_UP    }, { opt_drivename_cmd }},
   {{ AdminCmd::CMD_DRIVE,                AdminCmd::SUBCMD_DOWN  }, { opt_drivename_cmd, opt_force_flag.optional() }},
   {{ AdminCmd::CMD_DRIVE,                AdminCmd::SUBCMD_LS    }, { opt_drivename_cmd.optional() }},
   {{ AdminCmd::CMD_DRIVE,                AdminCmd::SUBCMD_RM    }, { opt_drivename_cmd, opt_force_flag.optional() }},
   {{ AdminCmd::CMD_FAILEDREQUEST,        AdminCmd::SUBCMD_LS    },
      { opt_header.optional(), opt_justarchive.optional(), opt_justretrieve.optional(), opt_log.optional(), opt_summary.optional() }},
   {{ AdminCmd::CMD_FAILEDREQUEST,        AdminCmd::SUBCMD_SHOW  }, { opt_copynb.optional() }},
   {{ AdminCmd::CMD_FAILEDREQUEST,        AdminCmd::SUBCMD_RETRY }, { opt_copynb.optional() }},
   {{ AdminCmd::CMD_FAILEDREQUEST,        AdminCmd::SUBCMD_RM    }, { opt_copynb.optional() }},
   {{ AdminCmd::CMD_GROUPMOUNTRULE,       AdminCmd::SUBCMD_ADD   },
      { opt_instance, opt_username_alias, opt_mountpolicy, opt_comment }},
   {{ AdminCmd::CMD_GROUPMOUNTRULE,       AdminCmd::SUBCMD_CH    },
      { opt_instance, opt_username_alias, opt_mountpolicy.optional(), opt_comment.optional() }},
   {{ AdminCmd::CMD_GROUPMOUNTRULE,       AdminCmd::SUBCMD_RM    }, { opt_instance, opt_username_alias }},
   {{ AdminCmd::CMD_GROUPMOUNTRULE,       AdminCmd::SUBCMD_LS    }, { opt_header.optional() }},
   {{ AdminCmd::CMD_LISTPENDINGARCHIVES,  AdminCmd::SUBCMD_NONE  },
      { opt_header.optional(), opt_tapepool.optional(), opt_extended.optional() }},
   {{ AdminCmd::CMD_LISTPENDINGRETRIEVES, AdminCmd::SUBCMD_NONE  },
      { opt_header.optional(), opt_vid.optional(), opt_extended.optional() }},
   {{ AdminCmd::CMD_LOGICALLIBRARY,       AdminCmd::SUBCMD_ADD   }, { opt_logicallibrary_alias, opt_comment }},
   {{ AdminCmd::CMD_LOGICALLIBRARY,       AdminCmd::SUBCMD_CH    }, { opt_logicallibrary_alias, opt_comment }},
   {{ AdminCmd::CMD_LOGICALLIBRARY,       AdminCmd::SUBCMD_RM    }, { opt_logicallibrary_alias }},
   {{ AdminCmd::CMD_LOGICALLIBRARY,       AdminCmd::SUBCMD_LS    }, { opt_header.optional() }},
   {{ AdminCmd::CMD_MOUNTPOLICY,          AdminCmd::SUBCMD_ADD   },
      { opt_mountpolicy_alias, opt_archivepriority, opt_minarchiverequestage, opt_retrievepriority,
        opt_minretrieverequestage, opt_maxdrivesallowed, opt_comment }},
   {{ AdminCmd::CMD_MOUNTPOLICY,          AdminCmd::SUBCMD_CH    },
      { opt_mountpolicy_alias, opt_archivepriority.optional(), opt_minarchiverequestage.optional(),
        opt_retrievepriority.optional(), opt_minretrieverequestage.optional(), opt_maxdrivesallowed.optional(),
        opt_comment.optional() }},
   {{ AdminCmd::CMD_MOUNTPOLICY,          AdminCmd::SUBCMD_RM    }, { opt_mountpolicy_alias }},
   {{ AdminCmd::CMD_MOUNTPOLICY,          AdminCmd::SUBCMD_LS    }, { opt_header.optional() }},
   {{ AdminCmd::CMD_REPACK,               AdminCmd::SUBCMD_ADD   },
      { opt_vid.optional(), opt_vidfile.optional(), opt_bufferurl, opt_justexpand.optional(), opt_justrepack.optional() }},
   {{ AdminCmd::CMD_REPACK,               AdminCmd::SUBCMD_RM    }, { opt_vid }},
   {{ AdminCmd::CMD_REPACK,               AdminCmd::SUBCMD_LS    }, { opt_header.optional(), opt_vid.optional() }},
   {{ AdminCmd::CMD_REPACK,               AdminCmd::SUBCMD_ERR   }, { opt_vid }},
   {{ AdminCmd::CMD_REQUESTERMOUNTRULE,   AdminCmd::SUBCMD_ADD   },
      { opt_instance, opt_username_alias, opt_mountpolicy, opt_comment }},
   {{ AdminCmd::CMD_REQUESTERMOUNTRULE,   AdminCmd::SUBCMD_CH    },
      { opt_instance, opt_username_alias, opt_mountpolicy.optional(), opt_comment.optional() }},
   {{ AdminCmd::CMD_REQUESTERMOUNTRULE,   AdminCmd::SUBCMD_RM    }, { opt_instance, opt_username_alias }},
   {{ AdminCmd::CMD_REQUESTERMOUNTRULE,   AdminCmd::SUBCMD_LS    }, { opt_header.optional() }},
   {{ AdminCmd::CMD_SHRINK,               AdminCmd::SUBCMD_NONE  }, { opt_tapepool }},
   {{ AdminCmd::CMD_SHOWQUEUES,           AdminCmd::SUBCMD_NONE  }, { opt_header.optional() }},
   {{ AdminCmd::CMD_STORAGECLASS,         AdminCmd::SUBCMD_ADD   },
      { opt_instance, opt_storageclass_alias, opt_copynb, opt_comment }},
   {{ AdminCmd::CMD_STORAGECLASS,         AdminCmd::SUBCMD_CH    },
      { opt_instance, opt_storageclass_alias, opt_copynb.optional(), opt_comment.optional() }},
   {{ AdminCmd::CMD_STORAGECLASS,         AdminCmd::SUBCMD_RM    }, { opt_instance, opt_storageclass_alias }},
   {{ AdminCmd::CMD_STORAGECLASS,         AdminCmd::SUBCMD_LS    }, { opt_header.optional() }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_ADD   },
      { opt_vid, opt_mediatype, opt_vendor, opt_logicallibrary, opt_tapepool, opt_capacity, opt_disabled, opt_full,
        opt_comment.optional() }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_CH    },
      { opt_vid, opt_mediatype.optional(), opt_vendor.optional(), opt_logicallibrary.optional(),
        opt_tapepool.optional(), opt_capacity.optional(), opt_encryptionkey.optional(), opt_disabled.optional(),
        opt_full.optional(), opt_comment.optional() }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_RM    }, { opt_vid }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_RECLAIM }, { opt_vid }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_LS    },
      { opt_header.optional(), opt_vid.optional(), opt_mediatype.optional(), opt_vendor.optional(),
        opt_logicallibrary.optional(), opt_tapepool.optional(), opt_vo.optional(), opt_capacity.optional(),
        opt_lbp.optional(), opt_disabled.optional(), opt_full.optional(), opt_all.optional() }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_LABEL },
      { opt_vid, opt_force.optional(), opt_lbp.optional() }},
   {{ AdminCmd::CMD_TAPEPOOL,             AdminCmd::SUBCMD_ADD   },
      { opt_tapepool_alias, opt_vo, opt_partialtapes, opt_encrypted, opt_comment }},
   {{ AdminCmd::CMD_TAPEPOOL,             AdminCmd::SUBCMD_CH    },
      { opt_tapepool_alias, opt_vo.optional(), opt_partialtapes.optional(), opt_encrypted.optional(), opt_comment.optional() }},
   {{ AdminCmd::CMD_TAPEPOOL,             AdminCmd::SUBCMD_RM    }, { opt_tapepool_alias }},
   {{ AdminCmd::CMD_TAPEPOOL,             AdminCmd::SUBCMD_LS    }, { opt_header.optional() }},
   {{ AdminCmd::CMD_TEST,                 AdminCmd::SUBCMD_READ  },
      { opt_drivename, opt_vid, opt_firstfseq, opt_lastfseq, opt_checkchecksum, opt_output }},
   {{ AdminCmd::CMD_TEST,                 AdminCmd::SUBCMD_WRITE },
      { opt_drivename, opt_vid, opt_filename }},
   {{ AdminCmd::CMD_TEST,                 AdminCmd::SUBCMD_WRITE_AUTO },
      { opt_drivename, opt_vid, opt_number_of_files_alias, opt_size, opt_input }},
   {{ AdminCmd::CMD_VERIFY,               AdminCmd::SUBCMD_ADD   },
      { opt_vid, opt_number_of_files.optional() }},
   {{ AdminCmd::CMD_VERIFY,               AdminCmd::SUBCMD_RM    }, { opt_vid }},
   {{ AdminCmd::CMD_VERIFY,               AdminCmd::SUBCMD_LS    }, { opt_header.optional(), opt_vid.optional() }},
   {{ AdminCmd::CMD_VERIFY,               AdminCmd::SUBCMD_ERR   }, { opt_vid }},
};



/*!
 * Validate that all required command line options are present
 *
 * Throws a std::runtime_error if the command is invalid
 */
void validateCmd(const cta::admin::AdminCmd &admincmd);

}} // namespace cta::admin
