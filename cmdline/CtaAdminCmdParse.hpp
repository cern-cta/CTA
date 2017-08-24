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

#include "xroot_plugins/messages/CtaFrontendApi.hpp"

namespace cta {
namespace admin {

/*!
 * Command line option class
 */
class Option
{
public:
   enum option_t { OPT_PARAM, OPT_FLAG, OPT_BOOL, OPT_INT, OPT_STR };

   /*!
    * Constructor
    */
   Option(option_t type, const std::string &long_opt, const std::string &short_opt, const std::string &help_txt, const std::string &alias = "") :
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
    * Return per-option help string
    */
   std::string help() {
      std::string help = m_is_optional ? " [" : " ";
      help += (m_type == OPT_PARAM) ? "" : m_long_opt + '/' + m_short_opt;
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



/*!
 * Command/subcommand help class
 */
class CmdHelp
{
public:
   /*!
    * Constructor
    */
   CmdHelp(const std::string &cmd_long, const std::string &cmd_short, const std::vector<std::string> &sub_cmd, const std::string &help_txt = "") :
      m_cmd_long(cmd_long),
      m_cmd_short(cmd_short),
      m_sub_cmd(sub_cmd),
      m_help_txt(help_txt) {}

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
   std::string              m_cmd_long;     //!< Command string (long version)
   std::string              m_cmd_short;    //!< Command string (short version)
   std::vector<std::string> m_sub_cmd;      //!< Subcommands which are valid for this command, in the
                                            //!< same order that they should be displayed in the help
   std::string              m_help_txt;     //!< Optional extra help text above the options
};



/*
 * Type aliases
 */
using cmdLookup_t    = std::map<std::string, AdminCmd::Cmd>;
using subcmdLookup_t = std::map<std::string, AdminCmd::SubCmd>;
using cmd_key_t      = std::pair<AdminCmd::Cmd, AdminCmd::SubCmd>;
using cmd_val_t      = std::vector<Option>;



/*!
 * Map short and long command names to Protocol Buffer enum values
 */
const cmdLookup_t cmdLookup = {
   { "admin",                   AdminCmd::CMD_ADMIN },
   { "ad",                      AdminCmd::CMD_ADMIN },
   { "adminhost",               AdminCmd::CMD_ADMINHOST },
   { "ah",                      AdminCmd::CMD_ADMINHOST },
   { "archivefile",             AdminCmd::CMD_ARCHIVEFILE },
   { "af",                      AdminCmd::CMD_ARCHIVEFILE },
   { "archiveroute",            AdminCmd::CMD_ARCHIVEROUTE },
   { "ar",                      AdminCmd::CMD_ARCHIVEROUTE },
   { "drive",                   AdminCmd::CMD_DRIVE },
   { "dr",                      AdminCmd::CMD_DRIVE },
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
   { "rm",                      AdminCmd::SUBCMD_RM },
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
   { "--all",                   OptionBoolean::BOOL_ALL },
   { "--disabled",              OptionBoolean::BOOL_DISABLED },
   { "--encrypted",             OptionBoolean::BOOL_ENCRYPTED },
   { "--force",                 OptionBoolean::BOOL_FORCE },
   { "--full",                  OptionBoolean::BOOL_FULL },
   { "--lbp",                   OptionBoolean::BOOL_LBP },

   // hasOption options
   { "--checkchecksum",         OptionBoolean::BOOL_CHECK_CHECKSUM },
   { "--extended",              OptionBoolean::BOOL_EXTENDED },
   { "--header",                OptionBoolean::BOOL_SHOW_HEADER },
   { "--justexpand",            OptionBoolean::BOOL_JUSTEXPAND },
   { "--justrepack",            OptionBoolean::BOOL_JUSTREPACK },
   { "--summary",               OptionBoolean::BOOL_SUMMARY }
};



/*!
 * Map integer options to Protocol Buffer enum values
 */
const std::map<std::string, OptionInteger::Key> intOptions = {
   { "--archivepriority",       OptionInteger::INT_ARCHIVE_PRIORITY },
   { "--capacity",              OptionInteger::INT_CAPACITY },
   { "--copynb",                OptionInteger::INT_COPY_NUMBER },
   { "--firstfseq",             OptionInteger::INT_FIRST_FSEQ },
   { "--id",                    OptionInteger::INT_ARCHIVE_FILE_ID },
   { "--lastfseq",              OptionInteger::INT_LAST_FSEQ },
   { "--maxdrivesallowed",      OptionInteger::INT_MAX_DRIVES_ALLOWED },
   { "--minarchiverequestage",  OptionInteger::INT_MIN_ARCHIVE_REQUEST_AGE },
   { "--minretrieverequestage", OptionInteger::INT_MIN_RETRIEVE_REQUEST_AGE },
   { "--number",                OptionInteger::INT_NUMBER_OF_FILES },
   { "--partial",               OptionInteger::INT_PARTIAL }, 
   { "--partialtapesnumber",    OptionInteger::INT_PARTIAL_TAPES_NUMBER },
   { "--retrievepriority",      OptionInteger::INT_RETRIEVE_PRIORITY },
   { "--size",                  OptionInteger::INT_FILE_SIZE }                  
};



/*!
 * Map string options to Protocol Buffer enum values
 */
const std::map<std::string, OptionString::Key> strOptions = {
   { "--comment",               OptionString::STR_COMMENT },
   { "--diskid",                OptionString::STR_DISKID },
   { "--drive",                 OptionString::STR_DRIVE },
   { "--encryptionkey",         OptionString::STR_ENCRYPTION_KEY },
   { "--file",                  OptionString::STR_FILENAME },
   { "--group",                 OptionString::STR_GROUP },
   { "--hostname",              OptionString::STR_HOSTNAME },
   { "--input",                 OptionString::STR_INPUT },
   { "--instance",              OptionString::STR_INSTANCE },
   { "--logicallibrary",        OptionString::STR_LOGICAL_LIBRARY },
   { "--mountpolicy",           OptionString::STR_MOUNT_POLICY },
   { "--output",                OptionString::STR_OUTPUT },
   { "--owner",                 OptionString::STR_OWNER },
   { "--path",                  OptionString::STR_PATH },
   { "--storageclass",          OptionString::STR_STORAGE_CLASS },
   { "--tag",                   OptionString::STR_TAG },
   { "--tapepool",              OptionString::STR_TAPE_POOL },
   { "--username",              OptionString::STR_USERNAME },
   { "--vid",                   OptionString::STR_VID }
};



/*!
 * Specify the help text for commands
 */
const std::map<AdminCmd::Cmd, CmdHelp> cmdHelp = {
   { AdminCmd::CMD_ADMIN,                { "admin",                "ad",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_ADMINHOST,            { "adminhost",            "ah",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_ARCHIVEFILE,          { "archivefile",          "af",  { "ls" } }},
   { AdminCmd::CMD_ARCHIVEROUTE,         { "archiveroute",         "ar",  { "add", "ch", "rm", "ls" } }},
   { AdminCmd::CMD_DRIVE,                { "drive",                "dr",  { "up", "down", "ls" } }},
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
                                           "(to be run on an empty self-dedicated\n"
                                           "drive; it is a synchronous command that returns performance stats and errors;\n"
                                           "all locations are local to the tapeserver)" }},
   { AdminCmd::CMD_VERIFY,               { "verify",               "ve",  { "add", "rm", "ls", "err" } }}
};

// help << m_requestTokens.at(0) << " dr/drive up/down/ls (it is a synchronous command):"    << std::endl
//<< "Set the requested state of the drive. The drive will complete any running mount" << std::endl
//<< "unless it is preempted with the --force option."                                 << std::endl
//<< "List the states for one or all drives"                                           << std::endl



/*
 * Enumerate options
 */
const Option opt_all                 { Option::OPT_FLAG,  "--all",                "-a", "" };
const Option opt_capacity            { Option::OPT_INT,   "--capacity",           "-c", " <capacity_in_bytes>" };
const Option opt_comment             { Option::OPT_STR,   "--comment",            "-m", " <\"comment\">" };
const Option opt_copynb              { Option::OPT_INT,   "--copynb",             "-c", " <copy_number>" };
const Option opt_disabled            { Option::OPT_BOOL,  "--disabled",           "-d", " <\"true\" or \"false\">" };
const Option opt_drivename           { Option::OPT_PARAM, "--drivename",          "",   "<drive_name>" };
const Option opt_encrypted           { Option::OPT_BOOL,  "--encrypted",          "-e", " <\"true\" or \"false\">" };
const Option opt_encryptionkey       { Option::OPT_STR,   "--encryptionkey",      "-k", " <encryption_key>" };
const Option opt_force               { Option::OPT_BOOL,  "--force",              "-f", " <\"true\" or \"false\">" };
const Option opt_force_flag          { Option::OPT_FLAG,  "--force",              "-f", "" };
const Option opt_header              { Option::OPT_FLAG,  "--header",             "-h", "" };
const Option opt_hostname_alias      { Option::OPT_STR,   "--name",               "-n", " <host_name>", "--hostname" };
const Option opt_instance            { Option::OPT_STR,   "--instance",           "-i", " <instance_name>" };
const Option opt_lbp                 { Option::OPT_BOOL,  "--lbp",                "-p", " <\"true\" or \"false\">" };
const Option opt_logicallibrary      { Option::OPT_STR,   "--logicallibrary",     "-l", " <logical_library_name>" };
const Option opt_logicallibrary_alias{ Option::OPT_STR,   "--name",               "-n", " <logical_library_name>", "--logicallibrary" };
const Option opt_partialfiles        { Option::OPT_INT,   "--partial",            "-p", " <number_of_files_per_tape>" };
const Option opt_partialtapes        { Option::OPT_INT,   "--partialtapesnumber", "-p", " <number_of_partial_tapes>" };
const Option opt_storageclass        { Option::OPT_STR,   "--storageclass",       "-s", " <storage_class_name>" };
const Option opt_tag                 { Option::OPT_STR,   "--tag",                "-t", " <tag_name>" };
const Option opt_tapepool            { Option::OPT_STR,   "--tapepool",           "-n", " <tapepool_name>" };
const Option opt_tapepool_alias      { Option::OPT_STR,   "--name",               "-n", " <tapepool_name>", "--tapepool" };
const Option opt_username            { Option::OPT_STR,   "--username",           "-u", " <user_name>" };
const Option opt_vid                 { Option::OPT_STR,   "--vid",                "-v", " <vid>" };
const Option opt_full                { Option::OPT_BOOL,  "--full",               "-f", " <\"true\" or \"false\">" };



/*!
 * Map valid options to commands
 */
const std::map<cmd_key_t, cmd_val_t> cmdOptions = {
   {{ AdminCmd::CMD_ADMIN,                AdminCmd::SUBCMD_ADD     }, { opt_username, opt_comment }},
   {{ AdminCmd::CMD_ADMIN,                AdminCmd::SUBCMD_CH      }, { opt_username, opt_comment }},
   {{ AdminCmd::CMD_ADMIN,                AdminCmd::SUBCMD_RM      }, { opt_username }},
   {{ AdminCmd::CMD_ADMIN,                AdminCmd::SUBCMD_LS      }, { opt_header.optional() }},
   {{ AdminCmd::CMD_ADMINHOST,            AdminCmd::SUBCMD_ADD     }, { opt_hostname_alias, opt_comment }},
   {{ AdminCmd::CMD_ADMINHOST,            AdminCmd::SUBCMD_CH      }, { opt_hostname_alias, opt_comment }},
   {{ AdminCmd::CMD_ADMINHOST,            AdminCmd::SUBCMD_RM      }, { opt_hostname_alias }},
   {{ AdminCmd::CMD_ADMINHOST,            AdminCmd::SUBCMD_LS      }, { opt_header.optional() }},
   {{ AdminCmd::CMD_ARCHIVEROUTE,         AdminCmd::SUBCMD_ADD     }, { opt_instance, opt_storageclass, opt_copynb,
                                                                        opt_tapepool, opt_comment
                                                                      }},
   {{ AdminCmd::CMD_ARCHIVEROUTE,         AdminCmd::SUBCMD_CH      }, { opt_instance, opt_storageclass, opt_copynb,
                                                                        opt_tapepool.optional(), opt_comment.optional()
                                                                      }},
   {{ AdminCmd::CMD_ARCHIVEROUTE,         AdminCmd::SUBCMD_RM      }, { opt_instance, opt_storageclass, opt_copynb }},
   {{ AdminCmd::CMD_ARCHIVEROUTE,         AdminCmd::SUBCMD_LS      }, { opt_header.optional() }},
   {{ AdminCmd::CMD_DRIVE,                AdminCmd::SUBCMD_UP      }, { opt_drivename }},
   {{ AdminCmd::CMD_DRIVE,                AdminCmd::SUBCMD_DOWN    }, { opt_drivename, opt_force_flag.optional() }},
   {{ AdminCmd::CMD_DRIVE,                AdminCmd::SUBCMD_LS      }, { opt_drivename.optional() }},
   {{ AdminCmd::CMD_LOGICALLIBRARY,       AdminCmd::SUBCMD_ADD     }, { opt_logicallibrary_alias, opt_comment }},
   {{ AdminCmd::CMD_LOGICALLIBRARY,       AdminCmd::SUBCMD_CH      }, { opt_logicallibrary_alias, opt_comment }},
   {{ AdminCmd::CMD_LOGICALLIBRARY,       AdminCmd::SUBCMD_RM      }, { opt_logicallibrary_alias }},
   {{ AdminCmd::CMD_LOGICALLIBRARY,       AdminCmd::SUBCMD_LS      }, { opt_header.optional() }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_ADD     }, { opt_vid, opt_logicallibrary, opt_tapepool,
                                                                        opt_capacity, opt_disabled, opt_full,
                                                                        opt_comment.optional()
                                                                      }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_CH      }, { opt_vid, opt_logicallibrary.optional(),
                                                                        opt_tapepool.optional(), opt_capacity.optional(),
                                                                        opt_encryptionkey.optional(),
                                                                        opt_disabled.optional(), opt_full.optional(),
                                                                        opt_comment.optional()
                                                                      }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_RM      }, { opt_vid }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_RECLAIM }, { opt_vid }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_LS      }, { opt_header.optional(), opt_vid.optional(),
                                                                        opt_logicallibrary.optional(),
                                                                        opt_tapepool.optional(), opt_capacity.optional(),
                                                                        opt_lbp.optional(), opt_disabled.optional(),
                                                                        opt_full.optional(), opt_all.optional()
                                                                      }},
   {{ AdminCmd::CMD_TAPE,                 AdminCmd::SUBCMD_LABEL   }, { opt_vid, opt_force.optional(),
                                                                        opt_lbp.optional(), opt_tag.optional()
                                                                      }},
   {{ AdminCmd::CMD_TAPEPOOL,             AdminCmd::SUBCMD_ADD     }, { opt_tapepool_alias, opt_partialtapes,
                                                                        opt_encrypted, opt_comment
                                                                      }},
   {{ AdminCmd::CMD_TAPEPOOL,             AdminCmd::SUBCMD_CH      }, { opt_tapepool_alias, opt_partialtapes.optional(),
                                                                        opt_encrypted.optional(), opt_comment.optional()
                                                                      }},
   {{ AdminCmd::CMD_TAPEPOOL,             AdminCmd::SUBCMD_RM      }, { opt_tapepool_alias }},
   {{ AdminCmd::CMD_TAPEPOOL,             AdminCmd::SUBCMD_LS      }, { opt_header.optional() }},
   {{ AdminCmd::CMD_LISTPENDINGARCHIVES,  AdminCmd::SUBCMD_NONE    }, { }},
   {{ AdminCmd::CMD_LISTPENDINGRETRIEVES, AdminCmd::SUBCMD_NONE    }, { }},
   {{ AdminCmd::CMD_SHOWQUEUES,           AdminCmd::SUBCMD_NONE    }, { }},
   {{ AdminCmd::CMD_SHRINK,               AdminCmd::SUBCMD_NONE    }, { }},
};

}} // namespace cta::admin
