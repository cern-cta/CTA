/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include "version.h"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "rdbms/Login.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/SyslogLogger.hpp"
#include "FrontendService.hpp"
#include "Config.hpp"

#include <fstream>

namespace cta::frontend {

FrontendService::FrontendService(const std::string& configFilename) : m_archiveFileMaxSize(0) {
  int logToSyslog = 0;
  int logToStdout = 0;
  int logtoFile = 0;
  std::string logFilePath = "";

  // Read CTA namespaced configuration options from XRootD config file
  Config config(configFilename);

  // Instantiate the CTA logging system
  try {
    // Set the logger URL
    auto loggerURL = config.getOptionValueStr("cta.log.url");
    if(!loggerURL.has_value()) loggerURL = "syslog:";
    const auto shortHostname = utils::getShortHostname();

    // Set the logger level
    auto loggerLevelStr = config.getOptionValueStr("cta.log.level");
    auto loggerLevel = loggerLevelStr.has_value() ? log::toLogLevel(loggerLevelStr.value()) : log::INFO;

    // Set the log context
    if(loggerURL.value() == "syslog:") {
      m_log = std::make_unique<log::SyslogLogger>(shortHostname, "cta-frontend", loggerLevel);
      logToSyslog = 1;
    } else if(loggerURL.value() == "stdout:") {
      m_log = std::make_unique<log::StdoutLogger>(shortHostname, "cta-frontend");
      logToStdout = 1;
    } else if(loggerURL.value().substr(0, 5) == "file:") {
      logtoFile = 1;
      logFilePath = loggerURL.value().substr(5);
      m_log = std::make_unique<log::FileLogger>(shortHostname, "cta-frontend", logFilePath, loggerLevel);
    } else {
      throw exception::UserError(std::string("Unknown log URL: ") + loggerURL.value());
    }

    // Set the logger output format
    auto loggerFormat = config.getOptionValueStr("cta.log.format");
    if(loggerFormat.has_value()) m_log->setLogFormat(loggerFormat.value());
  } catch(exception::Exception& ex) {
    std::string ex_str("Failed to instantiate object representing CTA logging system: ");
    throw exception::Exception(ex_str + ex.getMessage().str());
  }
  log::Logger& log = *m_log;

  const std::list<log::Param> params = {log::Param("version", CTA_VERSION)};
  {
    // Log starting message
    std::list<log::Param> params;
    params.push_back(log::Param("version", CTA_VERSION));
    params.push_back(log::Param("configFilename", configFilename));
    params.push_back(log::Param("logToStdout", std::to_string(logToStdout)));
    params.push_back(log::Param("logToSyslog", std::to_string(logToSyslog)));
    params.push_back(log::Param("logtoFile", std::to_string(logtoFile)));
    params.push_back(log::Param("logFilePath", logFilePath));
    log(log::INFO, std::string("Starting cta-frontend"), params);
  }

  // Initialise the Catalogue
  std::string catalogueConfigFile = "/etc/cta/cta-catalogue.conf";
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile(catalogueConfigFile);
  auto catalogue_numberofconnections = config.getOptionValueInt("cta.catalogue.numberofconnections");
  if(!catalogue_numberofconnections.has_value()) {
    throw exception::UserError("cta.catalogue.numberofconnections is not set in configuration file " + configFilename);
  }
  const uint64_t nbArchiveFileListingConns = 2;

  {
    // Log catalogue.numberofconnections
    std::list<log::Param> params;
    params.push_back(log::Param("source", configFilename));
    params.push_back(log::Param("category", "cta.catalogue"));
    params.push_back(log::Param("key", "numberofconnections"));
    params.push_back(log::Param("value", std::to_string(catalogue_numberofconnections.value())));
    log(log::INFO, "Configuration entry", params);
  }
  {
    // Log catalogue number of archive file listing connections
    std::list<log::Param> params;
    params.push_back(log::Param("source", "Compile time default"));
    params.push_back(log::Param("category", "cta.catalogue"));
    params.push_back(log::Param("key", "nbArchiveFileListingConns"));
    params.push_back(log::Param("value", std::to_string(nbArchiveFileListingConns)));
    log(log::INFO, "Configuration entry", params);
  }

  {
    auto catalogueFactory = catalogue::CatalogueFactoryFactory::create(*m_log, catalogueLogin,
      catalogue_numberofconnections.value(), nbArchiveFileListingConns);
    m_catalogue = catalogueFactory->create();
    try {
      m_catalogue->Schema()->ping();
    } catch(cta::exception::Exception& ex) {
      auto lc = getLogContext();
      lc.log(cta::log::CRIT, ex.getMessageValue());
      throw ex;
    }
  }

  m_catalogue_conn_string = catalogueLogin.connectionString;

  // Initialise the Scheduler DB
  const std::string DB_CONN_PARAM = "cta.objectstore.backendpath";
  auto db_conn = config.getOptionValueStr(DB_CONN_PARAM);
  if(!db_conn.has_value()) {
    throw exception::UserError(DB_CONN_PARAM + " is not set in configuration file " + configFilename);
  }

  {
    // Log cta.objectstore.backendpath
    std::list<log::Param> params;
    params.push_back(log::Param("source", configFilename));
    params.push_back(log::Param("category", "cta.objectstore"));
    params.push_back(log::Param("key", "backendpath"));
    params.push_back(log::Param("value", db_conn.value()));
    log(log::INFO, "Configuration entry", params);
  }

  m_scheddbInit = std::make_unique<SchedulerDBInit_t>("Frontend", db_conn.value(), *m_log);
  m_scheddb     = m_scheddbInit->getSchedDB(*m_catalogue, *m_log);

  const auto schedulerThreadStackOpt = config.getOptionValueInt("cta.schedulerdb.threadstacksize_mb");
  std::optional<size_t> schedulerThreadStackSize = schedulerThreadStackOpt.has_value() ?
                                                  std::optional<size_t>(schedulerThreadStackOpt.value() * 1024 * 1024) : std::nullopt;

  auto threadPoolSize = config.getOptionValueInt("cta.schedulerdb.numberofthreads");
  m_scheddb->initConfig(threadPoolSize, schedulerThreadStackSize);

  // Log cta.schedulerdb.numberofthreads
  if(threadPoolSize.has_value()) {
    std::list<log::Param> params;
    params.push_back(log::Param("source", configFilename));
    params.push_back(log::Param("category", "cta.schedulerdb"));
    params.push_back(log::Param("key", "numberofthreads"));
    params.push_back(log::Param("value", std::to_string(threadPoolSize.value())));
    log(log::INFO, "Configuration entry", params);
  }

  // Initialise the Scheduler
  m_scheduler = std::make_unique<cta::Scheduler>(*m_catalogue, *m_scheddb, 5, 2*1000*1000);

  // Initialise the Frontend
  auto archiveFileMaxSize = config.getOptionValueUInt("cta.archivefile.max_size_gb");
  // Convert archiveFileMaxSize from GB to bytes
  m_archiveFileMaxSize = archiveFileMaxSize.has_value() ?  static_cast<uint64_t>(archiveFileMaxSize.value()) * 1024 * 1024 * 1024 : 0;

  {
    // Log cta.archivefile.max_size_gb
    std::list<log::Param> params;
    params.push_back(log::Param("source", archiveFileMaxSize.has_value() ? configFilename : "Compile time default"));
    params.push_back(log::Param("category", "cta.archivefile"));
    params.push_back(log::Param("key", "max_size_gb"));
    params.push_back(log::Param("value", std::to_string(archiveFileMaxSize.has_value() ? archiveFileMaxSize.value() : 0)));
    log(log::INFO, "Configuration entry", params);
  }

  // Get the repack buffer URL
  auto repackBufferURLConf = config.getOptionValueStr("cta.repack.repack_buffer_url");
  if(repackBufferURLConf.has_value()) {
    m_repackBufferURL = repackBufferURLConf.value();
  }

  auto repackMaxFilesToSelectConf = config.getOptionValueUInt("cta.repack.repack_max_files_to_select");
  if(repackMaxFilesToSelectConf.has_value()) {
    m_repackMaxFilesToSelect = repackMaxFilesToSelectConf.value();
  }

  // Get the verification mount policy
  const auto verificationMountPolicy = config.getOptionValueStr("cta.verification.mount_policy");
  if(verificationMountPolicy.has_value()) {
    m_verificationMountPolicy = verificationMountPolicy.value();
  }

  {
    // Log cta.repack.repack_buffer_url
    if(repackBufferURLConf.has_value()) {
      std::list<log::Param> params;
      params.push_back(log::Param("source", configFilename));
      params.push_back(log::Param("category", "cta.repack"));
      params.push_back(log::Param("key", "repack_buffer_url"));
      params.push_back(log::Param("value", repackBufferURLConf.value()));
      log(log::INFO, "Configuration entry", params);
    }
  }

  // Get the endpoint for namespace queries
  auto nsConf = config.getOptionValueStr("cta.ns.config");
  if(nsConf.has_value()) {
    setNamespaceMap(nsConf.value());
  } else {
    log(log::WARNING, "'cta.ns.config' not specified; namespace queries are disabled");
  }

  {
    // Log cta.ns.config
    if(nsConf.has_value()) {
      std::list<log::Param> params;
      params.push_back(log::Param("source", configFilename));
      params.push_back(log::Param("category", "cta.ns"));
      params.push_back(log::Param("key", "config"));
      params.push_back(log::Param("value", nsConf.value()));
      log(log::INFO, "Configuration entry", params);
    }
  }

  // Get the mount policy name for verification requests

  // All done
  log(log::INFO, std::string("cta-frontend started"), params);
}

void FrontendService::setNamespaceMap(const std::string& keytab_file) {
  // Open the keytab file for reading
  std::ifstream file(keytab_file);
  if(!file) {
    throw cta::exception::UserError("Failed to open namespace keytab configuration file " + keytab_file);
  }

  // Parse the keytab line by line
  std::string line;
  for(int lineno = 0; std::getline(file, line); ++lineno) {
    // Strip out comments
    auto pos = line.find('#');
    if(pos != std::string::npos) {
      line.resize(pos);
    }

    // Parse one line
    std::istringstream ss(line);
    std::string diskInstance;
    std::string endpoint;
    std::string token;
    std::string eol;
    ss >> diskInstance >> endpoint >> token >> eol;

    // Ignore blank lines, all other lines must have exactly 3 elements
    if(token.empty() || !eol.empty()) {
      if(diskInstance.empty() && endpoint.empty() && token.empty()) continue;
      throw cta::exception::UserError("Could not parse namespace keytab configuration file line " + std::to_string(lineno) + ": " + line);
    }
    m_namespaceMap.insert(std::make_pair(diskInstance, cta::Namespace(endpoint, token)));
  }
}

} // namespace cta::frontend
