/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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

#include "FrontendCmd.hpp"
#include "ServiceAuthProcessor.hpp"
#include "AsyncServer.hpp"
#include "ServerNegotiationRequestHandler.hpp"
#include "ServerTapeLsRequestHandler.hpp"
#include "TokenStorage.hpp"
#include "utils.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "rdbms/Login.hpp"
#include "common/log/FileLogger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/log/SyslogLogger.hpp"
#include "common/log/Logger.hpp"
#include "common/log/LogContext.hpp"
#include "common/Configuration.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/CommandLineNotParsed.hpp"
#include "common/utils/utils.hpp"
#include "common/Constants.hpp"

#include <getopt.h>
#include <iostream>
#include <memory>
#include <type_traits>

cta::frontend::grpc::server::FrontendCmd::FrontendCmd(std::istream &inStream,
                                                      std::ostream &outStream,
                                                      std::ostream &errStream) noexcept:
                                                      m_in(inStream),
                                                      m_out(outStream),
                                                      m_err(errStream) {
}

int cta::frontend::grpc::server::FrontendCmd::main(const int argc, char** argv) {

  m_strExecName = argv[0];
  
  const int ARGNO = 1;
  const std::string HOST_NAME = cta::utils::getShortHostname();
  const std::string FRONTEND_NAME = "cta-frontend-async-grpc";
  const unsigned int DEFAULT_PORT = 17017;
  char c = 0;
  int iOptIdx = 0;
  bool bShortHeader = false;
  unsigned int uiPort = 0;
  unsigned int uiThreads = 2;
  std::string strService = "";
  std::string strKeytab = "";
  std::string strConFile = "/etc/cta/cta.conf";
  // SSL
  std::string strSslRoot;
  std::string strSslKey;
  std::string strSslCert;
  ::grpc::SslServerCredentialsOptions sslOpt;
  std::shared_ptr<::grpc::ServerCredentials> spServerCredentials = nullptr;

  if (argc <= ARGNO) {
    printUsage(m_out);
    return EXIT_FAILURE;
  }
  
  static struct option longOpt[] = {
    {"config",        required_argument, 0, 'c'},
    {"keytab",        required_argument, 0, 'k'},
    {"no-log-header", no_argument,       0, 'n'},
    {"port",	        required_argument, 0, 'p'},
    {"threads",       required_argument, 0, 't'},
    {"version",       no_argument,       0, 'v'},
    {"help",          no_argument,       0, 'h'},
    {0,               0,                 0, 0}
  };

  opterr = 0; // prevent the default error message printed by getopt_long
  /*
   * POSIX:
   * The getopt() function shall return the next option character specified on the command line.
   * A <colon> ( ':' ) shall be returned if getopt() detects a missing argument and the first character of optstring was a <colon> ( ':' ).
   * A <question-mark> ( '?' ) shall be returned if getopt() encounters an option character not in optstring or detects a missing argument and the first character of optstring was not a <colon> ( ':' ).
   * Otherwise, getopt() shall return -1 when all command line options are parsed.
   */
  while ((c = getopt_long(argc, argv, ":c:k:np:t:vh", longOpt, &iOptIdx)) != -1) {
    switch(c) {
      case 'c':
        strConFile = std::string(optarg);
        break;
      case 'k':
        strKeytab = std::string(optarg);
        break;
      case 'n':
        bShortHeader = true;
        break;
      case 'p':
        try {
          int iVal = std::stoi(optarg);
          if(iVal < 0) {
            throw std::invalid_argument("Invalid argument");
          }
          uiPort = static_cast<unsigned int>(iVal);
        } catch (const std::invalid_argument& ia) {
      	  m_err << m_strExecName << ": command line error: invalid argument for option -p" << std::endl;
          return EXIT_FAILURE;
        }
        break;
      case 't':
        try {
          int iVal = std::stoi(optarg);
          if(iVal < 0) {
            throw std::invalid_argument("Invalid argument");
          }
          uiThreads = static_cast<unsigned int>(iVal);
        } catch (const std::invalid_argument& ia) {
          m_err << m_strExecName << ": command line error: invalid argument for option -t" << std::endl;
          return EXIT_FAILURE;
        }
        break;
      case 'h':
        printUsage(m_out);
        return EXIT_SUCCESS;
      case 'v':
        m_out << FRONTEND_NAME << " version: " << CTA_VERSION << std::endl;
        return EXIT_SUCCESS;
        // break;
      case ':': // Missing parameter
        m_err << m_strExecName << ": command line error: option: '-" << (char)optopt << "' requires an argument"<< std::endl
                  << "Try '" << m_strExecName << " -h' for more information." << std::endl;
        return EXIT_FAILURE;
        // break;
      default /* '?' */:
        m_err << m_strExecName << ": command line error: unrecognised option '-" << (char)optopt << "'"<< std::endl
                  << "Try '" << m_strExecName << " -h' for more information." << std::endl;
        return EXIT_FAILURE;
    }
  }
  
  switch (argc - optind) {
    case ARGNO:
      // while (optind < argc) {
      //     strService = argv[optind++];
      // }
      /*
       * Right now only one service name is allowed
       * iteration is not needed
       */
      strService = argv[optind];
      break;
    case 0:
      m_err << m_strExecName << ": the service name is unspecified" << std::endl
            << "Try '" << m_strExecName << " -h' for more information." << std::endl;
      return EXIT_FAILURE;
    default:
      m_err << m_strExecName << ": only " << ARGNO << " service name is allowed" << std::endl
            << "Try '" << m_strExecName << " -h' for more information." << std::endl;
      return EXIT_FAILURE;
  }
  
  // Now only StdoutLogger
  std::unique_ptr<cta::log::Logger> upLog = std::make_unique<cta::log::StdoutLogger>(HOST_NAME, FRONTEND_NAME, bShortHeader);
  cta::log::LogContext lc(*upLog);
  //
  lc.log(log::INFO, "Starting " +  FRONTEND_NAME + " " + std::string(CTA_VERSION));
  // Reading configuration file
  cta::common::Configuration config(strConFile);
  try {
    if (!uiPort) {
      uiPort = config.getConfEntInt<unsigned int>("gRPC", "port", DEFAULT_PORT, upLog.get());
    }
    cta::frontend::grpc::utils::read(config.getConfEntString("gRPC", "SslRoot", upLog.get()), strSslRoot);
    cta::frontend::grpc::utils::read(config.getConfEntString("gRPC", "SslKey", upLog.get()), strSslKey);
    cta::frontend::grpc::utils::read(config.getConfEntString("gRPC", "SslCert", upLog.get()), strSslCert);
  } catch(const cta::exception::Exception &ex) {
    m_err << m_strExecName << ": problem while reading a configuration file - " << ex.getMessage().str() << std::endl;
    return EXIT_FAILURE;
  }
  
  if(strKeytab.empty()) {
    strKeytab = config.getConfEntString("gRPC", "Keytab", "");
    // and check again
    if(strKeytab.empty()) {
      m_err << m_strExecName << ": the keytab file is unspecified" << std::endl
            << "Try '" << m_strExecName << " -h' for more information." << std::endl;
      return EXIT_FAILURE;
    }
  }
  
  /*
   * Configuration reading is done.
   * Next -> init & startup.
   */
  // Init catalogue
  const std::string strCatalogueConfigFile = "/etc/cta/cta-catalogue.conf";
  const cta::rdbms::Login catalogueLogin = cta::rdbms::Login::parseFile(strCatalogueConfigFile);
  const uint64_t uiArchiveFileListingConns = 2;
  std::unique_ptr<cta::catalogue::CatalogueFactory> upCatalogueFactory
          = cta::catalogue::CatalogueFactoryFactory::create(*upLog,
                                                            catalogueLogin,
                                                            10,
                                                            uiArchiveFileListingConns);
  
  std::unique_ptr<cta::catalogue::Catalogue> upCatalogue = upCatalogueFactory->create();
  try {
    upCatalogue->Schema()->ping();
    log::ScopedParamContainer params(lc);
    params.add("SchemaVersion", upCatalogue->Schema()->getSchemaVersion().getSchemaVersion<std::string>());
    lc.log(cta::log::INFO, "In cta::frontend::grpc::server::FrontendCmd::main(): Connected to catalog.");
  } catch (cta::exception::Exception &ex) {
    log::ScopedParamContainer params(lc);
    params.add("message", ex.getMessageValue());
    lc.log(cta::log::CRIT, "In cta::frontend::grpc::server::FrontendCmd::main(): Got an exception.");
    return EXIT_FAILURE;
  }
  // Token storage
  TokenStorage tokenStorage;
  // Init server
  cta::frontend::grpc::server::AsyncServer server(*upLog, *upCatalogue, tokenStorage, uiPort, uiThreads);
  // Register services & run
  try {
    // SERVICE: Negotiation
    server.registerService<cta::frontend::rpc::Negotiation::AsyncService>();
    try {
      server.registerHandler<cta::frontend::grpc::server::NegotiationRequestHandler,  cta::frontend::rpc::Negotiation::AsyncService>(strKeytab, strService);
    } catch(const cta::exception::Exception &ex) {
      log::ScopedParamContainer params(lc);
      params.add("handler", "NegotiationRequestHandler");
      lc.log(cta::log::ERR, "In cta::frontend::grpc::server::FrontendCmd::main(): Error while registering handler.");
      throw;
    }
    // SERVICE: CtaRpcStream
    server.registerService<cta::frontend::rpc::CtaRpcStream::AsyncService>();
    server.registerHandler<cta::frontend::grpc::server::TapeLsRequestHandler,  cta::frontend::rpc::CtaRpcStream::AsyncService>();
    // Setup SSL
    sslOpt.pem_root_certs = strSslRoot;
    sslOpt.pem_key_cert_pairs.push_back({strSslKey, strSslCert});
    spServerCredentials = ::grpc::SslServerCredentials(sslOpt);
    // std::shared_ptr<::grpc::ServerCredentials> spServerCredentials = ::grpc::InsecureServerCredentials();
    // std::shared_ptr<::grpc::ServerCredentials> spServerCredentials = ::grpc::experimental::LocalServerCredentials(grpc_local_connect_type::LOCAL_TCP);
    
    std::shared_ptr<ServiceAuthProcessor> spAuthProcessor = std::make_shared<ServiceAuthProcessor>(tokenStorage);
    
    server.run(spServerCredentials, spAuthProcessor);
  } catch(const cta::exception::Exception& e) {
    log::ScopedParamContainer params(lc);
    params.add("message", e.getMessageValue());
    lc.log(cta::log::CRIT, "In cta::frontend::grpc::server::FrontendCmd::main(): Got an exception.");
    return EXIT_FAILURE;
  } catch(const std::exception& e) {
    log::ScopedParamContainer params(lc);
    params.add("message", e.what());
    lc.log(cta::log::CRIT, "In cta::frontend::grpc::server::FrontendCmd::main(): Got an exception.");
    return EXIT_FAILURE;
  }
  
  return EXIT_SUCCESS;
}

void cta::frontend::grpc::server::FrontendCmd::printUsage(std::ostream &osHelp) const {
   osHelp << "CTA Frontend gRPC" << std::endl << std::endl
           << "Usage: " << m_strExecName << " [<OPTION>...] <SERVICE_NAME>" << std::endl
           << "Options:" << std::endl
           << "  -c, --config<FILE>      config file location (default: /etc/cta/cta.conf)" << std::endl
           << "  -k, --keytab<FILE>      keytab file location" << std::endl
           // << "  -s<name>, --service<name>             ???" << std::endl
           << "  -n, --no-log-header     don't add hostname and timestamp to log outputs" << std::endl
           << "  -p, --port<NUMBER>      TCP port number to use (default: 17017)"  << std::endl
           << "  -t, --threads<NUMBER>   number of threads (default: 2)" << std::endl
           // << "  -s, --tls                 enable Transport Layer Security (TLS)" << std::endl
           << "  -v, --version           print version and exit" << std::endl
           << "  -h, --help              print this help and exit" << std::endl;
}

int main(const int argc, char **argv) {

  cta::frontend::grpc::server::FrontendCmd cmd(std::cin, std::cout, std::cerr);
  
  return cmd.main(argc, argv);
  
}

