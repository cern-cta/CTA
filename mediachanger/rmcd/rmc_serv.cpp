/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rmc_serv.hpp"

#include "common/log/LogContext.hpp"
#include "mediachanger/librmc/Cdomainname.hpp"
#include "mediachanger/librmc/Cnetdb.hpp"
#include "mediachanger/librmc/getconfent.hpp"
#include "mediachanger/librmc/marshall.hpp"
#include "mediachanger/librmc/net.hpp"
#include "mediachanger/librmc/serrno.hpp"
#include "mediachanger/librmc/smc_struct.hpp"
#include "rmc_constants.hpp"
#include "rmc_procreq.hpp"
#include "rmc_sendrep.hpp"
#include "rmc_smcsubr.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

/* Forward declaration */
static int
rmc_getreq(cta::log::LogContext& lc, const int s, int* const req_type, char* const req_data, char** const clienthost);
static void
rmc_procreq(cta::log::LogContext& lc, const int rpfd, const int req_type, char* const req_data, char* const clienthost);
static int rmc_dispatchRqstHandler(cta::log::LogContext& lc,
                                   const int req_type,
                                   const struct rmc_srv_rqst_context* const rqst_context);
static void rmc_doit(cta::log::LogContext& lc, const int rpfd);

/* extern globals */
struct extended_robot_info g_extended_robot_info;

/* globals with file scope */
char g_localhost[CA_MAXHOSTNAMELEN + 1];

void handle_connection(cta::log::LogContext& lc, int s, struct pollfd* pfd) {
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);

  if (!(pfd->revents & POLLIN)) {
    return;  // No incoming connection
  }

  int rpfd = accept(s, reinterpret_cast<struct sockaddr*>(&from), &fromlen);
  if (rpfd < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;  // Non-blocking; no connections
    }
    const int acceptErrno = errno;
    cta::log::ScopedParamContainer params(lc);
    params.add(cta::semconv::log::errorMessage, std::string(strerror(acceptErrno)));
    lc.log(cta::log::ERR, "Failed to accept connection");
    return;
  }

  rmc_doit(lc, rpfd);  // Handle accepted connection
}

int rmc_main(const std::string& robot,
             int port,
             const std::string& listen_scope,
             cta::log::LogContext& lc,
             std::stop_token stopToken) {
  int c;
  char domainname[CA_MAXHOSTNAMELEN + 1];
  const char* msgaddr;
  int on = 1; /* for REUSEADDR */
  int s;
  struct sockaddr_in sin;
  struct smc_status smc_status;

  char localhost[CA_MAXHOSTNAMELEN + 1];
  gethostname(localhost, CA_MAXHOSTNAMELEN + 1);
  localhost[CA_MAXHOSTNAMELEN] = '\0';
  if (strchr(localhost, '.') != nullptr) {
    strncpy(g_localhost, localhost, CA_MAXHOSTNAMELEN + 1);
  } else {
    if (Cdomainname(domainname, sizeof(domainname)) < 0) {
      cta::log::ScopedParamContainer params(lc);
      params.add("localHost", std::string(localhost));
      lc.log(cta::log::WARNING, "Unable to get domain name; using unqualified local host name");
      strncpy(g_localhost, localhost, CA_MAXHOSTNAMELEN + 1);
    } else {
      // Truncate at first space to avoid multiple domains
      char* first_space = strchr(domainname, ' ');
      if (first_space) {
        *first_space = '\0';
      }
      cta::log::ScopedParamContainer params(lc);
      params.add("domainName", std::string(domainname));
      lc.log(cta::log::INFO, "Using domain name");
      if (int ret = snprintf(g_localhost, sizeof(g_localhost), "%s.%s", localhost, domainname);
          ret < 0 || ret >= static_cast<int>(sizeof(g_localhost))) {
        lc.log(cta::log::WARNING, "Fully qualified local host name exceeds maximum length");
      }
    }
    cta::log::ScopedParamContainer params(lc);
    params.add("localHost", std::string(g_localhost));
    lc.log(cta::log::INFO, "Determined local host name");
  }
  if (robot.empty()) {
    lc.log(cta::log::CRIT, "Media changer path cannot be empty");
    return 1;
  }

  g_extended_robot_info.smc_ldr[CA_MAXRBTNAMELEN] = '\0';

  if (robot.starts_with('/')) {
    snprintf(g_extended_robot_info.smc_ldr, sizeof(g_extended_robot_info.smc_ldr), "%s", robot.c_str());
  } else {
    snprintf(g_extended_robot_info.smc_ldr, sizeof(g_extended_robot_info.smc_ldr), "/dev/%s", robot.c_str());
  }
  if (g_extended_robot_info.smc_ldr[CA_MAXRBTNAMELEN] != '\0') {
    lc.log(cta::log::CRIT, "Invalid media changer path: " + robot);
    return 1;
  }
  g_extended_robot_info.smc_fd = -1;

  /* get robot geometry */
  {
    const int max_nb_attempts = 3;
    int attempt_nb = 1;
    for (attempt_nb = 1; attempt_nb <= max_nb_attempts; attempt_nb++) {
      cta::log::ScopedParamContainer params(lc);
      params.add("attemptNumber", attempt_nb);
      lc.log(cta::log::INFO, "Attempting to get tape library geometry");
      c = smc_get_geometry(g_extended_robot_info.smc_fd,
                           g_extended_robot_info.smc_ldr,
                           &g_extended_robot_info.robot_info);

      if (0 == c) {
        lc.log(cta::log::INFO, "Got tape library geometry");
        break;
      }

      c = smc_lasterror(lc, &smc_status, &msgaddr);
      params.add(cta::semconv::log::errorMessage, std::string(msgaddr));
      lc.log(cta::log::ERR, "Failed to get tape library geometry");

      // If this was the last attempt
      if (max_nb_attempts == attempt_nb) {
        return 1;
      } else {
        sleep(1);
      }
    }
  }

  signal(SIGPIPE, SIG_IGN);
  signal(SIGXFSZ, SIG_IGN);

  if (stopToken.stop_requested()) {
    return EXIT_SUCCESS;
  }

  /* open request socket */

  if ((s = socket(AF_INET, SOCK_STREAM | O_NONBLOCK, 0)) < 0) {
    cta::log::ScopedParamContainer params(lc);
    params.add(cta::semconv::log::errorMessage, std::string(neterror()));
    lc.log(cta::log::CRIT, "Failed to create request socket");
    return 1;
  }
  memset(&sin, 0, sizeof(struct sockaddr_in));
  sin.sin_family = AF_INET;
  sin.sin_port = htons(port);
  // rmcd should only accept connections from the loopback interface by default
  if (listen_scope == "any") {
    lc.log(cta::log::WARNING,
           "Listen scope set to 'any' (0.0.0.0); this exposes rmcd to unauthenticated remote connections.");
    sin.sin_addr.s_addr = htonl(INADDR_ANY);
  } else if (listen_scope == "loopback") {
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  } else {
    lc.log(cta::log::WARNING, "Received unsupported listen scope: \"" + listen_scope + "\". Defaulting to loopback.");
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  }
  if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, static_cast<const void*>(&on), sizeof(on)) < 0) {
    cta::log::ScopedParamContainer params(lc);
    params.add(cta::semconv::log::errorMessage, std::string(neterror()));
    lc.log(cta::log::WARNING, "Failed to enable address reuse on request socket");
  }
  if (bind(s, reinterpret_cast<const struct sockaddr*>(&sin), sizeof(sin)) < 0) {
    cta::log::ScopedParamContainer params(lc);
    params.add(cta::semconv::log::errorMessage, std::string(neterror()));
    lc.log(cta::log::CRIT, "Failed to bind request socket");
    close(s);
    return 1;
  }
  listen(s, 5);

  struct pollfd pfd;
  pfd.fd = s;
  pfd.events = POLLIN;

  /* main loop */
  while (!stopToken.stop_requested()) {
    // Check for connections
    if (int ret = poll(&pfd, 1, RMC_CHECKI * 1000); ret < 0) {
      const int pollErrno = errno;
      cta::log::ScopedParamContainer params(lc);
      params.add(cta::semconv::log::errorMessage, std::string(strerror(pollErrno)));
      lc.log(cta::log::ERR, "Failed to poll request socket");
      continue;
    } else if (ret == 0) {
      continue;  // timeout; no new connection
    }
    if (stopToken.stop_requested()) {
      break;
    }
    handle_connection(lc, s, &pfd);
  }

  close(s);
  return EXIT_SUCCESS;
}

static void rmc_doit(cta::log::LogContext& lc, const int rpfd) {
  int c;
  char* clienthost = nullptr;
  char req_data[REQ_DATA_SIZE];
  int req_type = 0;

  if ((c = rmc_getreq(lc, rpfd, &req_type, req_data, &clienthost)) == 0) {
    rmc_procreq(lc, rpfd, req_type, req_data, clienthost);
    if (clienthost != nullptr) {
      free(clienthost);
    }
  } else if (c > 0) {
    rmc_sendrep(lc, rpfd, RMC_RC, c);
  } else {
    close(rpfd);
  }
}

static int
rmc_getreq(cta::log::LogContext& lc, const int s, int* const req_type, char* const req_data, char** const clienthost) {
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  int l;
  int magic;
  int msglen;
  int n;
  char* rbp;
  char req_hdr[3 * LONGSIZE];

  l = netread_timeout(s, req_hdr, sizeof(req_hdr), RMC_TIMEOUT);
  if (l == sizeof(req_hdr)) {
    rbp = req_hdr;
    unmarshall_LONG(rbp, magic);
    unmarshall_LONG(rbp, n);
    *req_type = n;
    unmarshall_LONG(rbp, msglen);
    if (msglen > RMC_REQBUFSZ) {
      cta::log::ScopedParamContainer params(lc);
      params.add("requestSize", msglen);
      params.add("maxRequestSize", RMC_REQBUFSZ);
      lc.log(cta::log::ERR, "Request too large");
      return -1;
    }
    l = msglen - sizeof(req_hdr);
    n = netread_timeout(s, req_data, l, RMC_TIMEOUT);
    if (getpeername(s, reinterpret_cast<struct sockaddr*>(&from), &fromlen) < 0) {
      cta::log::ScopedParamContainer params(lc);
      params.add(cta::semconv::log::errorMessage, std::string(neterror()));
      lc.log(cta::log::ERR, "Failed to get client address");
      return ERMCUNREC;
    }
    {
      struct hostent hbuf;
      struct hostent* hp = nullptr;
      char buffer[1024];
      char client_ip[INET6_ADDRSTRLEN];
      int h_err;
      if (gethostbyaddr_r((void*) (&from.sin_addr),
                          sizeof(struct in_addr),
                          from.sin_family,
                          &hbuf,
                          buffer,
                          sizeof(buffer),
                          &hp,
                          &h_err)
            != 0
          || hp == nullptr) {
        if (inet_ntop(AF_INET, &from.sin_addr, client_ip, sizeof(client_ip)) == nullptr) {
          const int inetNtopErrno = errno;
          cta::log::ScopedParamContainer params(lc);
          params.add(cta::semconv::log::errorMessage, std::string(strerror(inetNtopErrno)));
          lc.log(cta::log::ERR, "Failed to convert client IP address to text");
          return ERMCUNREC;
        }
        // Duplicate the strings to prevent undefined behaviour after exiting function
        *clienthost = strdup(client_ip);
      } else {
        *clienthost = strdup(hp->h_name);
      }
    }
    return 0;
  } else {
    if (l > 0) {
      cta::log::ScopedParamContainer params(lc);
      params.add("bytesRead", l);
      params.add("expectedBytes", sizeof(req_hdr));
      lc.log(cta::log::ERR, "Failed to read complete request header");
    } else if (l < 0) {
      cta::log::ScopedParamContainer params(lc);
      params.add(cta::semconv::log::errorMessage, std::string(sstrerror(serrno)));
      lc.log(cta::log::ERR, "Failed to read request header");
    }
    return ERMCUNREC;
  }
}

static void rmc_procreq(cta::log::LogContext& lc,
                        const int rpfd,
                        const int req_type,
                        char* const req_data,
                        char* const clienthost) {
  struct rmc_srv_rqst_context rqst_context = {g_localhost, rpfd, req_data, clienthost};

  const int handlerRc = rmc_dispatchRqstHandler(lc, req_type, &rqst_context);

  if (ERMCUNREC == handlerRc) {
    rmc_sendrep(lc, rpfd, MSG_ERR, RMC03, req_type);
  }
  rmc_sendrep(lc, rpfd, RMC_RC, handlerRc);
}

/**
 * Dispatches the appropriate request handler.
 *
 * @param req_type The type of the request to be handled.
 * @param rqst_context The context of the request.
 * @return The result of handling the request.
 */
static int rmc_dispatchRqstHandler(cta::log::LogContext& lc,
                                   const int req_type,
                                   const struct rmc_srv_rqst_context* const rqst_context) {
  switch (req_type) {
    case RMC_MOUNT:
      return rmc_srv_mount(lc, rqst_context);
    case RMC_UNMOUNT:
      return rmc_srv_unmount(lc, rqst_context);
    case RMC_EXPORT:
      return rmc_srv_export(lc, rqst_context);
    case RMC_IMPORT:
      return rmc_srv_import(lc, rqst_context);
    case RMC_GETGEOM:
      return rmc_srv_getgeom(lc, rqst_context);
    case RMC_READELEM:
      return rmc_srv_readelem(lc, rqst_context);
    case RMC_FINDCART:
      return rmc_srv_findcart(lc, rqst_context);
    default:
      return ERMCUNREC;
  }
}
