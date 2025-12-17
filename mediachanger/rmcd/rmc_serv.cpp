/*
 * SPDX-FileCopyrightText: 2001 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "Cinit.hpp"
#include "mediachanger/librmc/Cdomainname.hpp"
#include "mediachanger/librmc/Cnetdb.hpp"
#include "mediachanger/librmc/getconfent.hpp"
#include "mediachanger/librmc/marshall.hpp"
#include "mediachanger/librmc/net.hpp"
#include "mediachanger/librmc/serrno.hpp"
#include "mediachanger/librmc/smc_struct.hpp"
#include "rmc_constants.hpp"
#include "rmc_logit.hpp"
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
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

/* Forward declaration */
static int rmc_getreq(const int s, int* const req_type, char* const req_data, char** const clienthost);
static void rmc_procreq(const int rpfd, const int req_type, char* const req_data, char* const clienthost);
static int rmc_dispatchRqstHandler(const int req_type, const struct rmc_srv_rqst_context* const rqst_context);
static void rmc_doit(const int rpfd);

/* extern globals */
int g_jid;
struct extended_robot_info g_extended_robot_info;

/* globals with file scope */
char g_localhost[CA_MAXHOSTNAMELEN + 1];

void handle_connection(int s, struct pollfd* pfd) {
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);

  if (!(pfd->revents & POLLIN)) {
    return;  // No incoming connection
  }

  int rpfd = accept(s, (struct sockaddr*) &from, &fromlen);
  if (rpfd < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;  // Non-blocking; no connections
    }
    perror("accept() error");
    return;
  }

  rmc_doit(rpfd);  // Handle accepted connection
}

int rmc_main(const char* const robot) {
  int c;
  char domainname[CA_MAXHOSTNAMELEN + 1];
  const char* msgaddr;
  int on = 1; /* for REUSEADDR */
  int s;
  struct sockaddr_in sin;
  struct smc_status smc_status;
  const char* const func = "rmc_serv";

  g_jid = getpid();
  rmc_logit(func, "started\n");

  char localhost[CA_MAXHOSTNAMELEN + 1];
  gethostname(localhost, CA_MAXHOSTNAMELEN + 1);
  localhost[CA_MAXHOSTNAMELEN] = '\0';
  if (strchr(localhost, '.') != nullptr) {
    strncpy(g_localhost, localhost, CA_MAXHOSTNAMELEN + 1);
  } else {
    if (Cdomainname(domainname, sizeof(domainname)) < 0) {
      rmc_logit(func, "Unable to get domainname\n");
    } else {
      rmc_logit(func, "Using first word from the list as domain name: %s", domainname);
      // Truncate at first space to avoid multiple domains
      char* first_space = strchr(domainname, ' ');
      if (first_space) {
        *first_space = '\0';
      }
    }
    if (int ret = snprintf(g_localhost, CA_MAXHOSTNAMELEN, "%s.%s", localhost, domainname);
        ret < 0 || ret >= CA_MAXHOSTNAMELEN) {
      rmc_logit(func, "localhost.domainname exceeds maximum length\n");
    }
    rmc_logit(func, "found the following localhost.domainname: %s", g_localhost);
  }
  if (*robot == '\0') {
    rmc_logit(func, RMC06, "robot");
    exit(USERR);
  }

  g_extended_robot_info.smc_ldr[CA_MAXRBTNAMELEN] = '\0';
  if (*robot == '/') {
    strncpy(g_extended_robot_info.smc_ldr, robot, CA_MAXRBTNAMELEN + 1);
  } else {
    snprintf(g_extended_robot_info.smc_ldr, CA_MAXRBTNAMELEN + 1, "/dev/%s", robot);
  }
  if (g_extended_robot_info.smc_ldr[CA_MAXRBTNAMELEN] != '\0') {
    rmc_logit(func, RMC06, "robot");
    exit(USERR);
  }
  g_extended_robot_info.smc_fd = -1;

  /* get robot geometry */
  {
    const int max_nb_attempts = 3;
    int attempt_nb = 1;
    for (attempt_nb = 1; attempt_nb <= max_nb_attempts; attempt_nb++) {
      rmc_logit(func, "Trying to get geometry of tape library: attempt_nb=%d\n", attempt_nb);
      c = smc_get_geometry(g_extended_robot_info.smc_fd,
                           g_extended_robot_info.smc_ldr,
                           &g_extended_robot_info.robot_info);

      if (0 == c) {
        rmc_logit(func, "Got geometry of tape library\n");
        break;
      }

      c = smc_lasterror(&smc_status, &msgaddr);
      rmc_logit(func, RMC02, "get_geometry", msgaddr);

      // If this was the last attempt
      if (max_nb_attempts == attempt_nb) {
        exit(c);
      } else {
        sleep(1);
      }
    }
  }

  signal(SIGPIPE, SIG_IGN);
  signal(SIGXFSZ, SIG_IGN);

  /* open request socket */

  if ((s = socket(AF_INET, SOCK_STREAM | O_NONBLOCK, 0)) < 0) {
    rmc_logit(func, RMC02, "socket", neterror());
    exit(CONFERR);
  }
  memset((char*) &sin, 0, sizeof(struct sockaddr_in));
  sin.sin_family = AF_INET;
  {
    const char* p;
    p = getenv("RMC_PORT");
    if (!p) {
      p = getconfent_fromfile("RMC", "PORT", 0);
    }

    if (p) {
      sin.sin_port = htons((unsigned short) atoi(p));
    } else {
      sin.sin_port = htons((unsigned short) RMC_PORT);
    }
  }
  // rmcd should only accept connections from the loopback interface
  sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char*) &on, sizeof(on)) < 0) {
    rmc_logit(func, RMC02, "setsockopt", neterror());
  }
  if (bind(s, (struct sockaddr*) &sin, sizeof(sin)) < 0) {
    rmc_logit(func, RMC02, "bind", neterror());
    exit(CONFERR);
  }
  listen(s, 5);

  struct pollfd pfd;
  pfd.fd = s;
  pfd.events = POLLIN;

  /* main loop */
  while (1) {
    // Check for connections
    if (int ret = poll(&pfd, 1, RMC_CHECKI * 1000); ret < 0) {
      perror("poll() error");
      continue;
    } else if (ret == 0) {
      continue;  // timeout; no new connection
    }
    handle_connection(s, &pfd);
  }
}

/**
 * Returns 1 if the rmcd daemon should run in the background or 0 if the
 * daemon should run in the foreground.
 */
static int run_rmcd_in_background(const int argc, char** argv) {
  int i = 0;
  for (i = 1; i < argc; i++) {
    if (0 == strcmp(argv[i], "-f")) {
      return 0;
    }
  }

  return 1;
}

/**
 * Returns the number of command-line arguments that start with '-'.
 */
static int get_nb_cmdline_options(const int argc, char** argv) {
  int nbOptions = 0;
  for (int i = 1; i < argc; i++) {
    if (*argv[i] == '-') {
      nbOptions++;
    }
  }
  return nbOptions;
}

int main(const int argc, char** argv) {
  const char* robot = "";
  const int nb_cmdline_options = get_nb_cmdline_options(argc, argv);

  switch (argc) {
    case 1:
      fprintf(stderr, "RMC01 - wrong arguments given ,specify the device file of the tape library\n");
      exit(USERR);
    case 2:
      if (0 == nb_cmdline_options) {
        robot = argv[1];
      } else {
        fprintf(stderr, "RMC01 - robot parameter is mandatory\n");
        exit(USERR);
      }
      break;
    case 3:
      if (0 == nb_cmdline_options) {
        fprintf(stderr, "Too many robot parameters\n");
        exit(USERR);
      } else if (2 == nb_cmdline_options) {
        fprintf(stderr, "RMC01 - robot parameter is mandatory\n");
        exit(USERR);
        /* At this point there is one argument starting with '-' */
      } else if (0 == strcmp(argv[1], "-f")) {
        robot = argv[2];
      } else if (0 == strcmp(argv[2], "-f")) {
        robot = argv[1];
      } else {
        fprintf(stderr, "Unknown option\n");
        exit(USERR);
      }
      break;
    default:
      fprintf(stderr, "Too many command-line arguments\n");
      exit(USERR);
  }

  if (run_rmcd_in_background(argc, argv) && (Cinitdaemon("rmcd", nullptr) < 0)) {
    exit(SYERR);
  }
  exit(rmc_main(robot));
}

static void rmc_doit(const int rpfd) {
  int c;
  char* clienthost = nullptr;
  char req_data[REQ_DATA_SIZE];
  int req_type = 0;

  if ((c = rmc_getreq(rpfd, &req_type, req_data, &clienthost)) == 0) {
    rmc_procreq(rpfd, req_type, req_data, clienthost);
    if (clienthost != nullptr) {
      free(clienthost);
    }
  } else if (c > 0) {
    rmc_sendrep(rpfd, RMC_RC, c);
  } else {
    close(rpfd);
  }
}

static int rmc_getreq(const int s, int* const req_type, char* const req_data, char** const clienthost) {
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  int l;
  int magic;
  int msglen;
  int n;
  char* rbp;
  char req_hdr[3 * LONGSIZE];
  const char* const func = "rmc_getreq";

  l = netread_timeout(s, req_hdr, sizeof(req_hdr), RMC_TIMEOUT);
  if (l == sizeof(req_hdr)) {
    rbp = req_hdr;
    unmarshall_LONG(rbp, magic);
    unmarshall_LONG(rbp, n);
    *req_type = n;
    unmarshall_LONG(rbp, msglen);
    if (msglen > RMC_REQBUFSZ) {
      rmc_logit(func, RMC46, RMC_REQBUFSZ);
      return -1;
    }
    l = msglen - sizeof(req_hdr);
    n = netread_timeout(s, req_data, l, RMC_TIMEOUT);
    if (getpeername(s, (struct sockaddr*) &from, &fromlen) < 0) {
      rmc_logit(func, RMC02, "getpeername", neterror());
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
          perror("inet_ntop");
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
      rmc_logit(func, RMC04, l);
    } else if (l < 0) {
      rmc_logit(func, RMC02, "netread", sstrerror(serrno));
    }
    return ERMCUNREC;
  }
}

static void rmc_procreq(const int rpfd, const int req_type, char* const req_data, char* const clienthost) {
  struct rmc_srv_rqst_context rqst_context;

  rqst_context.localhost = g_localhost;
  rqst_context.rpfd = rpfd;
  rqst_context.req_data = req_data;
  rqst_context.clienthost = clienthost;

  const int handlerRc = rmc_dispatchRqstHandler(req_type, &rqst_context);

  if (ERMCUNREC == handlerRc) {
    rmc_sendrep(rpfd, MSG_ERR, RMC03, req_type);
  }
  rmc_sendrep(rpfd, RMC_RC, handlerRc);
}

/**
 * Dispatches the appropriate request handler.
 *
 * @param req_type The type of the request to be handled.
 * @param rqst_context The context of the request.
 * @return The result of handling the request.
 */
static int rmc_dispatchRqstHandler(const int req_type, const struct rmc_srv_rqst_context* const rqst_context) {
  switch (req_type) {
    case RMC_MOUNT:
      return rmc_srv_mount(rqst_context);
    case RMC_UNMOUNT:
      return rmc_srv_unmount(rqst_context);
    case RMC_EXPORT:
      return rmc_srv_export(rqst_context);
    case RMC_IMPORT:
      return rmc_srv_import(rqst_context);
    case RMC_GETGEOM:
      return rmc_srv_getgeom(rqst_context);
    case RMC_READELEM:
      return rmc_srv_readelem(rqst_context);
    case RMC_FINDCART:
      return rmc_srv_findcart(rqst_context);
    default:
      return ERMCUNREC;
  }
}
