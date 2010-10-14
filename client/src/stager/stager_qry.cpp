/*
 * $Id: stager_qry.c,v 1.34 2009/01/14 17:33:32 sponcec3 Exp $
 */

/*
 * Copyright (C) 2005 by CERN/IT/ADC/CA
 * All rights reserved
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Castor_limits.h"
#include "stager_api.h"
#include "stager_errmsg.h"
#include "serrno.h"
#include "Cgetopt.h"
#include "stager_client_commandline.h"
#include "client/src/stager/stager_client_api_query.hpp"
#include "castor/query/DiskPoolQueryType.hpp"

#define BUFSIZE 200

/**
 * struct to collect date from the command line for file queries
 */
struct cmd_args {
  int nbreqs;
  struct stage_query_req *requests;
  struct stage_options opts;
};

/**
 * Complete set of options for stage_qry
 */
static struct Coptions longopts[] =
  {
    {"hsm_filename",       REQUIRED_ARGUMENT,  NULL,      'M'},
    {"hsm_filelist",       REQUIRED_ARGUMENT,  NULL,      'f'},
    {"fileid",             REQUIRED_ARGUMENT,  NULL,      'F'},
    {"usertag",            REQUIRED_ARGUMENT,  NULL,      'U'},
    {"requestid",          REQUIRED_ARGUMENT,  NULL,      'r'},
    {"next",               NO_ARGUMENT,        NULL,      'n'},
    {"diskPool",           REQUIRED_ARGUMENT,  NULL,      'd'},
    {"statistic",          NO_ARGUMENT,        NULL,      's'},
    {"svcClass",           REQUIRED_ARGUMENT,  NULL,      'S'},
    {"help",               NO_ARGUMENT,        NULL,      'h'},
    {"si",                 NO_ARGUMENT,        NULL,      'i' },
    {"human-readable",     NO_ARGUMENT,        NULL,      'H' },
    {"available",          NO_ARGUMENT,        NULL,      'a' },
    {"total",              NO_ARGUMENT,        NULL,      't' },
    {NULL,                 0,                  NULL,       0 }
  };

/**
 * Set of options for file queries
 */
static struct Coptions longopts_fileQuery[] =
  {
    {"hsm_filename",       REQUIRED_ARGUMENT,  NULL,      'M'},
    {"hsm_filelist",       REQUIRED_ARGUMENT,  NULL,      'f'},
    {"fileid",             REQUIRED_ARGUMENT,  NULL,      'F'},
    {"usertag",            REQUIRED_ARGUMENT,  NULL,      'U'},
    {"requestid",          REQUIRED_ARGUMENT,  NULL,      'r'},
    {"next",               NO_ARGUMENT,        NULL,      'n'},
    {"svcClass",           REQUIRED_ARGUMENT,  NULL,      'S'},
    {NULL,                 0,                  NULL,       0 }
  };

/**
 * Set of options for diskpool queries
 */
static struct Coptions longopts_diskPoolQuery[] =
  {
    {"statistic",         NO_ARGUMENT,        NULL,      's'},
    {"diskPool",          REQUIRED_ARGUMENT,  NULL,      'd'},
    {"svcClass",          REQUIRED_ARGUMENT,  NULL,      'S'},
    {"si",                NO_ARGUMENT,        NULL,      'i'},
    {"human-readable",    NO_ARGUMENT,        NULL,      'H' },
    {"available",         NO_ARGUMENT,        NULL,      'a' },
    {"total",             NO_ARGUMENT,        NULL,      't' },
    {NULL,                0,                  NULL,       0 }
  };

/**
 * Small enum defining the type of a query
 */
enum queryType {
  FILEQUERY,
  DISKPOOLQUERY
};

/**
 * Displays usage
 * @param cmd the command to use when displaying usage
 */
void usage (char *cmd);

/**
 * Checks the type of query and counts the numberof arguments
 * @param argc the number of arguments on the command line
 * @param argv the command line
 * @param count filled with the number of arguments to the command,
 *              options excluded
 * @param type the type of query the user did
 * @return 0 if parsing was ok, else -1
 */
int checkAndCountArguments(int argc, char *argv[],
                           int* count, enum queryType* type);

/**
 * handles a FileQuery request
 * @param argc the number of arguments on the command line
 * @param argv the command line
 * @param nbArgs the number of arguments to the command
 */
void handleFileQuery(int argc, char *argv[], int nbArgs);

/**
 * handles a DiskPool request
 * @param argc the number of arguments on the command line
 * @param argv the command line
 */
void handleDiskPoolQuery(int argc, char *argv[]);

/**
 * parses the command line for a file query
 * @param argc the number of arguments on the command line
 * @param argv the command line
 * @param args a struct filled with the result of the parsing
 * @return 0 if parsing succeeded
 */
int parseCmdLineFileQuery(int argc, char *argv[], struct cmd_args *args);

/**
 * parses the command line for a diskPool query
 * @param argc the number of arguments on the command line
 * @param argv the command line
 * @param diksPool the diskPool given (if any), or NULL
 * @param svcClass the svcClass specified (if any), or NULL
 * @param siflag flag to indicate whether to display size
 * related information in powers of 1000 not 1024
 * @param queryType flag to indicate which type of diskPoolQuery
 * to perform (default, available space or total space)
 * @return 0 if parsing succeeded
 */
int parseCmdLineDiskPoolQuery(int argc, char *argv[],
                              char** diskpool, char** svcClass, int* siflag,
                              enum castor::query::DiskPoolQueryType* queryType);

// -----------------------------------------------------------------------
// main
// -----------------------------------------------------------------------
int main(int argc, char *argv[]) {
  int nbArgs;
  enum queryType type;
  int rc = checkAndCountArguments(argc, argv, &nbArgs, &type);
  if (rc < 0) {
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }
  switch (type) {
  case FILEQUERY:
    if (nbArgs <= 0) {
      usage(argv[0]);
      exit(EXIT_FAILURE);
    }
    handleFileQuery(argc, argv, nbArgs);
    break;
  case DISKPOOLQUERY:
    if (nbArgs != 0) {
      usage(argv[0]);
      exit(EXIT_FAILURE);
    }
    handleDiskPoolQuery(argc, argv);
    break;
  }
  return 0;
}

// -----------------------------------------------------------------------
// handleFileQuery
// -----------------------------------------------------------------------
void handleFileQuery(int argc, char *argv[], int nbArgs) {

  struct cmd_args args;
  struct  stage_filequery_resp *responses;
  int nbresps, rc, errflg, i;
  char errbuf[BUFSIZE];

  args.nbreqs = nbArgs;
  args.opts.stage_host = NULL;
  args.opts.service_class = NULL;
  args.opts.stage_port = 0;
  args.nbreqs = nbArgs;

  create_query_req(&(args.requests), args.nbreqs);

  errflg = parseCmdLineFileQuery(argc, argv, &args);
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }
  /* Setting the error buffer */
  stager_seterrbuf(errbuf, sizeof(errbuf));

  /* Getting env and default arguments */
  getDefaultForGlobal(&args.opts.stage_host,&args.opts.stage_port,&args.opts.service_class);

  /* Actual call to fileQuery */
  rc = stage_filequery(args.requests,
                       args.nbreqs,
                       &responses,
                       &nbresps,
                       &(args.opts));

  if (rc < 0) {
    if(serrno != 0) {
      fprintf(stderr, "Error: %s\n", sstrerror(serrno));
    }
    fprintf(stderr, "%s\n", errbuf);
    exit(EXIT_FAILURE);
  }

  printReceivedResponses(nbresps);

  for (i=0; i<nbresps; i++) {
    if (responses[i].errorCode == 0) {
      printf("%s %s %s",
             responses[i].castorfilename,
             responses[i].filename,
             stage_fileStatusName(responses[i].status));
    } else {
      /* a single failure in the list makes the command fail as a whole */
      rc = 1;
      printf("Error %d/%s (%s)",
             responses[i].errorCode,
             sstrerror(responses[i].errorCode),
             responses[i].errorMessage);
    }
    printf ("\n");
  }

  free_query_req(args.requests, args.nbreqs);
  free_filequery_resp(responses, nbresps);

  exit(rc);
}

// -----------------------------------------------------------------------
// handleDiskPoolQuery
// -----------------------------------------------------------------------
void handleDiskPoolQuery(int argc, char *argv[]) {

  int errflg;
  char errbuf[BUFSIZE];
  char *diskPool = NULL;
  int siflag = 0;
  enum castor::query::DiskPoolQueryType queryType = castor::query::DISKPOOLQUERYTYPE_DEFAULT;
  struct stage_options opts;

  // parsing the commane line
  opts.stage_host = NULL;
  opts.stage_port = 0;
  opts.service_class = NULL;
  errflg = parseCmdLineDiskPoolQuery
    (argc, argv, &diskPool, &(opts.service_class), &siflag, &queryType);
  if (errflg != 0) {
    usage (argv[0]);
    exit (EXIT_FAILURE);
  }

  /* Setting the error buffer */
  stager_seterrbuf(errbuf, sizeof(errbuf));

  /* Getting env and default arguments */
  getDefaultForGlobal(&opts.stage_host,&opts.stage_port,&opts.service_class);

  /* Setting the error buffer */
  stager_seterrbuf(errbuf, sizeof(errbuf));

  /* Actual call to stage_diskpoolquery */
  if (NULL == diskPool) {
    struct stage_diskpoolquery_resp *responses;
    int i, nbresps;
    int rc = stage_diskpoolsquery_internal(&responses,
                                           &nbresps,
                                           &opts,
                                           queryType);
    // check for errors
    if (rc < 0) {
      if(serrno != 0) {
        fprintf(stderr, "Error: %s\n", sstrerror(serrno));
      }
      fprintf(stderr, "%s\n", errbuf);
      exit(EXIT_FAILURE);
    }
    // display and cleanup memory
    for (i = 0; i < nbresps; i++) {
      stage_print_diskpoolquery_resp(stdout, &(responses[i]), siflag);
      stage_delete_diskpoolquery_resp(&(responses[i]));
    }
  } else {
    struct stage_diskpoolquery_resp response;
    int rc = stage_diskpoolquery_internal(diskPool, &response, &opts, queryType);
    // check for errors
    if (rc < 0) {
      if(serrno != 0) {
        fprintf(stderr, "Error: %s\n", sstrerror(serrno));
      }
      fprintf(stderr, "%s\n", errbuf);
      exit(EXIT_FAILURE);
    }
    // display and cleanup memory
    stage_print_diskpoolquery_resp(stdout, &response, siflag);
    stage_delete_diskpoolquery_resp(&response);
  }
  // end
  exit(EXIT_SUCCESS);
}

// -----------------------------------------------------------------------
// parseCmdLineFileQuery
// -----------------------------------------------------------------------
int parseCmdLineFileQuery(int argc, char *argv[],
                          struct cmd_args *args) {
  int nbargs, errflg, getNextMode, i;
  char c;

  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  nbargs = 0;
  getNextMode = 0;
  while ((c = Cgetopt_long (argc, argv,
                            "M:f:E:F:U:r:nS:",
                            longopts_fileQuery, NULL)) != -1) {
    switch (c) {
    case 'M':
      args->requests[nbargs].type = BY_FILENAME;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;
      break;
    case 'f':
      {
        FILE *infile;
        char line[CA_MAXPATHLEN+1];
        infile = fopen(Coptarg, "r");
        if(NULL == infile) {
          fprintf (stderr, "unable to read file %s\n", Coptarg);
          errflg++;
          break;
        }
        while (fgets(line, sizeof(line), infile) != NULL) {
          // drop trailing \n
          while (strlen(line) &&
                 ((line[strlen(line)-1] == '\n') || (line[strlen(line)-1] == '\r'))) {
            line[strlen(line) - 1] = 0;
          }
          // check whether we got a castor file name or a fileid
          if (line[0] == '/') {
            args->requests[nbargs].type = BY_FILENAME;
          } else {
            args->requests[nbargs].type = BY_FILEID;
          }
          args->requests[nbargs].param = (char *)strdup(line);
          nbargs++;
        }
      }
      break;
    case 'F':
      args->requests[nbargs].type = BY_FILEID;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;
      break;
    case 'U':
      args->requests[nbargs].type = BY_USERTAG;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;
      break;
    case 'r':
      args->requests[nbargs].type = BY_REQID;
      args->requests[nbargs].param = (char *)strdup(Coptarg);
      nbargs++;
      break;
    case 'S':
      args->opts.service_class = (char *)strdup(Coptarg);
      break;
    case 'n':
      getNextMode = 1;
      break;
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }

  if(getNextMode) {
    errflg++;
    for(i = 0; i < nbargs; i++) {
      if(args->requests[i].type == BY_REQID) {
        args->requests[i].type = BY_REQID_GETNEXT;
        errflg = 0;
      }
      else if(args->requests[i].type == BY_USERTAG) {
        args->requests[i].type = BY_USERTAG_GETNEXT;
        errflg = 0;
      }
    }
  }

  return errflg;
}

// -----------------------------------------------------------------------
// parseCmdLineDiskPoolQuery
// -----------------------------------------------------------------------
int parseCmdLineDiskPoolQuery(int argc, char *argv[],
                              char** diskPool, char** svcClass, int *siflag,
                              enum castor::query::DiskPoolQueryType* queryType) {
  int errflg;
  char c;

  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  *siflag = 0;
  *queryType = castor::query::DISKPOOLQUERYTYPE_DEFAULT;
  while ((c = Cgetopt_long (argc, argv,
                            "sd:S:iHat",
                            longopts_diskPoolQuery, NULL)) != -1) {
    switch (c) {
    case 'd':
      *diskPool = (char *)strdup(Coptarg);
      break;
    case 'S':
      *svcClass = (char *)strdup(Coptarg);
      break;
    case 'i':
      *siflag |= SIUNITS;
      break;
    case 'H':
      *siflag |= HUMANREADABLE;
      break;
    case 'a':
      *queryType = castor::query::DISKPOOLQUERYTYPE_AVAILABLE;
      break;
    case 't':
      *queryType = castor::query::DISKPOOLQUERYTYPE_TOTAL;
      break;
    case 's':
      break;
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }
  return errflg;
}

// -----------------------------------------------------------------------
// checkAndCountArguments
// -----------------------------------------------------------------------
int checkAndCountArguments(int argc, char *argv[],
                           int* count, enum queryType* type) {
  int errflg;
  char c;

  Coptind = 1;
  Copterr = 1;
  errflg = 0;
  *count = 0;
  *type = FILEQUERY;
  while ((c = Cgetopt_long
          (argc, argv, "M:f:F:U:r:nhsd:S:iHat", longopts, NULL)) != -1) {
    switch (c) {
    case 'M':
    case 'F':
    case 'U':
    case 'r':
      (*count)++;
      break;
    case 'f':
      {
        FILE *infile;
        char line[CA_MAXPATHLEN+1];
        infile = fopen(Coptarg, "r");
        if(NULL == infile) {
          fprintf (stderr, "unable to read file %s\n", Coptarg);
          errflg++;
          break;
        }
        while (fgets(line, sizeof(line), infile) != NULL) {
          (*count)++;
        }
      }
      break;
    case 's':
      *type = DISKPOOLQUERY;
      break;
    case 'S':
    case 'i':
    case 'n':
    case 'd':
    case 'H':
    case 'a':
    case 't':
      break;
    case 'h':
    default:
      errflg++;
      break;
    }
    if (errflg != 0) break;
  }
  if (errflg)
    return -1;
  else
    return 0;
}

// -----------------------------------------------------------------------
// usage
// -----------------------------------------------------------------------
void usage(char *cmd) {
  fprintf (stderr, "usage: %s ", cmd);
  fprintf (stderr, "%s",
           "[-M hsmfile [-M ...]] [-f hsmFileList] [-F fileid@nshost] [-S svcClass] [-U usertag] [-r requestid] [-n] [-h]\n");
  fprintf (stderr, "       %s ", cmd);
  fprintf (stderr, "%s", "-s [-S svcClass] [-d diskPool] [-H] [-i] [-h] [-a]\n");
}
