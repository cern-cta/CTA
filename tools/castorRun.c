/*
 * $Id: castorRun.c,v 1.3 2005/03/31 13:37:11 jdurand Exp $
 */

/* cc -Wall -fPIC -D_LARGEFILE64_SOURCE -g -I../h -I../  -pthread -DCTHREAD_POSIX -D_THREAD_SAFE -D_REENTRANT -o castorRun castorRun.c -L../shlib -lshift */

/* Usage: castorRun <method> <protocol> <castorpath> <mode> <size> <producer> [producer arguments] */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/wait.h>
#include "serrno.h"
#include "osdep.h"
#include "rfio_api.h"
#include "u64subr.h"
#include "stager_client_api.h"
#include "Cgetopt.h"                    /* CASTOR getopt interface */
#include "Cinit.h"                      /* This is to have the main_args structure */

/* Local prototypes */
static int  castorRunGetOpt _PROTO((struct main_args *));
static void castorRunHelp _PROTO((char *));

/* Parameter values */
char *method = NULL;
char *filename = NULL;
char *producer = NULL;

/* Option values */
char *protocol = "";
char *usertag = "";
mode_t mode = 0;
u_signed64 size = 0;
int create = 0;
int help = 0;
struct stage_options opts = {NULL,NULL};

/* -------------------------------------------------------------------- */
/* main                                                                 */
/*                                                                      */
/* Purpose: Command-line interface to stage_{put|get|update}            */
/*                                                                      */
/* Input:  (int) argc [number of arguments]                             */
/*         (char **) argv [arguments]                                   */
/*                                                                      */
/* Output: EXIT_SUCCESS or EXIT_FAILURE                                 */
/* -------------------------------------------------------------------- */
int main(argc,argv)
     int argc;
     char **argv;
{
  struct main_args castorRunMainArgs;
  int fd = -1;
  int pid = -1;
  mode_t user_umask = 0;
  mode_t internal_mode = 0;
  struct stage_io_fileresp *response = NULL;
  char *requestId = NULL;
  char *rfio_path = NULL;
  char *castor_filename = NULL;
  int rc_api = 0;
  
  castorRunMainArgs.argc = argc;
  castorRunMainArgs.argv = argv;
  
  /* Get command-line options */
  /* ------------------------ */
  switch (castorRunGetOpt(&castorRunMainArgs)) {
  case 1:
    exit(EXIT_SUCCESS);
  case -1:
    exit(EXIT_FAILURE);
  default:
    break;
  }
  
  /* Recuper user's umask and make sure our umask does not perturb the rest */
  /* ---------------------------------------------------------------------- */
  user_umask = umask(0);

  /* Get the default value for mode if not setted */
  /* -------------------------------------------- */
  if (! mode) {
    mode = 0666;
  }

  /* Verify there is at least 3 other arguments */
  if (Coptind >= argc || Coptind < (argc - 3)) {
    castorRunHelp(argv[0]);
    exit(EXIT_FAILURE);
  }

  method = argv[Coptind];
  filename = argv[Coptind+1];
  producer = argv[Coptind+2];
  
  /* Unknown method ? */
  /* ---------------- */
  if ((strcmp(method,"put") != 0) &&
      (strcmp(method,"update") != 0) &&
      (strcmp(method,"get") != 0)) {
    castorRunHelp(argv[0]);
    exit(EXIT_FAILURE);
  }
  
  /* Hack for the global filesystem */
  if (strcmp(protocol,"file") == 0) {
    internal_mode = 0666;
  } else {
    internal_mode = mode;
  }

  if (strcmp(method,"put") == 0) {
    rc_api = stage_put("castorRun",protocol,filename,mode,size,&response,&requestId,&opts);
  } else if (strcmp(method,"update") == 0) {
    rc_api = stage_update("castorRun",protocol,filename,create,mode,size,&response,&requestId,&opts);
  } else {
    rc_api = stage_get("castorRun",protocol,filename,&response,&requestId,&opts);
  }
  if (rc_api < 0) {
    if (response != NULL) {
      fprintf(stderr,"stage_%s error: Error %d/%s\n", method, response->errorCode, response->errorMessage != NULL ? response->errorMessage : "<No errorMessage!>");
    } else {
      fprintf(stderr,"stage_%s error: No error returned (!?).\n", method);
    }
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  }

  if (response == NULL) {
    /* Ahem */
    fprintf(stderr,"stage_%s returned ok but no i/o response:\n", method);
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  }

  /* We build an explicit RFIO-aware file so that we will connect to the job */
  if ((rfio_path = (char *) malloc(strlen("rfio://") +
				   strlen(response->server) +
				   strlen(":") +
				   strlen("2147483647") +
				   strlen("/") +
				   strlen(response->filename) + 1)) == NULL) {
    perror("malloc");
    free(response);
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  }
  sprintf(rfio_path,"rfio://%s:%d/%s",response->server,response->port,response->filename);

  /* Open the internal file */
  rfio_errno = errno = 0;
  if (strcmp(method,"put") == 0) {
    fd = rfio_open(rfio_path,O_CREAT|O_TRUNC|O_WRONLY,internal_mode);
  } else if (strcmp(method,"update") == 0) {
    fd = rfio_open(rfio_path,O_CREAT|O_TRUNC|O_RDONLY,internal_mode);
  } else {
    fd = rfio_open(rfio_path,O_CREAT|O_TRUNC|O_RDWR,internal_mode);
  }

  free(rfio_path);

  if (fd < 0) {
    rfio_perror(filename);
    free(response);
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  }

  /* Restore umask */
  umask(user_umask);

  if (strcmp(protocol,"file") == 0) {
    /* We create the environment variable CASTOR_LOCALFILENAME */
    if ((castor_filename = (char *) malloc(strlen("CASTOR_LOCALFILENAME=") + strlen(response->filename) + 1)) == NULL) {
      perror("malloc");
      if (rfio_close(fd) < 0) {
	rfio_perror(filename);
      }
      free(response);
      if (requestId != NULL) {
	free(requestId);
      }
      exit(EXIT_FAILURE);
    }
    sprintf(castor_filename,"CASTOR_LOCALFILENAME=%s",response->filename);
    
    if (putenv(castor_filename) < 0) {
      perror("putenv");
      if (rfio_close(fd) < 0) {
	rfio_perror(filename);
      }
      free(response);
      if (requestId != NULL) {
      free(requestId);
      }
      exit(EXIT_FAILURE);
    }
  }

  /* We fork/exec the producer and its arguments */
  if ((pid = fork()) < 0) {
    perror("fork");
    if (rfio_close(fd) < 0) {
      rfio_perror(filename);
    }
    free(response);
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  } else if (pid == 0) {
    /* We are in the child : */
    execv(argv[Coptind+2],argv+Coptind+2);
    /* Failure to execute the child */
    exit(EXIT_FAILURE);
  }

  /* Wait for the child */
  while (1) {
    int child_pid;
    int term_status;

    if ((child_pid = waitpid(-1,&term_status,WNOHANG)) < 0) {
      break;
    }

    if (child_pid > 0) {
      if (WIFEXITED(term_status)) {
	if (WEXITSTATUS(term_status) != 0) {
	  fprintf(stderr, "WARNING :: Your producer %s (pid=%d) exited with status %d", producer, child_pid, WEXITSTATUS(term_status));
	}
      } else if (WIFSIGNALED(term_status)) {
	fprintf(stderr, "WARNING :: Your producer %s (pid=%d) exited due to uncaught signal %d", producer, child_pid, WTERMSIG(term_status));
      } else {
	fprintf(stderr, "WARNING :: Your producer %s (pid=%d) was stopped", producer, child_pid);
      }
    }
    sleep(1);
  }

  /* We close the file descriptor */
  if (rfio_close(fd) < 0) {
    rfio_perror(filename);
    free(response);
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  }

  free(response);
  if (requestId != NULL) {
    free(requestId);
  }
  exit(EXIT_SUCCESS);
}

/* -------------------------------------------------------------------- */
/* castorRunGetOpt                                                      */
/*                                                                      */
/* Purpose: Get command-line options                                    */
/*                                                                      */
/* Input:  (struct main_args *) main_args [command-line arguments]      */
/*                                                                      */
/* Output: (int) 0 [OK], -1 [ERROR], or 1 [OK BUT EXIT NOW, -h option]  */
/* -------------------------------------------------------------------- */
static int castorRunGetOpt(main_args)
     struct main_args *main_args;
{
  int argc;
  char **argv;
  int errflag;                          /* Command-line parsing error flag */
  int c;                                /* To switch in Cgetopt output */
  static struct Coptions longopts[] =
    {
      {"Class",              REQUIRED_ARGUMENT,  NULL,                 'C'},
      {"create",             NO_ARGUMENT,        NULL,                 'c'},
      {"Host",               REQUIRED_ARGUMENT,  NULL,                 'H'},
      {"help",               NO_ARGUMENT,        NULL,                 'h'},
      {"mode",               REQUIRED_ARGUMENT,  NULL,                 'm'},
      {"protocol",           REQUIRED_ARGUMENT,  NULL,                 'p'},
      {"size",               REQUIRED_ARGUMENT,  NULL,                 's'},
      {"usertag",            REQUIRED_ARGUMENT,  NULL,                 'u'},
      {NULL,                 0,                  NULL,                   0}
    };

  argc = main_args->argc;
  argv = main_args->argv;

  /* Get command-line options */
  c = 0;
  Coptarg = NULL;
  errflag = 0;
  /* Parse command-line options */
  Coptind = 1;
  Copterr = 1;

  while (errflag == 0 && (c = Cgetopt_long(argc,argv,"C:cH:hm:p:s:u:", longopts, NULL)) != -1) {
    char *endptr = NULL;

    switch (c) {
    case 'C':
      opts.service_class = Coptarg;
      break;
    case 'c':
      create = 1;
      break;
    case 'H':
      opts.stage_host = Coptarg;
      break;
    case 'h':
      help = 1;
      break;
    case 'm':
      mode = strtol(Coptarg,&endptr,0);
      if (*endptr != '\0') {
	errflag++;
      }
      break;
    case 'p':
      protocol = Coptarg;
      break;
    case 's':
      size = strutou64(Coptarg);
      break;
    case 'u':
      usertag = Coptarg;
      break;
    default:
      errflag++;
      break;
    }
  }
  
  /* Error processing command-line options ? */
  if (errflag != 0) {
    castorRunHelp(argv[0]);
    return(-1);
  }
  
  /* -h option in use ? */
  if (castorRunHelp != 0) {
    castorRunHelp(argv[0]);
    return(1);
  }

  return(0);
}

/* ---------------------------------------------------------------------- */
/* castorRunHelp                                                          */
/*                                                                        */
/* Purpose: Print online help                                             */
/*                                                                        */
/* Input:  (char *) programName [Name of the program on the command-line] */
/*                                                                        */
/* Output: n/a                                                            */
/* ---------------------------------------------------------------------- */
static void castorRunHelp(programName)
     char *programName;
{
  fprintf(stdout,
	  "Usage: %s [options] {method} {filename} {producer} {arguments}\n"
	  "\n"
	  "where options can be:\n"
	  "\n"
	  "\t--Class        or -C                \tService class\n"
	  "\t--create       or -c                \tCreate file (applicable if --protocol is \"update\")\n"
	  "\t--Host         or -H                \tStager host\n"
	  "\t--help         or -h                \tThis help"
	  "\t--mode         or -m {value}        \tMode when file is created (octal, decimal or hexadecimal)\n"
	  "\t--protocol     or -p                \tProtocol (\"rfio\", \"root\" or \"file\")\n"
	  "\t--size         or -s {value}        \tSize for disk allocation, can have an unit ('k','M','G','T','P')\n"
	  "\t--usertag      or -u {string}       \tUser tag\n"
	  "\n"
	  "Comments to: Castor.Support@cern.ch\n",
	  programName
	  );
}

