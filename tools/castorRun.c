/*
 * $Id: castorRun.c,v 1.2 2005/03/24 14:24:27 jdurand Exp $
 */

/* cc -Wall -fPIC -D_LARGEFILE64_SOURCE -g -I../h -I../  -pthread -DCTHREAD_POSIX -D_THREAD_SAFE -D_REENTRANT -o castorRun castorRun.c -L../shlib -lshift */

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

/* Usage: castorRun <method> <protocol> <castorpath> <mode> <size> <producer> [producer arguments] */

static void usage _PROTO((char *));

int main(argc,argv)
     int argc;
     char **argv;
{
  char *method = NULL;
  char *protocol = NULL;
  char *castorPath = NULL;
  char *producer = NULL;
  int fd = -1;
  int pid = -1;
  /* Note: INT_MAX will not exceed 2147483647 */
  mode_t internal_mode = 0666; /* Hack : unfortunately needed */
  mode_t mode;
  mode_t old_umask = 0;
  u_signed64 size = 0;
  struct stage_io_fileresp *response = NULL;
  char *requestId = NULL;
  struct stage_options* opts = NULL;
  char *rfio_path = NULL;
  char *castor_filename = NULL;
  char *endptr = NULL;

  if (argc < 6) {
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  method = argv[1];
  protocol = argv[2];
  castorPath = argv[3];
  size = strtou64(argv[4]);
  mode = strtol(argv[5],&endptr,0);
  producer = argv[6];

  /* Check methods - only "put", "update" or "get" are allowed */
  if ((strcmp(method,"put") != 0) &&
      (strcmp(method,"update") != 0) &&
      (strcmp(method,"get") != 0)) {
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  /* Although this must in any case be protected on the server-side */
  /* we can avoid the server doing the check on protocol right now */
  if (strcmp(protocol,"file") != 0) {
    usage(argv[0]);
    exit(EXIT_FAILURE);
  }

  if (stage_put("castorRun",protocol,castorPath,mode,size,&response,&requestId,opts) < 0) {
    if (response != NULL) {
      fprintf(stderr,"stage_put error: Error %d/%s\n", response->errorCode, response->errorMessage != NULL ? response->errorMessage : "<No errorMessage!>");
    } else {
      fprintf(stderr,"stage_put error: %s\n", strerror(SEINTERNAL));
    }
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  }

  if (response == NULL) {
    /* Ahem */
    fprintf(stderr,"stage_put returned ok but no i/o response: %s\n", sstrerror(SEINTERNAL));
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

  /* Make sure our umask does not perturb us */
  old_umask = umask(0);

  /* Open the internal file */
  rfio_errno = errno = 0;
  if (strcmp(protocol,"put") == 0) {
    fd = rfio_open(rfio_path,O_CREAT|O_TRUNC|O_WRONLY,internal_mode);
  } else if (strcmp(protocol,"update") == 0) {
    fd = rfio_open(rfio_path,O_CREAT|O_TRUNC|O_RDONLY,internal_mode);
  } else {
    fd = rfio_open(rfio_path,O_CREAT|O_TRUNC|O_RDWR,internal_mode);
  }

  free(rfio_path);

  if (fd < 0) {
    rfio_perror(castorPath);
    free(response);
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  }

  /* Restore umask */
  umask(old_umask);

  /* We create the environment variable CASTOR_LOCALFILENAME */
  if ((castor_filename = (char *) malloc(strlen("CASTOR_LOCALFILENAME=") + strlen(response->filename) + 1)) == NULL) {
    perror("malloc");
    if (rfio_close(fd) < 0) {
      rfio_perror(castorPath);
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
      rfio_perror(castorPath);
    }
    free(response);
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  }

  /* We fork/exec the producer and its arguments */
  if ((pid = fork()) < 0) {
    perror("fork");
    if (rfio_close(fd) < 0) {
      rfio_perror(castorPath);
    }
    free(response);
    if (requestId != NULL) {
      free(requestId);
    }
    exit(EXIT_FAILURE);
  } else if (pid == 0) {
    /* We are in the child : */
    execv(argv[6],argv+6);
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
	  fprintf(stderr, "WARNING :: Your application pid=%d exited with status %d", child_pid, WEXITSTATUS(term_status));
	}
      } else if (WIFSIGNALED(term_status)) {
	fprintf(stderr, "WARNING :: Your application pid=%d exited due to uncaught signal %d", child_pid, WTERMSIG(term_status));
      } else {
	fprintf(stderr, "WARNING :: Your application pid=%d was stopped", child_pid);
      }
    }
    sleep(1);
  }

  /* We close the file descriptor */
  if (rfio_close(fd) < 0) {
    rfio_perror(castorPath);
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

static void usage(argv0)
     char *argv0;
{
  printf("Usage: %s <method> <protocol> <castorPath> <size> <mode> <producer> [producer arguments]\n"
	 "\n"
	 "where method is \"put\", \"update\" or \"get\"\n"
	 "      protocol is \"file\"\n"
	 "      size     is the wanted allocated size on disk, 0 if not known\n"
	 "      mode     is the wanted mode for the castorPath (for example: 0644)\n",
	 argv0);
}
