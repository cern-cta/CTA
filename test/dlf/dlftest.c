/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlftest.c,v $ $Revision: 1.5 $ $Date: 2005/09/21 11:55:47 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <Cthread_api.h>
#include "net.h"
#include "marshall.h"
#include "serrno.h"
#include "dlf.h"
#include <dlf_api.h>
#include "Cuuid.h"
#include "Cgetopt.h"

#define DEFAULT_NBTHREADS 6
int  NBTHREADS=DEFAULT_NBTHREADS;

extern char *Coptarg;
extern int Coptind;

void *gen_log(void*);

int main(argc, argv)
     int argc;
     char **argv;
{
  uid_t uid;
  gid_t gid;
  int i;
  char prtbuf[1024];

  int c;
  int errflg;

  char *endptr;
  int msgs_set = 0;
  int num_msgs = 0;
  int *cid; /* Thread identifiers */
  int *status;

  uid = geteuid();
  gid = getegid();

#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
    fprintf (stderr, DLF53);
    exit (USERR);
  }
#endif
  errflg = 0;
  Coptind = 1; /* REQUIRED */
  while ((c = Cgetopt (argc, argv, "n:?t:")) != -1) {
    switch (c) {
    case 'n':
      num_msgs = strtol(Coptarg, &endptr, 10);
      if (*endptr != '\0' || num_msgs < 0 || num_msgs > 10000) {
        fprintf (stderr, "%s\n", strerror(EINVAL));
        errflg++;
      }
      else {
        msgs_set++;
      }
      break;
    case 't':
       NBTHREADS=strtol(Coptarg, &endptr, 10);
       if (*endptr != '\0' || NBTHREADS < 0 || NBTHREADS > 100)
          {
          fprintf (stderr, "%s\n", strerror(EINVAL));
          errflg++;
          }
       break;   
    case '?':
      errflg++;
      break;
    default:
      errflg++;
      break;
    }
  }
  if (Coptind < argc) {
    errflg++;
  }
  if (!(msgs_set)) {
    fprintf (stderr, "Not all required parameters are given\n");
    errflg++;
  }
  if (errflg) {
    fprintf (stderr, "usage: %s %s", argv[0],
             "-n number_of_messages_per_thread [-t number_of_thread]\n");
    exit (USERR);
  }
  
if((cid=calloc(NBTHREADS,sizeof(int)))==NULL)
   {
   fprintf (stderr, "Calloc error\n");
   exit (USERR);
   }
#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, DLF52);
    exit (SYERR);
  }
#endif
  if (dlf_init("DLF-CONTROL") < 0 ) {
    fprintf (stderr, "Error in initializing global structure.\n");
    exit (SYERR);
  }

  /* 1) Insert dlftest into the database - facility number 255 */

  if (dlf_enterfacility("dlftest", 255) < 0) {
    if (serrno != EEXIST) {
      fprintf(stderr, "Error entering facility\n");
      exit (SYERR);
    }
  }

  /* 2) Insert message texts into the database
     "Dlftest message number 1 to 255 */
  for (i = 1; i < 256; i++) {
    snprintf(prtbuf, sizeof(prtbuf), "DLFtest message number %d", i);
    if (dlf_entertext("dlftest", i, prtbuf) < 0) {
      if (serrno != EEXIST) {
        fprintf (stderr, "Error entering message text\n");
        exit (SYERR);
      }
    }
  }


  /* 3) Call dlf_reinit("dlftest") */

  if (dlf_reinit("dlftest") < 0) {
    fprintf (stderr, "Error initializing global structure.\n");
    exit (SYERR);
  }

  /* 4) Create threads which will be generating log messages */

  for (i = 0; i < NBTHREADS; i++) {
    cid[i] = Cthread_create(&gen_log, &num_msgs);
  }

  /* 5) Wait for threads to finish */
  for (i = 0; i < NBTHREADS; i++) {
    Cthread_join(cid[i], &status);
  }
  free(cid);
  exit(0);
}

void *gen_log(arg)
     void *arg;
{
  Cuuid_t req_id;
  Cuuid_t sreq_id;
  int n;
  struct timeval cur_time;
  int sev;
  int msgn;
  int tpn;
  char tpname[10];
  u_signed64 nsinv;
  int pari;
  U_HYPER pari64;
  float parf;
  double pard;
  int rv;
  int num_msgs;
  struct Cns_fileid ns_fileid = { "cns.cern.ch", 0 };

  fprintf (stdout,
           "[%d] - Generating and writing log messages.\n",
           Cthread_self());
  /* Seed random number generator with microseconds */
  gettimeofday(&cur_time, NULL);
  srand(cur_time.tv_usec);

  num_msgs = *(int *)arg;
  for (n = 0; n < num_msgs; n++) {
    /* Generate request id and subrequest id */
    Cuuid_create(&req_id);
    Cuuid_create(&sreq_id);
    /* Take random severity level and random message number */
    sev = 1 + (int) (10.0 * rand()/(RAND_MAX+1.0));
    msgn = 1 + (int)(255.0 * rand()/(RAND_MAX+1.0));
    /* Generate tape name */
    tpn = 1 + (int)(9999.0 * rand()/(RAND_MAX+1.0));
    snprintf(tpname, sizeof(tpname), "TP%04d", tpn);
    ns_fileid.fileid = ((U_HYPER)(0XF1234567) << 32) + 0X87654321;
    pari = 32768;
    pari64 = ((U_HYPER)(0X12345678) << 32) + 0X87654321;
    parf = 32.768;
    pard = 65.535;
    
    rv = dlf_write (req_id, sev, msgn, &ns_fileid, 7,
                    "PINT", DLF_MSG_PARAM_INT, pari,
                    "PSTR", DLF_MSG_PARAM_STR, "This_is_a_string",
                    NULL,   DLF_MSG_PARAM_UUID, sreq_id,
                    "PI64", DLF_MSG_PARAM_INT64, pari64,
                    NULL,   DLF_MSG_PARAM_TPVID, tpname,
                    "PDBL", DLF_MSG_PARAM_DOUBLE, pard,
                    "PFLT", DLF_MSG_PARAM_FLOAT, parf);
    if (rv < 0) {
      fprintf (stderr, "[%d] dlf_write() returns %d\n", Cthread_self(), rv);
      break;
    }
  }
  fprintf (stdout,
           "[%d] %d log messages have been written\n",
           Cthread_self(), n);
  return(0);
}

