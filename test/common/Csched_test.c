/*
 * $Id: Csched_test.c,v 1.1 2000/06/13 14:49:28 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char *sccsid = "@(#)$RCSfile: Csched_test.c,v $ $Revision: 1.1 $ $Date: 2000/06/13 14:49:28 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif

/*******************************************************************************
 * Base on:
 *******************************************************************************
 *
 * Experiment #6: Scheduling, Priorities, and Priority Inversion
 *
 *    Programmer: Eric Sorton
 *          Date: 3/10/97
 *           For: MSE599, Special Topics Class
 *
 *       Purpose: This program demostrates the priority inversion of two
 *		 processes.  To demonstrate this, three processes are created
 *		 along with one shared memory segment and one sempahore.  The
 *		 shared memory segment is used to allow the processes to
 *		 communicate who has the lock.  The semaphore is the critical
 *		 resource that both two processes need.
 *
 *		 Three processes will be started, a high priority process,
 *		 a middle priority process, and a low priority process.  The
 *		 high and low priority processes both need to get the lock.
 *		 It is critical that the high priority process get the lock
 *		 quickly when it needs it.
 *
 *		 However, this is not the case, the following scenario happens:
 *
 *			1) Low priority process gets lock
 *			2) High priority process needs lock
 *			3) Middle priority process runs and takes up a lot
 *				of CPU time delaying the time that it takes
 *				the High priority process to get the lock.
 *			4) When Middle finally finishes, Low runs, releases
 *				Lock.
 *			5) Finally, High gets the Lock.
 *
 *******************************************************************************/

#include "Cthread_api.h"        /* Thread interface */
#include "Cnetdb.h"             /* For h_errno external pb */
#include "osdep.h"              /* Castor's OS dependencies */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <errno.h>              /* errno and error codes */
#include "serrno.h"             /* Castor's errno and error codes */

/* The scheduler (default) */
#define SCHEDULER       "OTHER"

/* The number of children (default) that will be created */
#define NO_OF_CHILDREN 3

/* The priorities (default) */
#define PARENT_PRIORITY(priority) ( Csched_get_priority_max(priority) )
#define HIGH_PRIORITY(priority)   ( Csched_get_priority_max(priority) - 1 > 0 ? Csched_get_priority_max(priority)  - 1  : ( Csched_get_priority_max(priority) >= 0 ? Csched_get_priority_max(priority) : -1))
#define MEDIUM_PRIORITY(priority) ( Csched_get_priority_mid(priority) )
#define LOW_PRIORITY(priority)    ( Csched_get_priority_min(priority) )

#define UNLOCKED        1
#define HIGH_HAS_LOCK   2
#define LOW_HAS_LOCK    3

/* =============== */
/* Local functions */
/* =============== */
void _Csched_test_usage _PROTO(());
void *_Csched_test_doit _PROTO((void *));
void *High _PROTO((void *));
void *Medium _PROTO((void *));
void *Low _PROTO((void *));

int thread_status_ok = 0;
int thread_status_notok = -1;
int thread_arg[3];
int high_priority = -1;
int medium_priority = -1;
int low_priority = -1;
int lockit;

int main(argc,argv)
     int argc;
     char **argv;
{
  int rtn;
  int count;
  extern char *optarg;
  extern int optind, opterr, optopt;
  int c;
  int   scheduler = CSCHED_OTHER;
  int   parent_priority = -1;
  int   errflg = 0;
  int   cid, *status;

  while ((c = getopt(argc,argv,"S:P:H:M:L:h")) != EOF) {
    switch (c) {
    case 'S':
      if (strcmp(optarg,"FIFO") == 0) {
        scheduler = CSCHED_FIFO;
      } else if (strcmp(optarg,"RR") == 0) {
        scheduler = CSCHED_RR;
      } else if (strcmp(optarg,"OTHER") == 0) {
        scheduler = CSCHED_OTHER;
      } else if (strcmp(optarg,"FG_NP") == 0) {
        scheduler = CSCHED_FG_NP;
      } else if (strcmp(optarg,"BG_NP") == 0) {
        scheduler = CSCHED_BG_NP;
      } else {
        fprintf(stderr,"Wrong scheduler parameter \"%s\". Should be \"FIFO\", \"RR\", \"OTHER\", \"FG_NP\" or \"BG_NP\n",optarg);
        ++errflg;
      }
      break;
    case 'P':
      parent_priority = atoi(optarg);
      break;
    case 'H':
      high_priority = atoi(optarg);
      break;
    case 'M':
      medium_priority = atoi(optarg);
      break;
    case 'L':
      low_priority = atoi(optarg);
      break;
    case '?':
      ++errflg;
      printf("Unknown option\n");
      break;
    default:
      ++errflg;
      printf("?? getopt returned character code 0%o (octal) 0x%lx (hex) %d (int) '%c' (char) ?\n"
          ,c,(unsigned long) c,c,(char) c);
      break;
    }
    if (errflg != 0) {
      printf("### getopt error\n");
      _Csched_test_usage();
      return(EXIT_FAILURE);
    }  
  }

  if (parent_priority < 0) parent_priority = PARENT_PRIORITY(scheduler);
  if (high_priority < 0)   high_priority   = HIGH_PRIORITY  (scheduler);
  if (medium_priority < 0) medium_priority = MEDIUM_PRIORITY(scheduler);
  if (low_priority < 0)    low_priority    = LOW_PRIORITY(scheduler);

  printf("... Default scheduler: ");
  if (scheduler == CSCHED_FIFO) {
    printf("CSCHED_FIFO\n");
  } else if (scheduler == CSCHED_RR) {
    printf("CSCHED_RR\n");
  } else if (scheduler == CSCHED_OTHER) {
    printf("CSCHED_OTHER\n");
  } else if (scheduler == CSCHED_FG_NP) {
    printf("CSCHED_FG_NP\n");
  } else if (scheduler == CSCHED_BG_NP) {
    printf("CSCHED_BG_NO\n");
  }
  printf("... Possible priority range : [%d,%d]\n",
          Csched_get_priority_min(scheduler),Csched_get_priority_max(scheduler));
  printf("... Chosen Parent       Priority  :  %d\n",parent_priority);
  printf("... Chosen Child's High Priority  :  %d\n",high_priority);
  printf("... Chosen Child's Mid  Priority  :  %d\n",medium_priority);
  printf("... Chosen Child's Low  Priority  :  %d\n",low_priority);

  /* We create the parent thread */
  thread_arg[0] = scheduler;
  thread_arg[1] = parent_priority;
  thread_arg[2] = 0;
  if ((cid = Cthread_create(&_Csched_test_doit,NULL)) < 0) {
    fprintf(stderr,"### Cthread_create error (%s - %s)\n",
            strerror(errno),
            sstrerror(serrno));
    return(EXIT_FAILURE);
  }

  if (Cthread_join(cid,&status) != 0) {
    fprintf(stderr,"### Cthread_join error (%s - %s)\n",
            strerror(errno),
            sstrerror(serrno));
  }

  fprintf(stderr,"Test thread exiting with status %d%s\n",status != NULL ? *status : -1, status != NULL ? "" : " <unknown>");
}

void *_Csched_test_doit(arg)
     void *arg;
{
  Csched_param_t param;
  int   i;
  int pid[NO_OF_CHILDREN];
  int child_arg[NO_OF_CHILDREN];

  if (thread_arg[2] >= 0) {
    fprintf(stdout,"Parent: Setting default scheduler to %d, default priority %d\n",
            thread_arg[0],
            thread_arg[1]);
    param.sched_priority = thread_arg[1];
    if (Csched_setschedparam(Cthread_self(), thread_arg[0], &param) != 0) {
      fprintf(stderr,"### Csched_setschedparam error (%s - %s) [Are you root ?]\n",
              strerror(errno),
              sstrerror(serrno));
      return((void *) &thread_status_notok);
    }
    thread_arg[2] = -1;

    /* We create the child threads */
    for (i = 0; i < NO_OF_CHILDREN; i++) {
      child_arg[i] = i;
      if ((pid[i] = Cthread_create(&_Csched_test_doit,&(child_arg[i]))) < 0) {
        fprintf(stderr,"### Cthread_create error (%s - %s)\n",
                strerror(errno),
                sstrerror(serrno));
        return((void *) &thread_status_notok);
      }
    }
    
    for (i = 0; i < NO_OF_CHILDREN; i++) {
      Cthread_join(pid[i],NULL);
    }

    return((void *) &thread_status_ok);
  } else {
    int childno = (int) * (int *) arg;
    char *more[3] = { "High Priority"  ,
                      "Medium Priority",
                      "Low Priority"
    };

    printf("Child No %d (%s) : executing\n", childno, more[childno]);
    printf("Child No %d (%s) : Set our sched_priority to ", childno, more[childno]);
    switch (childno) {
    case 0:
      printf("%d\n",param.sched_priority = high_priority);
      break;
    case 1:
      printf("%d\n",param.sched_priority = medium_priority);
      break;
    case 2:
      printf("%d\n",param.sched_priority = low_priority);
      break;
    }
    if (Csched_setschedparam(Cthread_self(),thread_arg[0],&param) != 0) {
      fprintf(stderr,"Child No %d (%s) : ### Csched_setschedparam error (%s - %s) [Are you root ?]\n",
              childno,
              more[childno],
              strerror(errno),
              sstrerror(serrno));
      return((void *) &thread_status_notok);
    }
    switch (childno) {
    case 0:
      /* High priority */
      while (1) {
        usleep(1000);
        printf("Child No %d (%s): Trying to get the lock\n", childno, more[childno]);
        if (Cthread_lock_mtx(&lockit,-1) != 0) {
          fprintf(stderr,"Child No %d (%s) : ### Cthread_lock_mtx error (%s - %s)\n",
                  childno,
                  more[childno],
                  strerror(errno),
                  sstrerror(serrno));
          return((void *) &thread_status_notok);
        }
        lockit = HIGH_HAS_LOCK;
        printf("Child No %d (%s): Got the lock\n", childno, more[childno]);
        usleep(1000);
        printf("Child No %d (%s): Release the lock\n", childno, more[childno]);
        if (Cthread_mutex_unlock(&lockit) != 0) {
          fprintf(stderr,"Child No %d (%s) : ### Cthread_mutex_unlock error (%s - %s)\n",
                  childno,
                  more[childno],
                  strerror(errno),
                  sstrerror(serrno));
        }
        lockit = UNLOCKED;
      }
    case 1:
      /* Medium priority */
      /* It checks to see if the low priority has the lock. If it does, */
      /* it will enter a long loop, granning all the processes...       */
      while (1) {
        if (lockit == LOW_HAS_LOCK) {
          printf("Child No %d (%s): Low has the lock. Entering long loop !\n", childno, more[childno]);
          for (i = 0; i < 100000000; i++);
          printf("Child No %d (%s): Low has the lock. Exiting long loop !\n", childno, more[childno]);
        }
        usleep(100);
      }
    case 2:
      /* Low priority */
      /* It loops forever. attempting to get the lock at a low rate. One it gets   */
      /* the lock, it will host it for a short period of time and then release it. */
      while (1) {
        usleep(1000);
        printf("Child No %d (%s): Trying to get the lock\n", childno, more[childno]);
        if (Cthread_lock_mtx(&lockit,-1) != 0) {
          fprintf(stderr,"Child No %d (%s) : ### Cthread_lock_mtx error (%s - %s)\n",
                  childno,
                  more[childno],
                  strerror(errno),
                  sstrerror(serrno));
          return((void *) &thread_status_notok);
        }
        lockit = LOW_HAS_LOCK;
        printf("Child No %d (%s): Got the lock\n", childno, more[childno]);
        usleep(1000);
        printf("Child No %d (%s): Release the lock\n", childno, more[childno]);
        if (Cthread_mutex_unlock(&lockit) != 0) {
          fprintf(stderr,"Child No %d (%s) : ### Cthread_mutex_unlock error (%s - %s)\n",
                  childno,
                  more[childno],
                  strerror(errno),
                  sstrerror(serrno));
        }
        lockit = UNLOCKED;
        break;
      }
    }
  }
  return(NULL);
}

void _Csched_test_usage() {
  fprintf(stderr,
          "  Usage: Csched_test [-S scheduler]\n"
          "                     [-P parent_priority]\n"
          "                     [-H high_priority]\n"
          "                     [-M medium_priority]\n"
          "                     [-L low_priority]\n"
          "                     [-h]\n"
          "\n"
          "  where options are:\n"
          "\n"
          "  -S scheduler       Default scheduler, default to OTHER\n"
          "                     Possible values are FIFO (if supported)\n"
          "                                         RR   (if supported)\n"
          "                                         OTHER\n"
          "                                         FG_NP\n"
          "                                         BG_NP\n"
          "                     Default to %s\n"
          "  -P parent_priority Parent default priority. Default to %d and is adapted to -S option automatically if not set on the command-line\n"
          "  -H high_priority   Default child's high priority. Default to %d and is adapted to -S option automatically if not set on the command-line\n"
          "  -M medium_priority Default child's medium priority. Default to %d and is adapted to -S option automatically if not set on the command-line\n"
          "  -L low_priority    Default child's low priority. Default to %d and is adapted to -S option automatically if not set on the command-line\n"
          "  -h                 This help.\n"
          "\n"
          "  Comments to <castor-support@listbox.cern.ch>\n",
          SCHEDULER,
          PARENT_PRIORITY(CSCHED_OTHER),
          HIGH_PRIORITY(CSCHED_OTHER),
          MEDIUM_PRIORITY(CSCHED_OTHER),
          LOW_PRIORITY(CSCHED_OTHER));
}
