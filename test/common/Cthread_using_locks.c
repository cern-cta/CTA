/*
 * $Id: Cthread_using_locks.c,v 1.2 1999/12/09 13:47:49 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cthread_using_locks.c,v $ $Revision: 1.2 $ $Date: 1999/12/09 13:47:49 $ CERN/IT/PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <Cthread_api.h> /* For Cthread library      */
#include <stdio.h>       /* For stderr, etc...       */
#include <stdlib.h>      /* For EXIT_FAILURE, etc... */
#include <errno.h>       /* For errno ...            */

int loop_sleep = 0;
int thread_sleep = 0;

void *mythread(void *);
int   lock;

int main (int argc, char **argv) {
  /* usage: number of threads to create */
  int i;

  if (argc < 6) {
    fprintf(stderr,
            "Usage: %s <nthread> <detach> <join> <loop_sleep> <thread_sleep>\n"
            "\n"
            "where: nthread       Number of threads to create\n"
            "       detach        non-zero value: detach them\n"
            "       join          if detach is 0, a non-value\n"
            "                     for this parameter will mean\n"
            "                     a call to pthread_join for\n"
            "                     created thread, thus garbaging\n"
            "                     the Cthread library: All created\n"
            "                     threaded will have a Cthread ID = 0 !\n"
            "       loop_sleep    Seconds between thread creation\n"
            "       thread_sleep  Seconds before each thread dies\n"
            "\n"
            "Note: you can play with loop_sleep and thread_sleep\n"
            "      parameters. For example, with a loop_sleep=1 and\n"
            "      thread_sleep=2, the program will finish BEFORE the\n"
            "      threads, then you will not see all printouts, BUT\n"
            "      you will see that the garbage collection works\n"
            "      reading the Cthread ID of the threads that are auto-\n"
            "      -destroyuing them selves\n"
            "\n"
            ,argv[0]);
    exit(EXIT_FAILURE);
  }

  loop_sleep = atoi(argv[4]);
  thread_sleep = atoi(argv[5]);

  fprintf(stderr,"######## Creating %d threads\n",atoi(argv[1]));
  if (atoi(argv[2])) {
    fprintf(stderr,"######## They will be DETACHED\n"
            "... This means that unless Cthread_destroy(Cthread_self()) is\n"
            "... called at the end of each of them, Cthread will assign\n"
            "... a increasing Cthread ID to each new thread\n");
  } else {
    fprintf(stderr,"######## They will NOT be detached\n");
  }
    
  for (i=1; i <= atoi(argv[1]); i++) {
    int cid;
    if (atoi(argv[2])) {
      cid = Cthread_create_detached(mythread,argv[2]);
    } else {
      cid = Cthread_create(mythread,argv[2]);
    }
    if (cid < 0) {
      fprintf(stderr,">>> Error %d (%s)\n",errno,strerror(errno));
      continue;
    }
    Cthread_mutex_lock(&lock);
    fprintf(stderr,"Thread %d created\n",cid);
    Cthread_mutex_unlock(&lock);
    if (atoi(argv[3]))
      if (Cthread_join(cid,NULL)) {
        fprintf(stderr,">>> Error %d joining thread %d (%s)\n",
                errno,cid,strerror(errno));
      }
    /* Wait a bit */
    sleep(loop_sleep);
  }

  /* Let all detached threads finish */
  if (atoi(argv[2])) {
    fprintf(stderr,"<<< Waiting for all threads to terminate >>>\n");
    fprintf(stderr,"<<< Press control-c to force the exit... >>>\n");            
    sleep(atoi(argv[1]) * thread_sleep);
  }
}

void *mythread(void *arg) {
  int to_detach;
  int cid;
  int *test;

  sleep(1);

  /* Get argument */
  to_detach = atoi((char *)arg);

  if (to_detach) {
    Cthread_detach(Cthread_self());
    Cthread_mutex_lock(&lock);
    fprintf(stderr,"Hello I am thread %d detaching myself\n",Cthread_self());
    Cthread_mutex_unlock(&lock);
    Cthread_detach(Cthread_self());
  } else {
    Cthread_mutex_lock(&lock);
    fprintf(stderr,"Hello I am thread %d not detaching myself\n",Cthread_self());
    Cthread_mutex_unlock(&lock);
  }

  /* Wait a bit */
  sleep(thread_sleep);

  cid = Cthread_self();
  if (Cthread_destroy(cid)) {
    fprintf(stderr,">>> Error %d auto-destroying thread %d (%s)\n",
            errno,cid,strerror(errno));
  } else {
    fprintf(stderr,"Thread %d auto-destroyed\n",cid);
  }


  return(NULL);
}





