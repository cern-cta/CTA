/*
 * $Id: Cpool_test2.c,v 1.3 2004/03/16 15:02:11 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cpool_test2.c,v $ $Revision: 1.3 $ $Date: 2004/03/16 15:02:11 $ CERN/IT/PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <Cpool_api.h>
#include <stdio.h>
#include <errno.h>
#include <log.h>

#define NPOOL 2
#define PROCS_PER_POOL 20
#define TIMEOUT 2
void *testit(void *);

static int arguments[NPOOL][PROCS_PER_POOL+1];

extern int Cthread_debug;
extern int Cpool_debug;

int main() {
  int pid;
  int i, j;
  int ipool[NPOOL];
  int npool[NPOOL];

  Cthread_debug = 0;
  Cpool_debug = 0;

  initlog("Cpool_test_next_index",LOG_DEBUG,"");

  pid = getpid();

  fprintf(stderr,"... Defining %d pools with %d elements each\n",
         NPOOL,PROCS_PER_POOL);

  for (i=0; i < NPOOL; i++) {
    if ((ipool[i] = Cpool_create(PROCS_PER_POOL,&(npool[i]))) < 0) {
      fprintf(stderr,"### Error No %d creating pool (%s)\n",
             errno,strerror(errno));
    } else {
      fprintf(stderr,"... Pool No %d created with %d processes\n",
             ipool[i],npool[i]);
    }
  }

  for (i=0; i < NPOOL; i++) {
    /* Loop on the number of processes + 1 ... */
    for (j=0; j <= npool[i]; j++) {
      int index;

      index = Cpool_next_index(i);

      arguments[i][index] = i*100 + j;
      fprintf(stderr,"... Assign to pool %d (timeout=%d) the routine No %d [0x%x(%d)] to thread %d\n",
             ipool[i],TIMEOUT,j,(unsigned int) testit,arguments[i][index],index);

      if (Cpool_assign(ipool[i], testit, &(arguments[i][index]), TIMEOUT)) {
        fprintf(stderr,"### Can't assign to pool No %d (errno=%d [%s]) the %d-th routine\n",
               ipool[i],errno,strerror(errno),j);
      } else {
        fprintf(stderr,"... Okay for assign to pool No %d of the %d-th routine\n",
               ipool[i],j);
      }
    }
  }
  
  /* We wait enough time for our threads to terminate... */
  fprintf(stderr,"... Fine... sleep %d seconds\n", TIMEOUT*NPOOL*PROCS_PER_POOL);
  sleep(TIMEOUT*NPOOL*PROCS_PER_POOL);

  exit(EXIT_SUCCESS);
}

void *testit(void *arg) {
  int caller_pid, my_pid;

  my_pid = getpid();

  caller_pid = (int) * (int *) arg;

  fprintf(stderr,"... I am PID=%d called by pool %d, argument is %d, try No %d\n",
         my_pid,caller_pid/100,caller_pid,caller_pid - 100*(caller_pid/100));

  /*
   * Wait up to the timeout + 1
   */
  fprintf(stderr,"... sleep %d seconds\n", TIMEOUT*2);
  sleep(TIMEOUT*2);

  return(NULL);
}




