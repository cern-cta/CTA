/*
 * $Id: Cpool_test.c,v 1.3 2004/03/16 15:02:11 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cpool_test.c,v $ $Revision: 1.3 $ $Date: 2004/03/16 15:02:11 $ CERN/IT/PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <Cpool_api.h>
#include <stdio.h>
#include <errno.h>
#include <log.h>

#define NPOOL 2
#define PROCS_PER_POOL 2
#define TIMEOUT 2
void *testit(void *);

extern int Cthread_debug;
extern int Cpool_debug;

int main() {
  int pid;
  int i, j;
  int ipool[NPOOL];
  int npool[NPOOL];
  int *arg;

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
      if ((arg = malloc(sizeof(int))) == NULL) {
        fprintf(stderr,"### Malloc error, errno = %d (%s)\n",
               errno,strerror(errno));
        continue;
      }
      *arg = i*10+j;
      fprintf(stderr,"... Assign to pool %d (timeout=%d) the %d-th routine 0x%x(%d)\n",
             ipool[i],TIMEOUT,j+1,(unsigned int) testit,*arg);
      if (Cpool_assign(ipool[i], testit, arg, TIMEOUT)) {
        fprintf(stderr,"### Can't assign to pool No %d (errno=%d [%s]) the %d-th routine\n",
               ipool[i],errno,strerror(errno),j);
        free(arg);
      } else {
        fprintf(stderr,"... Okay for assign to pool No %d of the %d-th routine\n",
               ipool[i],j);
#ifndef _CTHREAD
        /* Non-thread environment: the child is in principle not allowed */
        /* to do free himself                                            */
        free(arg);
#endif
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

#ifdef _CTHREAD
  /* Thread environment : we free the memory */
  free(arg);
#endif

  fprintf(stderr,"... I am PID=%d called by pool %d, try No %d\n",
         my_pid,caller_pid/10,caller_pid - 10*(caller_pid/10));

  /*
   * Wait up to the timeout + 1
   */
  fprintf(stderr,"... sleep %d seconds\n", TIMEOUT*2);
  sleep(TIMEOUT*2);

  return(NULL);
}




