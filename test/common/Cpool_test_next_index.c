/*
 * Cpool_test_next_index.c,v 1.2 1999-12-09 14:47:48+01 jdurand Exp
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)Cpool_test_next_index.c,v 1.2 1999-12-09 14:47:48+01 CERN/IT/PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <Cpool_api.h>
#include <stdio.h>
#include <errno.h>
#include <serrno.h>
#include <log.h>

#define NPOOL 2
#define PROCS_PER_POOL 5
#define TIMEOUT 2
void *testit(void *);
void *testit_forever(void *);
int special_pid = -1;

static int arguments[NPOOL + 1][PROCS_PER_POOL+1];

extern int Cthread_debug;
extern int Cpool_debug;

int main() {
  int pid;
  int i, j;
  int ipool[NPOOL + 1];
  int npool[NPOOL + 1];

  Cthread_debug = 0;
  Cpool_debug = 0;

  /* initlog("Cpool_test_next_index",LOG_DEBUG,""); */

  pid = getpid();

  fprintf(stderr,"\n*** ... Defining %d pools with %d elements each\n\n",
         NPOOL,PROCS_PER_POOL);

  for (i=0; i < NPOOL; i++) {
    if ((ipool[i] = Cpool_create(PROCS_PER_POOL,&(npool[i]))) < 0) {
      fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Error creating pool (%s,%s)\n",
             errno,serrno,strerror(errno),sstrerror(serrno));
    } else {
      fprintf(stderr,"... Pool No %d created with %d processes\n",
             ipool[i],npool[i]);
    }
  }

  fprintf(stderr,"\n*** ... Defining another \"forever\" pool with %d elements\n\n",
         PROCS_PER_POOL);

  /* Create a pool of thread waiting forever */
  if ((ipool[NPOOL] = Cpool_create(PROCS_PER_POOL,&(npool[NPOOL]))) < 0) {
    fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Error creating \"forever\" pool (%s,%s)\n",
           errno,serrno,strerror(errno),sstrerror(serrno));
  } else {
    fprintf(stderr,"... \"forever\" Pool No %d created with %d processes\n",
           ipool[NPOOL],npool[NPOOL]);
  }

  /* Test pool of threads that do not end forever */
  fprintf(stderr,"\n***... Testing non-blocking pools with a timeout of %d seconds\n\n",TIMEOUT);
  for (i=0; i < NPOOL; i++) {
    /* Loop on the number of processes + 1 ... */
    for (j=0; j <= npool[i]; j++) {
      int index;

      if ((index = Cpool_next_index_timeout(i,TIMEOUT)) < 0) {
        fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Can't get next_index_timeout to work in pool No %d (%s,%s)\n",
               errno,serrno,ipool[i],strerror(errno),sstrerror(serrno));
        continue;
      }

      arguments[i][index] = i*100 + j;
      fprintf(stderr,"... Assign in pool %d (timeout=%d) the routine No %d [0x%x(%d)] to thread %d\n",
             ipool[i],TIMEOUT,j,(unsigned int) testit,arguments[i][index],index);

      if (Cpool_assign(ipool[i], &testit, &(arguments[i][index]), TIMEOUT)) {
        fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Can't assign to pool No %d (%s,%s) the %d-th routine\n",
               errno,serrno,ipool[i],strerror(errno),sstrerror(serrno),j);
      } else {
        fprintf(stderr,"... Okay for assign to pool No %d of the %d-th routine\n",
               ipool[i],j);
      }
    }
  }
  
  fprintf(stderr,"\n***... Testing non-blocking pools with no timeout\n\n");
  for (i=0; i < NPOOL; i++) {
    /* Loop on the number of processes + 1 ... */
    for (j=0; j <= npool[i]; j++) {
      int index;

      if ((index = Cpool_next_index(i)) < 0) {
        fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Can't get next_index to work in pool No %d (%s,%s)\n",
               errno,serrno,ipool[i],strerror(errno),sstrerror(serrno));
        continue;
      }

      arguments[i][index] = i*100 + j;
      fprintf(stderr,"... Assign in pool %d (timeout=-1) the routine No %d [0x%x(%d)] to thread %d\n",
             ipool[i],j,(unsigned int) testit,arguments[i][index],index);

      if (Cpool_assign(ipool[i], &testit, &(arguments[i][index]), -1)) {
        fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Can't assign to pool No %d (%s,%s) the %d-th routine\n",
               errno,serrno,ipool[i],strerror(errno),sstrerror(serrno),j);
      } else {
        fprintf(stderr,"... Okay for assign to pool No %d of the %d-th routine\n",
               ipool[i],j);
      }
    }
  }
  
  /* Test pool of threads that do not end */
  fprintf(stderr,"\n***... Testing blocking pools (except one of threads) with a timeout of %d seconds\n\n",TIMEOUT);
  fflush(stdout);
  fflush(stderr);
  i = NPOOL;
  /* Loop on the number of processes + 1 ... */
  for (j=0; j <= npool[i]; j++) {
    int index;
    
    if ((index = Cpool_next_index_timeout(i,TIMEOUT)) < 0) {
      fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Can't get next_index_timeout to work in pool No %d (%s,%s)\n",
             errno,serrno,ipool[i],strerror(errno),sstrerror(serrno));
      continue;
    }

    arguments[i][index] = i*100 + j;
    fprintf(stderr,"... Assign in pool %d (timeout=%d) the \"forever\" routine No %d [0x%x(%d)] to thread %d\n",
           ipool[i],TIMEOUT,j,(unsigned int) testit_forever,arguments[i][index],index);
    
    if (Cpool_assign(ipool[i], &testit_forever, &(arguments[i][index]), TIMEOUT)) {
      fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Can't assign to pool No %d (%s,%s) the %d-th routine\n",
             errno,serrno,ipool[i],strerror(errno),sstrerror(serrno),j);
    } else {
      fprintf(stderr,"... Okay for assign to pool No %d of the %d-th routine\n",
             ipool[i],j);
    }
  }
  
  /* Test pool of threads that do not end */
  fprintf(stderr,"\n***... Testing blocking pools (except one of its threads) with no timeout\n\n");
  fflush(stdout);
  fflush(stderr);
  i = NPOOL;
  /* Loop on the number of processes + 1 ... */
  for (j=0; j <= npool[i]; j++) {
    int index;
    
    if ((index = Cpool_next_index(i)) < 0) {
      fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Can't get next_index to work in pool No %d (%s,%s)\n",
             errno,serrno,ipool[i],strerror(errno),sstrerror(serrno));
      continue;
    }

    arguments[i][index] = i*100 + j;
    fprintf(stderr,"... Assign in pool %d (timeout=-1) the \"forever\" routine No %d [0x%x(%d)] to thread %d\n",
           ipool[i],j,(unsigned int) testit_forever,arguments[i][index],index);
    
    if (Cpool_assign(ipool[i], &testit_forever, &(arguments[i][index]), -1)) {
      fprintf(stderr,"### [Errno,Serrno]=[%d,%d] Can't assign to pool No %d (%s,%s) the %d-th routine\n",
             errno,serrno,ipool[i],strerror(errno),sstrerror(serrno),j);
    } else {
      fprintf(stderr,"... Okay for assign to pool No %d of the %d-th routine\n",
             ipool[i],j);
    }
  }
  
  exit(EXIT_SUCCESS);
}

void *testit(void *arg) {
  int caller_pid, my_pid;

  my_pid = getpid();

  caller_pid = (int) * (int *) arg;

  fprintf(stderr,"... I am PID=%d called by pool %d, argument is %d, try No %d ... Exiting in %d seconds\n",
         my_pid,caller_pid/100,caller_pid,caller_pid - 100*(caller_pid/100), TIMEOUT*2);

  /*
   * Wait up to the timeout + 1
   */
  sleep(TIMEOUT*2);

  return(NULL);
}

void *testit_forever(void *arg) {
  int caller_pid, my_pid;

  my_pid = getpid();

  caller_pid = (int) * (int *) arg;

  if (my_pid == special_pid || caller_pid / 100 * 100 == caller_pid) {
    fprintf(stderr,"... I am PID=%d called by pool %d, argument is %d, try No %d ... Exiting in %d seconds\n",
           my_pid,caller_pid/100,caller_pid,caller_pid - 100*(caller_pid/100), TIMEOUT*3);
    special_pid = my_pid;
  } else {
    fprintf(stderr,"... I am PID=%d called by pool %d, argument is %d, try No %d ... No exit !\n",
           my_pid,caller_pid/100,caller_pid,caller_pid - 100*(caller_pid/100));
  }
    
  if (my_pid == special_pid) {
	  fprintf(stderr,"... sleep %d seconds\n", TIMEOUT*3);
    sleep(TIMEOUT*3);
    return(NULL);
  } else {
    while (1) {
      /*
       * Never exit this thread
       */
		fprintf(stderr,"... sleep %d seconds\n", TIMEOUT*2);
      sleep(TIMEOUT*2);
    }
  }

  return(NULL);
}

