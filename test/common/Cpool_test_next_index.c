#include <Cpool_api.h>
#include <stdio.h>
#include <errno.h>

#define NPOOL 2
#define PROCS_PER_POOL 20
#define TIMEOUT 2
void *testit(void *);

static int arguments[NPOOL][PROCS_PER_POOL+1];

int main() {
  int pid;
  int i, j;
  int ipool[NPOOL];
  int npool[NPOOL];

  pid = getpid();

  printf("... Defining %d pools with %d elements each\n",
         NPOOL,PROCS_PER_POOL);

  for (i=0; i < NPOOL; i++) {
    if ((ipool[i] = Cpool_create(PROCS_PER_POOL,&(npool[i]))) < 0) {
      printf("### Error No %d creating pool (%s)\n",
             errno,strerror(errno));
    } else {
      printf("... Pool No %d created with %d processes\n",
             ipool[i],npool[i]);
    }
  }

  for (i=0; i < NPOOL; i++) {
    /* Loop on the number of processes + 1 ... */
    for (j=0; j <= npool[i]; j++) {
      int index;

      index = Cpool_next_index(i);

      arguments[i][index] = i*100 + j;
      printf("... Assign to pool %d (timeout=%d) the routine No %d [0x%x(%d)] to thread %d\n",
             ipool[i],TIMEOUT,j,(unsigned int) testit,arguments[i][index],index);

      if (Cpool_assign(ipool[i], testit, &(arguments[i][index]), TIMEOUT)) {
        printf("### Can't assign to pool No %d (errno=%d [%s]) the %d-th routine\n",
               ipool[i],errno,strerror(errno),j);
      } else {
        printf("... Okay for assign to pool No %d of the %d-th routine\n",
               ipool[i],j);
      }
    }
  }
  
  /* We wait enough time for our threads to terminate... */
  sleep(TIMEOUT*NPOOL*PROCS_PER_POOL);

  exit(EXIT_SUCCESS);
}

void *testit(void *arg) {
  int caller_pid, my_pid;

  my_pid = getpid();

  caller_pid = (int) * (int *) arg;

  printf("... I am PID=%d called by pool %d, argument is %d, try No %d\n",
         my_pid,caller_pid/100,caller_pid,caller_pid - 100*(caller_pid/100));

  /*
   * Wait up to the timeout + 1
   */
  sleep(TIMEOUT*2);

  return(NULL);
}




