#include <Cthread_api.h>
#include <errno.h>
#include <stdio.h>

#define NTHREADS 5
#define NLOOP    5

static int global_key;
static int global2key;

void *mythread(void *);
void  testit();

int main() {
  int i, n;

  for (i=1; i <= NTHREADS; i++) {
    if ((n = Cthread_create(&mythread,NULL)) < 0) {
      fprintf(stdout,"### Cthread_create error No %d (%s)\n",
              errno,strerror(errno));
      exit(EXIT_FAILURE);
    } else {
      fprintf(stdout,"[main] --> Created CthreadID %d\n",n);
    }
  }

  exit(EXIT_SUCCESS);
}

void *mythread(void *arg) {
  int i;
  void *dummy = NULL;
  
  dummy = malloc(sizeof(int));

  /* Call the same routine NLOOP times */
  for (i=1; i <= NLOOP; i++) {
    testit();
  }

  free(dummy);

  return(NULL);
}

void testit() {
  char *addr = NULL;
  char *addr2 = NULL;
  int   n;

  fprintf(stdout,"[%d] Detaching ourself\n",Cthread_self());

  if ((n = Cthread_detach(Cthread_self()))) {
    fprintf(stdout,"### Error at Cthread_detach [returns %d] (%s)\n",n,strerror(errno));
    exit(EXIT_FAILURE);
  }

  if ((n = Cthread_getspecific(&global_key,(void **) &addr))) {
    fprintf(stdout,"### Error at Cthread_getspecific [returns %d] (%s)\n",n,strerror(errno));
    exit(EXIT_FAILURE);
    return;
  }
  if (addr == NULL) {
    addr = malloc(100);
    fprintf(stdout,"[%d] --> new addr = 0x%x\n",Cthread_self(),(unsigned int) addr);
    if (Cthread_setspecific(&global_key,addr)) {
      fprintf(stdout,"### Error at Cthread_setspecific (%s)\n",strerror(errno));
      exit(EXIT_FAILURE);
    }
  } else {
    fprintf(stdout,"[%d] --> old addr = 0x%x\n",Cthread_self(),(unsigned int) addr);
  }
  
  if ((n = Cthread_getspecific(&global2key,(void **) &addr2))) {
    fprintf(stdout,"### Error at Cthread_getspecific [returns %d] (%s)\n",n,strerror(errno));
    exit(EXIT_FAILURE);
    return;
  }
  if (addr2 == NULL) {
    addr2 = malloc(100);
    fprintf(stdout,"[%d] --> new addr2 = 0x%x\n",Cthread_self(),(unsigned int) addr2);
    if (Cthread_setspecific(&global2key,addr2)) {
      fprintf(stdout,"### Error at Cthread_setspecific (%s)\n",strerror(errno));
      exit(EXIT_FAILURE);
    }
  } else {
    fprintf(stdout,"[%d] --> old addr2 = 0x%x\n",Cthread_self(),(unsigned int) addr2);
  }
  
  sprintf(addr, "[%d] Print with TSD buffer1 : CthreadID=%d\n",Cthread_self(),Cthread_self());
  sprintf(addr2,"[%d] Print with TSD buffer2 : CthreadID=%d\n",Cthread_self(),Cthread_self());

  fprintf(stdout,addr);
  fprintf(stdout,addr2);

  return;
}
