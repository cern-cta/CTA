#include <stdio.h>
#include <netdb.h>
#include <pthread.h>
#include <Cthread_api.h>

const int success = 1;
char *server;

void *worker2(void *arg) {
   void *status;
   struct hostent *hp;
   fprintf(stderr,"\t\tHello I'm the second worker %d, my arg=%d\n",
	Cthread_self(),*(int *)arg);
   
   hp = gethostbyname(server);
   fprintf(stderr,"%s -> 0x%x\n",server,((struct in_addr *)(hp->h_addr))->s_addr); 
   sleep(2);
   status = (void *)&success;
   fprintf(stderr,"\t\tSecond worker (%d) exits\n",Cthread_self());
   free(arg);
   return((void *)&success);
}

void *worker(void *arg) {
   void *status;
   int *crap;
   fprintf(stderr,"\tHello I'm the first worker %d, my arg=%d\n",Cthread_self(),
      *(int *)arg);
   crap = (int *)malloc(sizeof(int));
   *crap = *(int *)arg + 1;
   Cthread_create_detached(worker2,(void *)crap);
   status = (void *)&success;
   fprintf(stderr,"\tFirst worker (%d) exits\n",Cthread_self());
   return((void *)&success);
}

main(int argc, char *argv[]) {
   int i,poolsize,PoolID,*crap,*tmpcrap,rc;  

   server = argv[1];
   crap = (int *)malloc(10*sizeof(int));

   poolsize = 10;
   fprintf(stderr,"main() create pool of size 10\n");
   PoolID = Cpool_create(poolsize,&poolsize);
   for (i = 1; i<10; i++) {
	   fprintf(stderr,"main() start a worker\n");
	   rc = Cpool_next_index(PoolID);
	   tmpcrap = &crap[rc];
	   *tmpcrap = i;
	   Cpool_assign(PoolID,worker,(void *)tmpcrap,-1);
	   sleep(1);
   }
   sleep(16);
   exit;
}
