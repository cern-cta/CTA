#include <Cpool_api.h>
#include <stdio.h>
#include <errno.h>
#include <serrno.h>
#include <log.h>
#include <osdep.h>
#ifndef _WIN32
#include <unistd.h>
#endif

void *master_thread (void *);
void *testit (void *);
#define NMASTER_THREADS 10 /* Maximum: 10 */
int nthread_per_master[NMASTER_THREADS] = { 10,10,10,10,10,10,10,10,10,10 };
int poolid[NMASTER_THREADS];
int cid[NMASTER_THREADS];
int nassign[NMASTER_THREADS] = {1000,100000,100000,100000,100000,100000,100000,100000,100000,100000};

extern int Cpool_debug;

int main(argc,argv)
	int argc;
	char **argv;
{
	int i;
	int n;

	if (argc < 2) {
		fprintf(stderr, "Usage: %s <nb_thread> [n_assignment_per_thread] where nb_thread is between 1 and 10 and n_assignment_per_thread is > 0\n", argv[0]);
		exit(1);
	}
	n = atoi(argv[1]);
	if ((n < 1) || (n > 10)) {
		fprintf(stderr, "Usage: %s <nb_thread> [n_assignment_per_thread] where nb_thread MUST be between 1 and 10\n", argv[0]);
		exit(1);
	}
	if (argc == 3) {
		int z;
		
		if ((z = atoi(argv[2])) <= 0) {
			fprintf(stderr, "Usage: %s <nb_thread> [n_assignment_per_thread] where n_assignment_per_thread MUST be > 0\n", argv[0]);
			exit(1);
		}
		for (i = 0; i < n; i++) {
			nassign[i] = z;
		}
	}
	
	Cpool_debug = 0;

	initlog("Cpool_multiple_pools",LOG_DEBUG,"");

	for (i = 0; i < n; i++) {
		log(LOG_INFO,"Creating master thread No %d (will call %d times Cpool_assign)\n", i, nassign[i]);
		if ((cid[i] = Cthread_create(&master_thread,&poolid[i])) < 0) {
			log(LOG_ERR,"Create_create error, %s\n", sstrerror(serrno));
			exit(0);
		}
	}
	for (i = 0; i < NMASTER_THREADS; i++) {
		Cthread_join(cid[i],NULL);
	}
	/* Sleep 'forever' */
	/* sleep(10000); */

	exit(0);
}

void *master_thread(arg)
	void *arg;
{
	int i, j, n;

	for (i = 0; i < 10; i++) {
		if (arg == &poolid[i]) break;
	}

	log(LOG_INFO,"[Master No %d] Started\n", i+1);

	log(LOG_INFO,"[Master No %d] Creating a pool of %d threads\n", i+1, nthread_per_master[i]);

	if ((j = Cpool_create(nthread_per_master[i], NULL)) < 0) {
		log(LOG_ERR,"[Master No %d] Cpool_create error: %s\n", i+1, sstrerror(errno));
		exit(1);
	} else {
		log(LOG_INFO,"[Master No %d] Pool of %d threads created, PoolID=%d\n", i+1, nthread_per_master[i], j);
	}

	log(LOG_INFO,"[Master No %d] Looping on Cpool_next_index and Cpool_assign %d times\n", i+1, nassign[i]);
	n = 0;
	while (1) {
		int k;
		k = Cpool_next_index(j);
		if (Cpool_assign(j,testit,arg,10) < 0) {
			log(LOG_ERR,"[Master No %d] Cpool_assign error: %s\n", i+1, sstrerror(errno));
			exit(1);
		}
		if (++n > nassign[i]) {
			return(NULL);
		}
	}
}

void * testit (void *arg) {
	/*
	  int i;
	  unsigned long thattime;

	  if (arg == &poolid[0]) i = 0;
	  if (arg == &poolid[1]) i = 1;
	  if (arg == &poolid[2]) i = 2;

	  thattime = 1+(int) (1000000.0*rand()/(RAND_MAX+1.0));

	  usleep(thattime);
	*/
	return(NULL);
}
