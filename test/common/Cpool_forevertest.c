#include <Cpool_api.h>
#include <stdio.h>
#include <errno.h>
#include <serrno.h>
#include <log.h>
#include <osdep.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <time.h>

void *master_thread (void *);
void *testit (void *);
int npools;
int nthreads_per_pool;

extern int Cpool_debug;
extern int Cthread_debug;

int main(argc,argv)
	int argc;
	char **argv;
{
	int i;
	int n;

	if (argc != 3) {
		fprintf(stderr, "Usage: %s <nb_pool> <n_threads_per_pool>\n", argv[0]);
		exit(1);
	}
	n = atoi(argv[1]);
	if (n <= 0) {
		fprintf(stderr, "Usage: %s <nb_pool> <n_threads_per_pool> where nb_pool MUST be > 0\n", argv[0]);
		exit(1);
	}
	
	if ((nthreads_per_pool = atoi(argv[2])) <= 0) {
		fprintf(stderr, "Usage: %s <nb_pool> <n_threads_per_pool> where n_threads_per_pool MUST be > 0\n", argv[0]);
		exit(1);
	}
	
	Cpool_debug = 0;
	Cthread_debug = 0;

	initlog("Cpool_forevertest",LOG_DEBUG,"");

	for (i = 0; i < n; i++) {
		log(LOG_INFO,"Creating master thread No %d\n", i);
		if (Cthread_create(&master_thread,NULL) < 0) {
			log(LOG_ERR,"Create_create error, %s\n", sstrerror(serrno));
			exit(0);
		}
	}

	/* Wait forever */
	while (1) {
		sleep(1);
	}

	exit(0);
}

void *master_thread(arg)
	void *arg;
{
	int i, j;
	time_t thistime;

	log(LOG_INFO,"Started\n");
	log(LOG_INFO,"Creating a pool of %d threads\n", nthreads_per_pool);
	
	if ((j = Cpool_create(nthreads_per_pool, NULL)) < 0) {
		log(LOG_ERR,"Cpool_create error: %s\n", i+1, sstrerror(errno));
		exit(1);
	} else {
		log(LOG_INFO,"Pool of %d threads created\n", nthreads_per_pool);
	}
	
	log(LOG_INFO,"Looping on Cpool_next_index and Cpool_assign\n");
	thistime = time(NULL);
	while (1) {
		int k;

		log(LOG_ERR,"Cpool_next_index_timeout(%d,-1)\n", j);
		if ((k = Cpool_next_index_timeout(j,-1)) < 0) {
			log(LOG_ERR,"Cpool_next_index_timeout error: %s\n", sstrerror(errno));
		} else {
			log(LOG_ERR,"Cpool_next_index_timeout returned %d\n", k);
		}
		log(LOG_ERR,"Cpool_assign(%d,testit,NULL,-1)\n", j);
		if (Cpool_assign(j,testit,NULL,-1) < 0) {
			log(LOG_ERR,"Cpool_assign error: %s\n", sstrerror(errno));
		}
	}
}

void * testit (void *arg) {
	/* Use a random number between 0 and 1M (we use usleep) */
	unsigned long thattime;
	
	thattime = 1+(int) (1000000.0*rand()/(RAND_MAX+1.0));
	
	log(LOG_ERR,"usleep(%ld)\n", thattime);
	usleep(thattime);
	log(LOG_ERR,"return\n", thattime);
	return(NULL);
}
