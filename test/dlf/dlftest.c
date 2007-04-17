/******************************************************************************************************
 *                                                                                                    *
 * dlftest.c - Castor Distribution Logging Facility (Test Suite)                                      *
 * Copyright (C) 2006 CERN IT/FIO (castor-dev@cern.ch)                                                *
 *                                                                                                    *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU *
 * General Public License as published by the Free Software Foundation; either version 2 of the       *
 * License, or (at your option) any later version.                                                    *
 *                                                                                                    *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without  *
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU     *
 * General Public License for more details.                                                           *
 *                                                                                                    *
 * You should have received a copy of the GNU General Public License along with this program; if not, *
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307,  *
 * USA.                                                                                               *
 *                                                                                                    *
 ******************************************************************************************************/

/* headers */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "Cglobals.h"
#include "Cmutex.h"
#include "Cthread_api.h"
#include "Cuuid.h"
#include "dlf_api.h"
#include "dlf_lib.h"

/* definitions */
#define DEFAULT_THREAD_COUNT 1        /**< number of threads to use to generate messages  */
#define DEFAULT_MSG_TOTAL    1000     /**< the default total number of mesages to send    */

/* mutexes
 *   - used to block multiple worker threads from violating the count of total messages
 */
int test_mutex;

/* global variables */
int extensive = 0;
int rate      = 1;
int total     = 0;
int count     = 0;


/*
 * Generate string
 */

void genstring(char *str, int len) {

	/* variables */
        int  i = 0;
        char c = 0;

        for(i = 0; i < len; i++) {
                do {
                        c = (char)((float)rand() / (float)RAND_MAX * (float)256);
                } while(!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')));
                str[i] = c;
        }
        str[i] = '\0';

        return;
}


/*
 * Worker
 */

void worker(void *arg) {

	/* variables */
	time_t   now        = time(NULL);
	int      throughput = 0;
	int      generated  = 0;
	
	/* message variables */
	struct Cns_fileid nsfileid = { "cns.cern.ch", 0 };

	Cuuid_t  req_id;
	Cuuid_t  subreq_id;
	int      severity;
	int      msg_no;
	int      tpnum;
	int      rv;
	char     tpname[DLF_LEN_TAPEID + 20];
	int      pari;
	U_HYPER  pari64;
	float    parf;
	double   pard;
	char     pars[DLF_LEN_STRINGVALUE + 100];
	char     parname[DLF_LEN_PARAMNAME + 10];
	char     pars2[DLF_LEN_STRINGVALUE + 100];
	char     parname2[DLF_LEN_PARAMNAME + 10];
	
	
       	while (1) {

		/* usleep doesn't provide a high enough accuracy and is highly dependant on the scheduler. 
		 * To compensate we hold internal variables discribing the throughput rate per second. If 
		 * the throughput exceeds the desired amount we sleep for a while
		 */
		if (now == time(NULL)) {
			if (throughput >= rate) {
				usleep(1000);
				continue;
			} 
		} else {
			now        = time(NULL);
			throughput = 0;
		}
			
		/* total number of messages reached ? */
		if (total != 0) {
			Cthread_mutex_lock(&test_mutex);
			if (count >= total) {
				Cthread_mutex_unlock(&test_mutex);
				break;
			}
			count++;
			generated++;
			Cthread_mutex_unlock(&test_mutex);
		}
		
		/* generate message */
		Cuuid_create(&req_id);
		Cuuid_create(&subreq_id);

		severity = 1 + (int) (11.0  * rand()/(RAND_MAX + 1.0));
		msg_no   = 1 + (int) (255.0 * rand()/(RAND_MAX + 1.0));
		
		tpnum    = 1 + (int) (9999.0 * rand()/(RAND_MAX + 1.0));
		snprintf(tpname, sizeof(tpname), "TP%04d", tpnum);

		nsfileid.fileid = ((U_HYPER)(0XF1234567) << 32) + 0X87654321;

		pari   = 32768;
		pari64 = ((U_HYPER)(0X12345678) << 32) + 0X87654321;
		parf   = 32.768332;
		pard   = 35.535;

		if (!extensive) {
			rv = dlf_write(req_id, severity, msg_no, &nsfileid, 6,
				       "String",     DLF_MSG_PARAM_STR, "This is a string!",
				       NULL,         DLF_MSG_PARAM_UUID, subreq_id,
				       "Integer 64", DLF_MSG_PARAM_INT64, pari64,
				       NULL,         DLF_MSG_PARAM_TPVID, tpname,
				       "Double",     DLF_MSG_PARAM_DOUBLE, pard,
				       "Float",      DLF_MSG_PARAM_FLOAT, parf);
		} else {
			
			/* generate string values designed to overflow the internal buffers */
			genstring(pars,     sizeof(pars) - 1);
			genstring(parname,  sizeof(parname) - 1);
			genstring(tpname,   sizeof(tpname) - 1);
			genstring(pars2,    sizeof(pars2) - 1);
			genstring(parname2, sizeof(parname2) - 1);

			rv = dlf_write(req_id, severity, msg_no, &nsfileid, 7,
				       parname,  DLF_MSG_PARAM_STR, pars,
				       parname2, DLF_MSG_PARAM_STR, pars2,
				       NULL,     DLF_MSG_PARAM_UUID, subreq_id,
				       parname,  DLF_MSG_PARAM_INT64, pari64,
				       NULL,     DLF_MSG_PARAM_TPVID, tpname,
				       parname,  DLF_MSG_PARAM_DOUBLE, pard,
				       parname,  DLF_MSG_PARAM_FLOAT, parf);
			
		}
		throughput++;

		if (rv < 0) {
			fprintf(stderr, "Thread: %d - failed to dlf_write()\n", Cthread_self());
		}
	}

	printf("Thread %d exiting - (%d messages generated)\n", Cthread_self(), generated);	

	/* exit */
	Cthread_exit(0);
}


/*
 * Main
 */

int main (int argc, char **argv) {

	/* variables */
	struct timeval start;
	struct timeval end;

	char   msgtext[DLF_LEN_MSGTEXT];
	char   error[CA_MAXLINELEN + 1];
	char   c;
	char   *facility = strdup("DLFTest");
	int    rv;
	int    i;
	int    *tid;
	int    *status;
	int    threads;
	int    daemonise = 0;

	/* initialise variables to defaults */
	threads = DEFAULT_THREAD_COUNT;
	total   = DEFAULT_MSG_TOTAL;
	gettimeofday(&start, NULL);

	/* process options */
	while ((c = getopt(argc, argv, "ht:r:n:xf:D")) != -1) {
		switch(c) {
		case 'h':
			printf("Usage: %s [OPTIONS]\n", argv[0]);
			printf("The following options are available:\n\n");
			printf("  -h               display this help and exit\n");
			printf("  -t <int>         number of producer threads to create\n");
			printf("  -r <int>         number of messages per second\n");
			printf("  -x               extensive test designed to crash the api and server\n");
			printf("  -n               number of messages to generate before exiting\n");
			printf("  -f <name>        name of the facility, default DLFTest\n");
			printf("  -D               fork the process into the background\n");
			printf("\nReport bugs to castor.support@cern.ch\n");
			return 0;
		case 't':
			threads = atoi(optarg);
			break;
		case 'r':
			rate = atoi(optarg);
			break;
		case 'x':
			extensive = 1;
			break;
		case 'n':
			total = atoi(optarg);
			break;
		case 'f':
			free(facility);
			facility = strdup(optarg);
			break;
		case 'D':
			daemonise = 1;
			break;
		case ':':
			return -1;  /* missing parameter */
		case '?':
			return -1;  /* unknown parameter */
		}
	}
		
	/* initialise interface 
	 *   - note: the facility should be registered prior to the test, self registration from a client
	 *     is not supported!!
	 */
	rv = dlf_init(facility, error);
       	if (rv != 0) {
		fprintf(stderr, "dlf_init() - %s\n", error);
		free(facility);
		return -1;
	}

	/* initialise castor thread interface */
	Cthread_init();

	/* the interface will not work correctly unless message texts have been registered to the api 
	 * prior to the first call to dlf_write() or dlf_writep()
	 */
	for (i = 1; i <= 255; i++) {
		snprintf(msgtext, DLF_LEN_MSGTEXT, "This is message text %d", i);
		rv = dlf_regtext(i, msgtext);
		if (rv < 0) {
			fprintf(stderr, "Failed to dlf_regtext() %d:%s\n", i, msgtext); 
			free(facility);
			return -1;
		}
	}

	/* allocate memory to hold thread ids */
	tid = (int *) malloc(sizeof(int) * threads);
	if (tid == NULL) {
		fprintf(stderr, "Failed to malloc thread id array : %s\n", strerror(errno));
		free(facility);
		return -1;
	}

	/* daemonise the process ? */
	if (daemonise == 1) {
		dlf_prepare();
		rv = fork();
		if (rv < 0) {
			fprintf(stderr, "dlfserver: failed to fork() : %s\n", strerror(errno));
			return 1;
		} else if (rv > 0) {
			return 0;
		}
		
		setsid();
		
		fclose(stdin);
		fclose(stdout);
		fclose(stderr);
		
		dlf_child();
	}

	/* startup message */
	printf("-------------------------------------------------------------------------------------\n");
	printf("Test started with %d thread%s generating %d message%s per second\n",  
	       threads, threads == 1 ? "" : "s", rate * threads, rate * threads == 1 ? "" : "s");
	printf("Facility name: %s\n", facility);
	printf("-------------------------------------------------------------------------------------\n");


	/* create the thread pool */
	for (i = 0; i < threads; i++) {
		tid[i] = Cthread_create((void *(*)(void *))worker, NULL);
		if (tid[i] == -1) {
			printf("Failed to Cthread_create_detached() - %s\n", strerror(errno));
		} 
	}

	/* wait for the threads to finish */
	for (i = 0; i < threads; i++) {
		Cthread_join(tid[i], &status);
	}
	free(tid);
	free(facility);

	printf("\nAll threads exited\n");
	gettimeofday(&end, NULL);

	/* shutdown the interface */
	rv = dlf_shutdown(5);
	if (rv < 0) {
		fprintf(stderr, "Failed to dlf_shutdown()\n");
		return -1;
	}

	printf("-------------------------------------------------------------------------------------\n");
	printf("Generated %d messages in %.3f seconds\n", count, 
	       (((double)end.tv_sec * 1000) + ((double)end.tv_usec / 1000) - 
		((double)start.tv_sec * 1000) + ((double)start.tv_usec / 1000)) / 1000);
		
	/* warning message */
	printf("-------------------------------------------------------------------------------------\n");
	printf("CAUTION: The dlf api is asynchronous with no acknowledgement required from the server\n");
	printf("         with regards to the messages sent. Further more each remote server has an\n");
	printf("         internal queue for managing data being sent. If this queue reaches maximum\n");
	printf("         capacity, any new messages will be dropped with NO warning from the api\n");
	printf("-------------------------------------------------------------------------------------\n");

	return 0;
}


/** End-of-File **/
