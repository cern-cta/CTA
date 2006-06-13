/******************************************************************************************************
 *                                                                                                    *
 * server.c - Castor Distribution Logging Facility                                                    *
 * Copyright (C) 2005 CERN IT/FIO (castor-dev@cern.ch)                                                *
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

/**
 * $Id
 */

/* headers */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "common.h"
#include "Cmutex.h"
#include "Cthread_api.h"
#include "dbi.h"
#include "dlf_api.h"
#include "getconfent.h"
#include "log.h"
#include "net.h"
#include "process.h"
#include "queue.h"
#include "serrno.h"
#include "server.h"


/* queues and pools */
handler_t  *hpool[MAX_THREADS];          /**< thread pool for handlers      */
queue_t    *queue;                       /**< internal server fifo queue    */

/* mutexes
 *    - used to block multiple worker threads from picking up the same connection
 */
int server_mutex;

/* server variables */
long server_mode = MODE_DEFAULT;         /**< servers mode e.g. suspended   */


/* 
 * Worker
 */

void worker(handler_t *h) {

	/* variables */
	struct sockaddr_in client_addr;
	struct timeval     tv;
	
	int       client_len;
	int       client_fd;
	int       rv;
	int       i;
	int       found;
	int       high_fd;
	int       connected;

	fd_set    rfds;
	  
	/* worker thread loop
	 *   - deals with accepting and process new requests for this thread, this is done in an endless
	 *     loop until a shutdown is requested.
	 */
	while (!IsShutdown(server_mode)) {

		/* intialise variables */
		FD_ZERO(&rfds);
		FD_SET(h->listen, &rfds);
		high_fd = h->listen;

		/* timeout required to check for server mode changes */
		tv.tv_sec  = 1;
		tv.tv_usec = 0;

		/* loop over all connected client */
		for (i = 0; i < MAX_CLIENTS; i++) {
			if (h->clients[i].fd != -1) {
				
				/* connection timed out ? */
				if ((time(NULL) - h->clients[i].timestamp) > MAX_CLIENT_TIMEOUT) {
					if (MAX_CLIENT_TIMEOUT != 0) {
						close(h->clients[i].fd);
						h->clients[i].fd = -1;

						Cthread_mutex_lock(&h->mutex);
						h->timeouts++;
						Cthread_mutex_unlock(&h->mutex);
						continue;
					}
				}
				
				/* add connected clients to fdset */
				FD_SET(h->clients[i].fd, &rfds);
				if (h->clients[i].fd > high_fd) {
					high_fd = h->clients[i].fd;
				}
			} 
		}
		
		/* wait for changes in file descriptor 
		 *   - this call blocks which is ok because if nothing has changed, nothing required
		 *     processing
		 */
		rv = select(high_fd + 1, &rfds, (fd_set *)0, (fd_set *)0, &tv);
		if (rv == APP_FAILURE) {
			log(LOG_ERR, "wofker() - failed on select() : %s\n", strerror(errno));
			sleep(1);
			continue;
		}
				
		/* read data from clients */
		for (i = 0, connected = 0; i < MAX_CLIENTS; i++) {
			if (h->clients[i].fd != -1) {
				connected++;

				/* nothing to read on this client ? */
				if (!FD_ISSET(h->clients[i].fd, &rfds)) {
					continue;
				}

				/* process the request */
				rv = process_request(&h->clients[i]);
				if (rv == APP_SUCCESS) {
					h->clients[i].timestamp = time(NULL);	
					continue;
				} 
				
				/* client disconnection 
				 *   - if a message is destined for the client to notify them of an  
				 *     problem it is the responsibility of the parsing functions to do
				 *     this.
				 *   - it is also the responsibility of the parser to ack the message
				 *   - all errors result in a disconnect
				 */
				if (h->clients[i].fd != -1) {
					netclose(h->clients[i].fd);
					h->clients[i].fd = -1;
				}

				Cthread_mutex_lock(&h->mutex);
				h->errors++;
				connected--;
				Cthread_mutex_unlock(&h->mutex);
			}
		}

		/* max clients reached ? 
		 *   - this thread has become saturated with connections allow another to pick up new
		 *     connections. 
		 */
		if (connected == (MAX_CLIENTS - 1)) {
			sleep(1);
			continue;
		}
		h->connected = connected;

		/* set max simultaneous clients */
		Cthread_mutex_lock(&h->mutex);

		if (h->connected > h->clientpeak) {
			h->clientpeak = h->connected;
		}

		Cthread_mutex_unlock(&h->mutex);

		/* new clients awaiting connection */
		if ((FD_ISSET(h->listen, &rfds)) && (!IsShutdown(server_mode))) {

			/* load balancing enabled ? 
			 *   - determine if another handler thread is 5% less saturated then the current
			 *     one. This isn't done in a thread-safe way as mutex locking another 
			 *     threads data is nasty. The idea here is not perfect balancing but just to
			 *     stop some threads doing all the work while others do nothing
			 */
			if (HANDLER_THREAD_BALANCING == 1) {
				for (i = 0, rv = 1; hpool[i] != NULL; i++) {
					if (hpool[i]->index == h->index) {
						continue;                 /* ignore current thread  */
					}
					if (hpool[i]->connected == h->connected) {
						continue;                 /* same number of clients */
					}
					if (hpool[i]->connected < (h->connected-(100/MAX_CLIENTS)*5)) {
						if (h->connected > (100 / MAX_CLIENTS) * 10) {
							rv = 0; 
							break;
						}
					}
				}
				
				/* another thread should pick up the connection ? */ 
				if (rv == 0) {
					sleep(1);
					continue;
				}
			}

			/* initialise */
			client_len = sizeof(client_addr);

			/* accept connection, a mutex is used here to prevent multiple threads assigning
			 * the new connection to there client tables.
			 */			
			Cthread_mutex_lock(&server_mutex);
			client_fd = accept(h->listen, (struct sockaddr *)&client_addr, &client_len);
			Cthread_mutex_unlock(&server_mutex);

			/* error in accept ?, try again */
			if (client_fd == -1) {
				continue;
			}

			/* find an available client slot */
			for (i = 0, found = 0; i < MAX_CLIENTS; i++) {
				if (h->clients[i].fd == -1) {
					found = 1;
					break;
				}
			}

			/* too many connections on this thread */
			if (found == 0) {
				netclose(client_fd);
				continue;
			}
			
			/* initialise client variables 
			 *   - we already checked previously to see if the thread was fully loaded so
			 *     no need to do it again.
			 */
			strncpy(h->clients[i].ip, inet_ntoa(client_addr.sin_addr), sizeof(h->clients[i].ip) - 1);
			h->clients[i].fd        = client_fd;
			h->clients[i].timestamp = time(NULL);
		        h->clients[i].handler   = h;

			log(LOG_DEBUG, "connection accepted %s : %d\n", h->clients[i].ip, i);
			
			Cthread_mutex_lock(&h->mutex);
			h->connections++;
			Cthread_mutex_unlock(&h->mutex);
		}	
	}
	
	/* disconnect connected clients */
	for (i = 0; i < MAX_CLIENTS; i++) {
		if (h->clients[i].fd != -1) {
			close(h->clients[i].fd);
			h->clients[i].fd = -1;
		}
	}
	
	/* exit */
	Cthread_exit(0);
}


/*
 * Signal handler
 */

void signal_handler(void *arg) {

	/* variables */
	sigset_t set; 
	int      signal;

	/* catch all signal */
	sigfillset(&set);

	/* loop */
	while (1) {

		/* wait for a signal to arrive */
		if (sigwait(&set, &signal) != APP_SUCCESS) {
			continue;
		}

		/* process signal */
		switch(signal) {
		case SIGINT: {
			log(LOG_NOTICE, "Caught SIGINT\n");
			exit(APP_FAILURE);
		}
		case SIGTERM: {
			log(LOG_NOTICE, "Caught SIGTERM - shutting down\n");
			SetShutdown(server_mode);
			break;
		}
		case SIGUSR1: {

			/* server already suspended ? */
			if (IsSuspended(server_mode)) {
				break;
			}

			log(LOG_NOTICE, "Caught SIGUSR1 - suspending database insertions\n");
			SetSuspended(server_mode);
			break;
		}
		case SIGUSR2: {

			/* after SIGTERM a SIGCONT is received
			 *    - no point resuming if entering a shutdown request
			 */
			if (IsShutdown(server_mode)) {
				break;
			}
			if (!IsSuspended(server_mode)) {
				break;                    /* server already suspended */
			}

			log(LOG_NOTICE, "Caught SIGUSR2 - resuming database insertions\n");	
			ClrSuspended(server_mode);
			break;
		}
		case SIGCONT: break;
		case SIGSTOP: break;
		default: {
			log(LOG_NOTICE, "Caught signal %d - ignored\n", signal);
			break;
		}
		}
	}

	/* exit */
	Cthread_exit(0);
}


/*
 * Main
 */

int main(int argc, char **argv) {

	/* variables */
	struct sockaddr_in server_addr;
	struct servent     *servent;

	handler_t  *h; 
	int        i;
	int        j; 
	int        c;
	int        rv;     
	int        server_fd; 
	int        opts;  
	sigset_t   set;
	int        sid; 

	/* options */
	int        h_threads;  
	int        d_threads;
	int        port;             
	int        debug      = 0;  
	int        foreground = 0;      
	char       *logfile   = NULL; 
	char       *value;   

	/* initialise variables */
	h_threads = HANDLER_THREAD_POOLSIZE;
	d_threads = DB_THREAD_POOLSIZE;
	port      = DEFAULT_SERVER_PORT;
  	logfile   = strdup(LOGFILE);

	/* initialise castor thread interface */
	Cthread_init();

	/* port defined via config file, environment variable or /etc/services ? */
	if ((value = getenv("DLF_PORT")) || (value = getconfent("DLF", "PORT", 0))) {
		port = atoi(value);
	} else if ((servent = getservbyname("dlf", "tcp"))) {
		port = servent->s_port;
	}


	/* process options */
	while ((c = getopt(argc, argv, "hdfl:t:p:")) != -1) {
		switch(c) {
		case 'd':
			debug = 1;
			break;
		case 'f':
			foreground = 1;
			break;
		case 'l':
			free(logfile);
			logfile = strdup(optarg);
			break;

		/* the thread option must be defined as a unsigned integer couplet <num>:<num>. The 
		 * numbers refer to the number of handler and database threads respectively
		 */
		case 't':
			if (sscanf(optarg, "%d:%d", &h_threads, &d_threads) != 2) {
				fprintf(stderr, "dlfserver: invalid option argument '%s' -t (couplet expected)\n", optarg); 
				return APP_FAILURE;
			}
			if (h_threads > MAX_THREADS) {
                                fprintf(stderr, "dlfserver: invalid handler thread value (number exceeds %d)\n", MAX_THREADS);
				return APP_FAILURE;
			}
			if (h_threads < 0) {
                                fprintf(stderr, "dlfserver: invalid handler thread value (negative number not permitted)\n");
				return APP_FAILURE;
			}
                        if (d_threads > MAX_THREADS) {
                                fprintf(stderr, "dlfserver: invalid database thread value (number exceeds %d)\n", MAX_THREADS);
                                return APP_FAILURE;
                        }
                        if (d_threads < 0) {
                                fprintf(stderr, "dlfserver: invalid database thread value (negative number not permitted)\n");
                                return APP_FAILURE;
                        }
			break;
		case 'h': 
			printf("Usage: %s [OPTIONS]\n", argv[0]);
			printf("The following options are available:\n\n");
			printf("  -d               enable debugging mode\n");
			printf("  -f               keep process in the foreground, do not fork\n");
			printf("  -h               display this help and exit\n");
			printf("  -l <path>        path to log file\n");
			printf("  -p <int>         servers listening port (default: %d)\n", port);
			printf("  -t <int>:<int>   the number of transport threads in relation to database threads\n");
			printf("\nReport bugs to castor.support@cern.ch\n");
			return APP_SUCCESS;
		case 'p':
			port = atoi(optarg);
			break;
		case ':':
			return APP_FAILURE;   /* missing parameter */
		case '?':
			return APP_FAILURE;   /* unknown parameter */
		}
	}

	/* background process ? */
	if (foreground == 0) {

		/* fork process */
		rv = fork();
		if (rv < APP_SUCCESS) {
			fprintf(stderr, "dlfserver: failed to fork() : %s\n", strerror(errno));
			return APP_FAILURE;
		}

		/* new process group */
		setsid();

		/* close default file handles */
		fclose(stdin);
		fclose(stdout);
		fclose(stderr);
	}

	/* initialise logging */
	if (debug == 1) {
		initlog("dlfserver", LOG_DEBUG, logfile != NULL ? logfile : "stdout");
	} else {
		initlog("dlfserver", LOG_INFO, logfile != NULL ? logfile : "stdout");
	}

	log(LOG_INFO, "DLF server started - pid %d\n", getpid());

	/* initialise socket layer
	 *   - here we create a socket, bind it and listen for incoming connections on the defined port
	 *     we do not however accept any connections from with main(). This is the job of the worker
	 *     threads to block and wait for incoming connections. 
	 *   - one worker will deal with one and only one connection from the duration of the 
	 *     connections lifecycle
	 */
	server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd < 0) {
		log(LOG_CRIT, "Failed to create socket() - %s\n", strerror(errno));
		return APP_FAILURE;
	}
	memset (&server_addr, 0, sizeof(server_addr));
	
	/* define server and port */
	server_addr.sin_family      = AF_INET;
	server_addr.sin_addr.s_addr = INADDR_ANY;
	server_addr.sin_port        = htons(port);

	/* set socket options 
	 *   - allow the server to bind to an address which is in a TIME_WAIT state   
	 *   - keep the connection alive 
	 */
	opts = 1;
	rv = setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opts, sizeof(opts));
	if (rv != APP_SUCCESS) {
		log(LOG_CRIT, "Failed to setsockopt(SO_REUSEADDR) on socket - %s\n", strerror(errno));
		return APP_FAILURE;
	}	
	opts = 1;
	rv = setsockopt(server_fd, SOL_SOCKET, SO_KEEPALIVE, &opts, sizeof(opts));
	if (rv != APP_SUCCESS) {
		log(LOG_CRIT, "Failed to setsockopt(SO_KEEPALIVE) on socket - %s\n", strerror(errno));
		return APP_FAILURE;
	}

	/* bind and listen */
	rv = bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
	if (rv != APP_SUCCESS) {
		log(LOG_CRIT, "Failed to bind() - %s\n", strerror(errno));
		return APP_FAILURE;
	}

	rv = listen(server_fd, DEFAULT_SERVER_BACKLOG);
	if (rv != APP_SUCCESS) {
		log(LOG_CRIT, "Failed to listen() on port %d with backlog %d\n",  port, DEFAULT_SERVER_BACKLOG);
		return APP_FAILURE;
	}

	/* setup signal handling
	 *   - this implementation causes all newly create threads to ignore all signals. A dedicate
	 *     thread is then responsible for dealing with catching nondirected signals using sigwait
	 *   - the benefit of this is that we don't have to deal with signals within any other thread
	 */
	sigfillset(&set);
	sigprocmask(SIG_SETMASK, &set, NULL);

	sid = Cthread_create_detached((void *)signal_handler, NULL);
	if (sid == APP_FAILURE) {
		log(LOG_CRIT, "Failed to Cthread_create_detached() signal thread - %s\n", sstrerror(serrno));
		return APP_FAILURE;
	}

	/* create bounded fifo queue
	 *   - all worker threads will write to this queue which will hold data for db insertion
	 */
	rv = queue_create(&queue, MAX_QUEUE_SIZE);
	if (rv != APP_SUCCESS) {
		log(LOG_CRIT, "Failed to queue_create() interval server queue - %s\n", strerror(rv));
		return APP_FAILURE;
	}

	/* flush handler thread table */
	for (i = 0; i < MAX_THREADS; i++) {
		hpool[i] = NULL;
	}

	/* create thread pool */
	for (i = 0; i < h_threads; i++) {
		
		/* create structure */
		h = (handler_t *) malloc(sizeof(handler_t));
		if (h == NULL) {
			log(LOG_CRIT, "malloc() failed for handler structure - %s\n", strerror(errno));
			return APP_FAILURE;
		} 

		/* initialise handler_t structure */
		h->index       = i;
		h->listen      = server_fd;
		h->connections = 0;		
		h->clientpeak  = 0;
		h->messages    = 0;
		h->errors      = 0;
		h->timeouts    = 0;

		/* create client structure */
		h->clients = (client_t *) malloc(sizeof(client_t) * 150);
		if (h->clients == NULL) {
			log(LOG_CRIT, "malloc() failed for handler client structure - %s\n", strerror(errno));
			return APP_FAILURE;
		}
		
		/* set main client attributes */
		for (j = 0; j < MAX_CLIENTS; j++) {
			h->clients[j].fd    = -1;
			h->clients[j].index = j;
		}
	
		/* create thread */
		h->tid = Cthread_create_detached((void *)worker, (handler_t *)h);
		if (h->tid == APP_FAILURE) {
			log(LOG_CRIT, "Cthread_create_detached() failed for handler thread %d - %s\n", i, strerror(errno));
			return APP_FAILURE;
		}

		/* assign thread to handler pool */
		hpool[i] = h;
	}
	
	rv = db_init(d_threads);
	if (rv != APP_SUCCESS) {
		log(LOG_CRIT, "Failed to initialise database interface layer - shutting down\n");
		SetShutdown(server_mode);
	} 

	sleep(2);
	
	/* log information */
	if (!IsShutdown(server_mode)) {
		log(LOG_INFO, "Initialisation complete\n");
		log(LOG_INFO, "Server listening on port %d - maximum concurrent connections %d\n", port, h_threads * MAX_CLIENTS);
		log(LOG_INFO, "Internal server message queue %d messages\n", MAX_QUEUE_SIZE);
		log(LOG_INFO, "Resuming normal operation\n");
	}

	/* main loop */
	while (!IsShutdown(server_mode)) {
		db_monitoring(hpool, 300);
		db_stats(300);
		sleep(1);
	}

	log(LOG_NOTICE, "Initiating server shutdown\n");

	/* destroy handler thread pool */
	for (i = 0; i < MAX_THREADS; i++) {
		if (hpool[i] != NULL) {
			free(hpool[i]->clients);
			free(hpool[i]);
		}
	}

	close(server_fd);

	/* shutting down */
	log(LOG_NOTICE, "Deleted %d messages from the internal server queue\n", queue_size(queue));
	rv = queue_destroy(queue, (void *)free_message);
	if (rv != APP_SUCCESS) {
		log(LOG_ERR, "Failed to queue_destroy() - %s\n", strerror(rv));
	}

	db_shutdown();

	log(LOG_INFO, "DLF server stopped\n");

	/* free resources */
	if (logfile != NULL) {
		free(logfile);
	}

	free(queue);

      	return APP_SUCCESS;
}


/** End-of-File **/
