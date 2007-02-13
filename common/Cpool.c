/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* ------------------------------------ */
/* For the what command                 */
/* ------------------------------------ */
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cpool.c,v $ $Revision: 1.39 $ $Date: 2007/02/13 07:52:24 $ CERN IT-ADC-CA/HSM Jean-Damien Durand";
#endif /* not lint */

#include <Cpool_api.h>
/* We do these #undef because we want to create entry points */
/* for old application not recompiled using shared library */
/* and the #define's would cause is some problem... */
#undef Cpool_create
#undef Cpool_assign
#undef Cpool_next_index
#undef Cpool_next_index_timeout
#include <serrno.h>
#ifndef _WIN32
/* All that stuff is for CTHREAD_MULTI_PROCESS support on */
/* Unix-like systems.                                     */
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#endif /* _WIN32 */
#ifdef _AIX
/* Otherwise cc will not know about fd_set on */
/* old aix versions.                          */
#include <sys/select.h>
#endif
#ifdef DEBUG
#ifndef CPOOL_DEBUG
#define CPOOL_DEBUG
#endif
#endif /* DEBUG */

#ifdef CPOOL_DEBUG
#include <log.h>
#endif

#include <osdep.h>

int Cpool_debug = 0;

#ifndef _WIN32
#ifndef _CTHREAD
/* ------------------------------------ */
/* Undefinition of memory wrappers      */
/* ------------------------------------ */
/* We don't want to recursively call   */
/* ourselves...                        */
#undef calloc
#undef malloc
#undef realloc
#undef free
#endif 
#endif /* _WIN32 */

/* ------------------------------------ */
/* Linked list of pools                 */
/* ------------------------------------ */
struct Cpool_t {
	int                     lock_parent;
	int                     poolnb;   /* Pool number                        */
	int                     nbelem;   /* Nb of elems                        */
	int                    *cid;      /* Elements Cthread ID                */
	int                     forceid;  /* Index forcing the assignment to    */
#ifndef _WIN32
	/* If CTHREAD_MULTI_PROCESS */
	int                    *writefd;  /* Parent->Child (only on unix)       */
	int                    *readfd;   /* Child->Parent (only on unix)       */
#endif
	/* If CTHREAD_TRUE_THREAD */
	int                    *state;    /* Elements current status (0=READY)  */
	/*                        (1=RUNNING) */
	/*                       (-1=STARTED) */
	
	int                     flag;     /* Parent flag (-1=WAITING)           */
	void                  *(**start) _PROTO((void *)); /* Start routines             */
	void                  **arg;
	
	void                   *lock_parent_cthread_structure;
	void                   **state_cthread_structure;

	struct Cpool_t         *next;     /* Next pool                          */
};

#ifndef _WIN32
static struct Cpool_t Cpool = { -1, 0, 0, NULL, -1, NULL, NULL, NULL, 0, NULL, NULL, NULL};
#else
static struct Cpool_t Cpool = { -1, 0, 0, NULL, -1, NULL, 0, NULL, NULL, NULL};
#endif

#ifndef _WIN32
/* ------------------------------------ */
/* Linked list of memory allocation     */
/* ------------------------------------ */
struct Cmalloc_t {
	void *start;
	void *end;
	struct Cmalloc_t *next;
};
static struct Cmalloc_t Cmalloc = { NULL, NULL, NULL};
#endif /* _WIN32 */

#ifndef _WIN32
/* ------------------------------------ */
/* Non-thread environment pipe protocol */
/* ------------------------------------ */
static int  tubes[5];
#endif /* _WIN32 */

#ifndef _WIN32
/* ------------------------------------ */
/* Typedefs                             */
/* ------------------------------------ */
typedef void    Sigfunc _PROTO((int));
#endif /* _WIN32 */

#ifdef hpux
/* hpux wants int instead of fd_set */
typedef int _cpool_fd_set;
/* typedef fd_set _cpool_fd_set; */
#else
typedef fd_set _cpool_fd_set;
#endif /* hpux */

/* ------------------------------------ */
/* Prototypes                           */
/* ------------------------------------ */
void   DLL_DECL  *_Cpool_starter _PROTO((void *));
#ifndef _WIN32
size_t   _Cpool_writen _PROTO((int, void *, size_t));
size_t   _Cpool_readn _PROTO((int, void *, size_t));
#ifdef CPOOL_DEBUG
size_t   _Cpool_writen_timeout _PROTO((char *, int, int, void *, size_t, int));
size_t   _Cpool_readn_timeout _PROTO((char *, int, int, void *, size_t, int));
#else
size_t   _Cpool_writen_timeout _PROTO((int, void *, size_t, int));
size_t   _Cpool_readn_timeout _PROTO((int, void *, size_t, int));
#endif
void     _Cpool_alarm _PROTO((int));
Sigfunc *_Cpool_signal _PROTO((int, Sigfunc *));
#endif /* _WIN32 */
int    DLL_DECL  _Cpool_self();

#ifndef _WIN32
/* ------------------------------------ */
/* Constants used in the fork() model   */
/* ------------------------------------ */
#ifndef _CPOOL_STARTER_TIMEOUT
#define _CPOOL_STARTER_TIMEOUT 10
#endif
#ifndef _CPOOL_SLEEP_FLAG
#define _CPOOL_SLEEP_FLAG       -1
#endif
static void *_cpool_sleep_flag = (void *) _CPOOL_SLEEP_FLAG;
#endif /* _WIN32 */

/* ============================================ */
/* Routine  : Cpool_create                      */
/* Arguments: Number of requested processes     */
/*            &Number of created processses     */
/* -------------------------------------------- */
/* Output   : >= 0 (PoolNumber) -1 (ERROR)      */
/* -------------------------------------------- */
/* History:                                     */
/* 17-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cpool_create(nbreq,nbget)
	int nbreq;
	int *nbget;
{
	return(Cpool_create_ext(nbreq,nbget,NULL));
}

/* ============================================ */
/* Routine  : Cpool_create_ext                  */
/* Arguments: Number of requested processes     */
/*            &Number of created processses     */
/*            &Cpool_structure                  */
/* -------------------------------------------- */
/* Output   : >= 0 (PoolNumber) -1 (ERROR)      */
/* -------------------------------------------- */
/* History:                                     */
/* 17-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* 04-SEP-2003       Added &Cpool_structure     */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cpool_create_ext(nbreq,nbget,pooladdr)
	int nbreq;
	int *nbget;
	void **pooladdr;
{
	struct Cpool_t         *current          = &Cpool;
	struct Cpool_t         *previous         = &Cpool;
	int                     i;
	int                     j;
	int                     k;
	int                     poolnb, nbcreated;
#ifndef _WIN32
	/* If CTHREAD_MULTI_PROCESS */
	int                     p_to_c[2];
	int                     c_to_p[2];
	int                    *to_close = NULL;
	int                     pid = 0;
#endif
	/* If CTHREAD_TRUE_THREAD */
	void                   *cpool_arg = NULL;

	/* We makes sure that Cthread pakage is initalized */
	Cthread_init();
  
#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create(%d,0x%lx)\n",
			_Cpool_self(),_Cthread_self(),nbreq,(unsigned long) nbget);
#endif

	if (nbreq <= 0) {
		serrno = EINVAL;
		return(-1);
	}

#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : lock on &Cpool\n",
			_Cpool_self(),_Cthread_self());
#endif

	/* We search the last available pool */
	if (Cthread_mutex_lock(&Cpool) != 0) {
		return(-1);
	}
	previous = &Cpool;
	if (current->next != NULL) {
		while (current->next != NULL) {
			previous = current;
			current  = current->next;
		}
		previous = current;
		poolnb = current->poolnb;
	} else {
		/* Make sure that ++poolnb will return a number >= 0 */
		poolnb = -1;
	}

#ifndef _WIN32
	if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
		/* To let know the child which parent is the creator */
		/* and to die in case of non-parent existence        */
		/* (on unix only, this is to prevent zombies)        */
		pid = getpid();
    
		/* We further more don't want to die in case of SIGCHLD */
		/* or SIGALRM (which is our private signal for timeout) */
		/* 
		   _Cpool_signal(SIGCHLD, SIG_IGN);
		   _Cpool_signal(SIGPIPE, SIG_IGN);
		   _Cpool_signal(SIGALRM, SIG_IGN);
		*/

		/* We prepare the list of fd's to close */
		if ((to_close = malloc(2 * nbreq * sizeof(int))) == NULL) {
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
	}
#endif
  
	/* We create a new pool element */
	if ((current = malloc(sizeof(struct Cpool_t))) == NULL) {
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
				_Cpool_self(),_Cthread_self());
#endif
		Cthread_mutex_unlock(&Cpool);
		serrno = SEINTERNAL;
		return(-1);
	}

	/* Allocation for Cthread ID's */
	if ((current->cid = malloc(nbreq * sizeof(int))) == NULL) {
#ifndef _WIN32
		if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
			free(to_close);
		}
#endif
		free(current);
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
				_Cpool_self(),_Cthread_self());
#endif
		Cthread_mutex_unlock(&Cpool);
		serrno = SEINTERNAL;
		return(-1);
	}
#ifndef _WIN32
	if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
		/* Allocation for writing pipes (unix only) */
		if ((current->writefd = malloc(nbreq * sizeof(int))) == NULL) {
			free(to_close);
			free(current->cid);
			free(current);
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
		/* Allocation for reading pipes (unix only) */
		if ((current->readfd = malloc(nbreq * sizeof(int))) == NULL) {
			free(to_close);
			free(current->writefd);
			free(current->cid);
			free(current);
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
	} else {
#endif /* _WIN32 */
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : lock on &(current->lock_parent)\n",
				_Cpool_self(),_Cthread_self());
#endif
		if (Cthread_mutex_lock(&(current->lock_parent)) != 0) {
			free(current->cid);
			free(current);
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
		if ((current->lock_parent_cthread_structure = Cthread_mutex_lock_addr(&(current->lock_parent))) == NULL) {
			free(current->cid);
			free(current);
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on current->lock_parent_cthread_structure\n",
				_Cpool_self(),_Cthread_self());
#endif
		Cthread_mutex_unlock_ext(current->lock_parent_cthread_structure);
		/* Allocation for threads status */
		if ((current->state = malloc(nbreq * sizeof(int))) == NULL) {
			free(current->cid);
			free(current);
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
		if ((current->state_cthread_structure = (void **) malloc(nbreq * sizeof(void *))) == NULL) {
			free(current->state);
			free(current->cid);
			free(current);
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
		{
			int icthread;
			for (icthread = 0; icthread < nbreq; icthread++) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : lock on &(current->state[%d]\n",
						_Cpool_self(),_Cthread_self(), icthread);
#endif
				if (Cthread_mutex_lock(&(current->state[icthread])) != 0) {
					free(current->state_cthread_structure);
					free(current->state);
					free(current->cid);
					free(current);
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0)
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
							_Cpool_self(),_Cthread_self());
#endif
					Cthread_mutex_unlock(&Cpool);
					serrno = SEINTERNAL;
					return(-1);
				}
				if ((current->state_cthread_structure[icthread] = Cthread_mutex_lock_addr(&(current->state[icthread]))) == NULL) {
					free(current->state_cthread_structure);
					free(current->state);
					free(current->cid);
					free(current);
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0)
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
							_Cpool_self(),_Cthread_self());
#endif
					Cthread_mutex_unlock(&Cpool);
					serrno = SEINTERNAL;
					return(-1);
				}
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on current->state_cthread_structure[%d]\n",
						_Cpool_self(),_Cthread_self(), icthread);
#endif
				Cthread_mutex_unlock_ext(current->state_cthread_structure[icthread]);
			}
		}
		/* Allocation for threads starting routines */
		if ((current->start = malloc(nbreq * sizeof(void *))) == NULL) {
			free(current->state_cthread_structure);
			free(current->state);
			free(current->cid);
			free(current);
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
		/* Allocation for threads arguments addresses */
		if ((current->arg = malloc(nbreq * sizeof(void *))) == NULL) {
			free(current->state_cthread_structure);
			free(current->start);
			free(current->state);
			free(current->cid);
			free(current);
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
			serrno = SEINTERNAL;
			return(-1);
		}
#ifndef _WIN32
	}
#endif
	current->next = NULL;
	if (Cthread_environment() != CTHREAD_MULTI_PROCESS) {
		/* We tell that there is no dispatch at this time */
		current->flag = -2;
	}
    
	nbcreated = j = k = 0;
	/* We create the pools */
	for (i = 0; i < nbreq; i++) {
#ifndef _WIN32
		if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
			/* The pipes */
			if (pipe(p_to_c))
				continue;
			if (pipe(c_to_p)) {
				close(p_to_c[0]);
				close(p_to_c[1]);
				continue;
			}
			/* We prepare the argument */
			tubes[0] = p_to_c[0]; /* For reading */
			tubes[1] = p_to_c[1]; /* For writing */
			tubes[2] = c_to_p[0]; /* For reading */
			tubes[3] = c_to_p[1]; /* For writing */
			tubes[4] = pid;
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : pipes give [%d,%d,%d,%d]\n",
					_Cpool_self(),_Cthread_self(),tubes[0],tubes[1],tubes[2],tubes[3]);
#endif
			/* Non thread environment : everything will be done with pipes */
			current->cid[i] = Cthread_create(_Cpool_starter,NULL);
		} else {
#endif /* _WIN32 */
			/* Thread environment : everything will be done with shared mem. and cond. vars. */
			/* We send as argument the address of the current pool structure as well as the  */
			/* index of this thread in this structure                                        */
			if ((cpool_arg = malloc(sizeof(struct Cpool_t *) + sizeof(int))) != NULL) {
				char *dummy = cpool_arg;
				memcpy(dummy,&current,sizeof(struct Cpool_t *));
				dummy += sizeof(struct Cpool_t *);
				memcpy(dummy,&i,sizeof(int));
				/* And we don't forget to initialize the state of this thread to be STARTED... */
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : lock on current->state_cthread_structure[%d]\n",
						_Cpool_self(),_Cthread_self(), i);
#endif
				Cthread_mutex_lock_ext(current->state_cthread_structure[i]);
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : Setting current->state_cthread_structure[%d] to 1\n",
						_Cpool_self(),_Cthread_self(), i);
#endif
				current->state[i] = 1;
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : Cthread_create\n",
						_Cpool_self(),_Cthread_self());
#endif
				if ((current->cid[i] = Cthread_create(_Cpool_starter,cpool_arg)) < 0) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0)
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on current->state_cthread_structure[%d]\n",
							_Cpool_self(),_Cthread_self(), i);
#endif
					Cthread_mutex_unlock_ext(current->state_cthread_structure[i]);
					free(cpool_arg);
				} else {
					/* And we wait for this thread to have really started */
					while (current->state[i] != 0) {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0)
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : cond-wait on current->state_cthread_structure[%d] until it is == 0\n",
								_Cpool_self(),_Cthread_self(), i);
#endif
						Cthread_cond_wait_ext(current->state_cthread_structure[i]);
					}
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0)
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on current->state_cthread_structure[%d]\n",
							_Cpool_self(),_Cthread_self(), i);
#endif
					Cthread_mutex_unlock_ext(current->state_cthread_structure[i]);
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0)
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : Thread No %d started\n",
							_Cpool_self(),_Cthread_self(),i);
#endif
				}
			} else {
				serrno = SEINTERNAL;
				current->cid[i] = -1;
			}
#ifndef _WIN32
		}
#endif /* _WIN32 */
		if (current->cid[i] < 0) {
			/* Error at cthread_create : we clean fd created an try the */
			/* next iteration                                           */
#ifndef _WIN32
			if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
				for (j=0; j <= 3; j++) {
					close(tubes[j]);
				}
			}
#endif /* _WIN32 */
		} else {
#ifndef _WIN32
			if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
				current->writefd[i] = p_to_c[1];
				current->readfd[i] = c_to_p[0];
				to_close[k++] = p_to_c[0];
				to_close[k++] = c_to_p[1];
			}
#endif /* _WIN32 */
			/* We count number of created processes */
			++nbcreated;
		}
	}

#ifndef _WIN32
	if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
		for (j=0; j < k; j++) {
			close(to_close[j]);
		}
		free(to_close);
	}
#endif /* _WIN32 */

	/* We update the return value */
	if (nbget != NULL)
		*nbget = nbcreated;

	/* We initialize the element structure */
	current->poolnb = ++poolnb;                  /* Pool number */
	current->nbelem = nbcreated;                 /* Number of threads in it */
	current->forceid = -1;                       /* Force thread index assignment */
	previous->next = current;                    /* Next pool */
	if (pooladdr != NULL) {
		*pooladdr = current;
	}
#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_create : un-lock on &Cpool\n",
			_Cpool_self(),_Cthread_self(), i);
#endif
	Cthread_mutex_unlock(&Cpool);
  
	return(current->poolnb);
}

/* ============================================ */
/* Routine  : _Cpool_starter                    */
/* Arguments: address of arguments              */
/*            (used only within threads)        */
/* -------------------------------------------- */
/* Output   : dummy (not used)                  */
/* -------------------------------------------- */
/* History:                                     */
/* 14-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Nota: the prototype of this function is like */
/* this just to agree with the prototype of     */
/* Cthread_create and al.                       */
/* ============================================ */
void *_Cpool_starter(arg)
	void *arg;
{
#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter(0x%lx)\n",
			_Cpool_self(),_Cthread_self(),(unsigned long) arg);
#endif

#ifndef _WIN32
	if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
		/*----------------------- */
		/* Non-Thread only        */
		/*----------------------- */
		int         p_to_c[2];
		int         c_to_p[2];
		int         ppid;
		void       *thisarg;
		int         ready = 1;
		size_t      thislength;
		void     *(*routine) _PROTO((void *));
		int         sleep_flag;
    
		/* We get the argument */
		p_to_c[0] = tubes[0];
		p_to_c[1] = tubes[1];
		c_to_p[0] = tubes[2];
		c_to_p[1] = tubes[3];
		ppid      = tubes[4];
    
		/* We close unnecessary fd's */
    
		close(p_to_c[1]);
		close(c_to_p[0]);
    
		while (1) {
			/* We send a flag in c_to_p[1] to say that we are ready */
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : Write ready flag with timeout\n",
					_Cpool_self(),_Cthread_self());
#endif
		  _cpool_multi_process_again:
			sleep_flag = 0;
			while (1) {
				if (_Cpool_writen_timeout(
#ifdef CPOOL_DEBUG
						__FILE__,__LINE__,
#endif
						c_to_p[1],&ready,sizeof(int),_CPOOL_STARTER_TIMEOUT) != 
					sizeof(int)) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : errno No %d (%s)\n",
							_Cpool_self(),_Cthread_self(),errno,strerror(errno));
						log(LOG_INFO,"[Cpool  [%2d][%2d]] . bis repetita .  : serrno No %d (%s)\n",
							_Cpool_self(),_Cthread_self(),errno,strerror(errno));
					}
#endif
					if (serrno == SETIMEDOUT) {
						/* Timeout */
						/* We check that the parent is still there */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0)
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : kill(%d,0)\n",
								_Cpool_self(),_Cthread_self(),ppid);
#endif
						if (kill(ppid,0) != 0) {
							/* Nope... */
							return(NULL);
						}
						continue;
					} else {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0)
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : serrno (%d) != SETIMEDOUT (%d). End of this process.\n",
								_Cpool_self(),_Cthread_self(),serrno,SETIMEDOUT);
#endif
						/* Error */
						return(NULL);
					}
				} else {
					break;
				}
			}
			/* And now we wait for something in the read tube */
			/* We read the address of the start routine, unless it is _CPOOL_SLEEP_FLAG */
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : Read startroutine address with timeout on fd = %d\n",
					_Cpool_self(),_Cthread_self(),p_to_c[0]);
#endif
			while (1) {
				if (_Cpool_readn_timeout(
#ifdef CPOOL_DEBUG
						__FILE__,__LINE__,
#endif
						p_to_c[0],&routine,sizeof(void *),_CPOOL_STARTER_TIMEOUT) !=
					sizeof(void *)) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : errno No %d (%s)\n",
							_Cpool_self(),_Cthread_self(),errno,strerror(errno));
						log(LOG_INFO,"[Cpool  [%2d][%2d]] . bis repetita .  : serrno No %d (%s)\n",
							_Cpool_self(),_Cthread_self(),errno,strerror(errno));
					}
#endif
					if (serrno == SETIMEDOUT) {
						/* Timeout */
						/* We check that the parent is still there */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0)
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : kill(%d,0)\n",_Cpool_self(),_Cthread_self(),ppid);
#endif
						if (kill(ppid,0) != 0) {
							/* Nope... */
							return(NULL);
						}
						continue;
					} else {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0)
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : serrno (%d) != SETIMEDOUT (%d). End of this process.\n",
								_Cpool_self(),_Cthread_self(),serrno,SETIMEDOUT);
#endif
						/* Error */
						return(NULL);
					}
				} else {
					if (routine == (void *(*) _PROTO((void *))) _CPOOL_SLEEP_FLAG) {
						/* We just had a hit from Cpool_next_index, that sent us the sleep flag */
						sleep_flag = 1;
					}
					break;
				}
			}

			if (sleep_flag == 1) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] -> Sleep flag received. Looping again\n",_Cpool_self(),_Cthread_self());
#endif
				goto _cpool_multi_process_again;
			}

#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] -> startroutine is at 0x%lx\n",_Cpool_self(),_Cthread_self(),(unsigned long) routine);
#endif
			/* In a non-thread environment we read the argument length and content */
			if (_Cpool_readn_timeout(
#ifdef CPOOL_DEBUG
					__FILE__,__LINE__,
#endif
					p_to_c[0],&thislength,sizeof(size_t),_CPOOL_STARTER_TIMEOUT) != 
				sizeof(size_t)) 
				return(NULL);
			if (thislength > 0) {
				if ((thisarg = malloc(thislength)) == NULL)
					exit(EXIT_FAILURE);
				if (_Cpool_readn_timeout(
#ifdef CPOOL_DEBUG
						__FILE__,__LINE__,
#endif
						p_to_c[0],thisarg,thislength,_CPOOL_STARTER_TIMEOUT) != 
					thislength)
					return(NULL);
			} else {
				thisarg = NULL;
			}
			/* We execute the routine */
			routine(thisarg);
			/* We free memory if needed */
			if (thisarg != NULL)
				free(thisarg);
		}
	} else {
#endif /* _WIN32 */
		{
			/*----------------------- */
			/* Thread only			  */
			/*----------------------- */
			struct Cpool_t *current;
			int 			index;
			char		   *dummy;
			void		  *(*start) _PROTO((void *));
			void		   *startarg;
			void *lock_parent_cthread_structure;
			/* We receive in the argument the address of the pool structure */
			/* And the index of our thread in this structure                */
			current = (struct Cpool_t *) * (struct Cpool_t **) arg;
			dummy = arg;
			dummy  += sizeof(struct Cpool_t *);
			index   = (int) * (int *) dummy;
			lock_parent_cthread_structure = current->lock_parent_cthread_structure;
		  
			/* And we free this memory */
			free(arg);

#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : lock on current->state_cthread_structure[%d]\n",_Cpool_self(),_Cthread_self(), index);
			}
#endif
			  
			/* We put a lock on the state index */
			if (Cthread_mutex_lock_ext(current->state_cthread_structure[index]) != 0) {
				return(NULL);
			}
			
			/* This is happening at startup only : parent is waiting on us */
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : set current->state[%d] to 0\n",
					_Cpool_self(),_Cthread_self(),index);
			}
#endif
			current->state[index] = 0;
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : cond-signal on current->state_cthread_structure[%d]\n",
					_Cpool_self(),_Cthread_self(),index);
			}
#endif
			Cthread_cond_signal_ext(current->state_cthread_structure[index]);
					
			/* We loop indefinitely */
			while (1) {
			  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : lock on lock_parent_cthread_structure\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
			  
				/* We check current->flag */
				if (Cthread_mutex_lock_ext(lock_parent_cthread_structure) != 0) {
					/* Error */
					return(NULL);
				}
			  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : current->flag is %d\n",
						_Cpool_self(),_Cthread_self(),current->flag);
				}
#endif
			  
				if (current->flag == -1) {
					/* Either the parent is waiting for any of us, or for us only */
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : current->forceid is %d\n",
							_Cpool_self(),_Cthread_self(),current->forceid);
					}
#endif
				  
					if (current->forceid == -1 || current->forceid == index) {
						/* The parent is waiting for: */
						/* -1   : any of us           */
						/* index: us, exactly         */
						/* We tell what is our index  */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : Parent waiting on %s (current->forceid=%d), setting current->flag to my index %d\n",
								_Cpool_self(),_Cthread_self(),current->forceid == -1 ? "any of us" : "me!", current->forceid, index);
						}
#endif
						current->flag = index;
						/* And we signal              */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : cond-signal on lock_parent_cthread_structure\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
						if (Cthread_cond_signal_ext(lock_parent_cthread_structure)) {
							/* Error */
#ifdef CPOOL_DEBUG
							if (Cpool_debug != 0) {
								log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : Signal on lock_parent_cthread_structure error: %s\n",
									_Cpool_self(),_Cthread_self(), strerror(errno));
							}
#endif
#ifdef CPOOL_DEBUG
							if (Cpool_debug != 0) {
								log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : un-lock on lock_parent_cthread_structure error\n",
									_Cpool_self(),_Cthread_self());
							}
#endif
							Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
							return(NULL);
						}
					}
				}
			  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : un-lock on lock_parent_cthread_structure\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
			  
				Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
			  
				/* We wait until there is something new      */
				/* The lock on "current->state[index]" is    */
				/* then released                             */
				
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : current->state[%d] is %d\n",
						_Cpool_self(),_Cthread_self(),index,current->state[index]);
				}
#endif
				while ( current->state[index] == 0 ) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : cond-wait on current->state_cthread_structure[%d] until it is != 0\n",
							_Cpool_self(),_Cthread_self(),index);
					}
#endif
					if (Cthread_cond_wait_ext(current->state_cthread_structure[index]) != 0) {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : wait on current->state_cthread_structure[%d] error: %s\n",
								_Cpool_self(),_Cthread_self(),index, strerror(errno));
						}
#endif
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : un-lock on current->state_cthread_structure[%d]\n",
								_Cpool_self(),_Cthread_self(),index);
						}
#endif
						Cthread_mutex_unlock_ext(current->state_cthread_structure[index]);
						return(NULL);
#ifdef CPOOL_DEBUG
					} else {
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : waked up on on current->state_cthread_structure[%d] with current->state[%d] == %d\n",
								_Cpool_self(),_Cthread_self(),index, index, current->state[index]);
						}
#endif
					}
				}
			  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : out of cond-wait loop, current->state[%d] is %d\n",
						_Cpool_self(),_Cthread_self(),index,current->state[index]);
				}
#endif
			  
				/* We are waked up: the routine and its arguments */
				/* address are put in current->start[index] and   */
				/* current->arg[index]                            */
				start    = (void *(*) _PROTO((void *))) current->start[index];
				startarg = (void *)            current->arg[index];
			  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : Execute 0x%lx(0x%lx)\n",
						_Cpool_self(),_Cthread_self(),(unsigned long) start,(unsigned long) (startarg != NULL ? startarg : 0));
				}
#endif
			  
				/* We execute the routine                         */
				(*start)(startarg);
			  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : Finished 0x%lx(0x%lx)\n",
						_Cpool_self(),_Cthread_self(),(unsigned long) start,(unsigned long) (startarg != NULL ? startarg : 0));
				}
#endif
			  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : Setting current->state[%d] to 0\n",
						_Cpool_self(),_Cthread_self(),index);
				}
#endif
				
				/* We flag us in the non-running state */
				current->state[index] = 0;

			}

#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter : un-lock on current->state_cthread_structure[%d]\n",
					_Cpool_self(),_Cthread_self(),index);
			}
#endif
			Cthread_mutex_unlock_ext(current->state_cthread_structure[index]);
			  
		}
#ifndef _WIN32
	}
#endif
}

#ifndef _WIN32
/* ============================================ */
/* Routine  : _Cpool_writen_timeout             */
/* Arguments: file des., pointer, size, timeout */
/* -------------------------------------------- */
/* Output   : Number of bytes writen            */
/* -------------------------------------------- */
/* History:                                     */
/* 17-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
size_t _Cpool_writen_timeout(
#ifdef CPOOL_DEBUG
	file,line,
#endif
	fd,vptr,n,timeout)
#ifdef CPOOL_DEBUG
	char *file;
	int line;
#endif
	int fd;
	void *vptr;
	size_t n;
	int timeout;
{
	size_t		nleft;
	ssize_t       nwriten;
	char         *ptr;
	int           save_serrno;

	/* We use the signal alarm */
	Sigfunc  *sigfunc;
  
#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_writen_timeout(%d,0x%lx,0x%x,%d) called at %s:%d\n",
			_Cpool_self(),_Cthread_self(),fd,(unsigned long) vptr, (unsigned int) n, timeout,
			file,line);
#endif

	/* Get previous handler */
	if ((sigfunc = _Cpool_signal(SIGALRM, _Cpool_alarm)) == SIG_ERR) {
		serrno = SEINTERNAL;
		return(0);
	}
  
	/* In any case we catch trap SIGPIPE */
	_Cpool_signal(SIGPIPE, SIG_IGN);
  
	ptr = vptr;
	nleft = n;
	while (nleft > 0) {
		alarm(timeout);
		if ( (nwriten = write(fd, ptr, nleft)) <= 0) {
			if (errno == EINTR) {
				errno = ETIMEDOUT;
				serrno = SETIMEDOUT;
			}
			goto doreturn;
		}    
		nleft -= nwriten;
		ptr   += nwriten;
	}
  doreturn:
	save_serrno = serrno;
	/* Disable alarm            */
	alarm(0);
	/* Restore previous handler */
	_Cpool_signal(SIGALRM, sigfunc);
	serrno = save_serrno;
	return(n - nleft);
}

/* ============================================ */
/* Routine  : _Cpool_writen                     */
/* Arguments: file des., pointer, size          */
/* -------------------------------------------- */
/* Output   : Number of bytes writen            */
/* -------------------------------------------- */
/* History:                                     */
/* 14-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
size_t _Cpool_writen(fd,vptr,n)
	int fd;
	void *vptr;
	size_t n;
{
	size_t		nleft;
	ssize_t       nwriten;
	char         *ptr;
	Sigfunc          *handler;

#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_writen(%d,0x%lx,0x%x)\n",
			_Cpool_self(),_Cthread_self(),fd,(unsigned long) vptr, (unsigned int) n);
#endif

	/* In any case we catch trap SIGPIPE */
	handler = _Cpool_signal(SIGPIPE, SIG_IGN);

	ptr = vptr;
	nleft = n;
	while (nleft > 0) {
		if ( (nwriten = write(fd, ptr, nleft)) <= 0) {
			if (errno == EINTR) {
				nwriten = 0;		/* and call write() again */
			} else {
				_Cpool_signal(SIGPIPE, handler);
				return(n - nleft);
			}
		}
		nleft -= nwriten;
		ptr   += nwriten;
	}
	_Cpool_signal(SIGPIPE, handler);
	return(n - nleft);
}

/* ============================================ */
/* Routine  : _Cpool_readn                      */
/* Arguments: file des., pointer, size          */
/* -------------------------------------------- */
/* Output   : Number of bytes read              */
/* -------------------------------------------- */
/* History:                                     */
/* 14-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
size_t _Cpool_readn(fd,vptr,n)
	int fd;
	void *vptr;
	size_t n;
{
	size_t	nleft;
	ssize_t   nread;
	char	*ptr;
	Sigfunc          *handler;
  
#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_readn(%d,0x%lx,0x%x)\n",
			_Cpool_self(),_Cthread_self(),fd,(unsigned long) vptr, (unsigned int) n);
#endif

	/* In any case we catch trap SIGPIPE */
	if ((handler = _Cpool_signal(SIGPIPE, SIG_IGN)) == SIG_ERR) {
		return(0);
	}
	_Cpool_signal(SIGPIPE, SIG_IGN);

	ptr = vptr;
	nleft = n;
	nread = 0;
	while (nleft > 0) {
		if ( (nread = read(fd, ptr, nleft)) < 0) {
			if (errno == EINTR) {
				nread = 0;		/* and call read() again */
			} else {
				_Cpool_signal(SIGPIPE, handler);
				return(n - nleft);
			}
		} else if (nread == 0) {
			break;				/* EOF */
		} else {
			nleft -= nread;
			ptr   += nread;
		}
	}
	_Cpool_signal(SIGPIPE, handler);
	return(n - nleft);		/* return >= 0 */
}

/* ============================================ */
/* Routine  : _Cpool_readn_timeout              */
/* Arguments: file des., pointer, size, timeout */
/* -------------------------------------------- */
/* Output   : Number of bytes read              */
/* -------------------------------------------- */
/* History:                                     */
/* 14-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
size_t _Cpool_readn_timeout(
#ifdef CPOOL_DEBUG
	file,line,
#endif
	fd,vptr,n,timeout)
#ifdef CPOOL_DEBUG
	char *file;
	int line;
#endif
	int fd;
	void *vptr;
	size_t n;
	int timeout;
{
	size_t	nleft;
	ssize_t   nread;
	char	*ptr;
	int       save_serrno;

	/* We use the signal alarm */
	Sigfunc  *sigfunc;
  
#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_readn_timeout(%d,0x%lx,0x%x,%d) called at %s:%d\n",
			_Cpool_self(),_Cthread_self(),fd,(unsigned long) vptr, (unsigned int) n, timeout,
			file,line);
#endif

	/* Get previous handler */
	if ((sigfunc = _Cpool_signal(SIGALRM, _Cpool_alarm)) == SIG_ERR) {
		serrno = SEINTERNAL;
		return(0);
	}

	/* In any case we trap SIGPIPE */
	_Cpool_signal(SIGPIPE, SIG_IGN);
  
	ptr = vptr;
	nleft = n;
	nread = 0;
	while (nleft > 0) {
		alarm(timeout);
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_readn_timeout : read(%d,0x%lx,%d)\n",
				_Cpool_self(),_Cthread_self(),fd,(unsigned long) ptr, (int) nleft);
#endif
		if ( (nread = read(fd, ptr, nleft)) < 0) {
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_readn_timeout : errno = %d (%s) [EINTR=%d]\n",
					_Cpool_self(),_Cthread_self(),errno,strerror(errno),EINTR);
#endif
			if (errno == EINTR) {
				errno = ETIMEDOUT;
				serrno = SETIMEDOUT;
			}
			goto doreturn;
		} else if (nread == 0) {
			break;				/* EOF */
		} else {
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_readn_timeout : nleft (%d) -= nread (%d)\n",
					_Cpool_self(),_Cthread_self(),(int) nleft,(int) nread);
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_readn_timeout : ptr (0x%lx) += nread (%d)\n",
					_Cpool_self(),_Cthread_self(),(unsigned long) ptr,(int) nread);
			}
#endif
			nleft -= nread;
			ptr   += nread;
		}
	}
  doreturn:
	save_serrno = serrno;
	/* Disable alarm            */
	alarm(0);
	/* Restore previous handler */
	_Cpool_signal(SIGALRM, sigfunc);
	serrno = save_serrno;
	return(n - nleft);		/* return >= 0 */
}

/* ============================================ */
/* Routine  : _Cpool_alarm                      */
/* Arguments: signal to trap                    */
/* -------------------------------------------- */
/* Output   :                                   */
/* -------------------------------------------- */
/* History:                                     */
/* 17-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
void _Cpool_alarm(signo)
	int signo;
{
#ifdef CPOOL_DEBUG
	/* Better not to do any mutex/cond etc... in a signal handler... */
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool[...[...]]] ### SIGALRM catched\n");
#endif
	/* We just trap the signal and return */
	return;
}

/* ============================================ */
/* Routine  : _Cpool_signal                     */
/* Arguments: signal to trap, signal routine    */
/* -------------------------------------------- */
/* Output   : status (SIG_ERR if error)         */
/* -------------------------------------------- */
/* History:                                     */
/* 11-MAY-1999       First implementation       */
/*                   [with R.W.Stevens example] */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
Sigfunc *_Cpool_signal(signo,func)
	int signo;
	Sigfunc *func;
{
	struct sigaction	act, oact;
	int n = 0;

	act.sa_handler = func;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	if (signo == SIGALRM) {
#ifdef	SA_INTERRUPT
		act.sa_flags |= SA_INTERRUPT;	/* SunOS 4.x */
#endif
	} else {
#ifdef	SA_RESTART
		act.sa_flags |= SA_RESTART;		/* SVR4, 44BSD */
#endif
	}
	n = sigaction(signo, &act, &oact);
	if (n < 0) {
		return(SIG_ERR);
	}

	return(oact.sa_handler);
}

/* ============================================ */
/* Set of wrappers around memory allocation     */
/* to allow passing of arguments between        */
/* forked child in case of pools processes      */
/* assignment.                                  */
/* This has also the advantage of beeing a      */
/* run-time checker for memory allocation       */
/* and use.                                     */
/* ============================================ */

/* ============================================ */
/* Routine  : Cpool_calloc                      */
/* Arguments: nb of members, size of each       */
/* -------------------------------------------- */
/* Output   : address allocated                 */
/* -------------------------------------------- */
/* History:                                     */
/* 11-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is a wrapper around calloc      */
/* ============================================ */
void DLL_DECL *Cpool_calloc(file,line,nmemb,size)
	char *file;
	int line;
	size_t nmemb;
	size_t size;
{
	struct Cmalloc_t *current  = &Cmalloc;
	struct Cmalloc_t *previous = &Cmalloc;
	char             *dummy;

	if (Cthread_environment() != CTHREAD_MULTI_PROCESS) {
		/* We are in multi-threaded mode : memory is shared    */
		/* and there is no need to try to know every allocated */
		/* memory in the Cmalloc linked list.                  */
		return(calloc(nmemb,size));
	}

#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_calloc(0x%x,0x%x) called at %s:%d\n",
			_Cpool_self(),_Cthread_self(),(unsigned int) nmemb, (unsigned int) size, file, line);
#endif

	/* We search the last element */
	while (current->next != NULL) {
		previous = current;
		current = current->next;
	}

	/* If there is no entry yet, then previous will be equal to &Cmalloc */

	/* We create an element */
	if ((previous->next = malloc(sizeof(struct Cmalloc_t))) == NULL) {
		return(NULL);
	}
	/* We create the requested memory */
	if ((previous->next->start = calloc(nmemb,size)) == NULL) {
		free(previous->next);
		previous->next = NULL;
		return(NULL);
	}
	/* Unfortunately "current->end += (nmemb * size)" is not ANSI-C and */
	/* will be rejected by a lot of compilers (exception I know is gcc) */
	dummy  = (char *) previous->next->start;
	dummy += ((nmemb * size) - 1);
	previous->next->end   = dummy;
	previous->next->next  = NULL;

	/* We return the result of _calloc */
	return(previous->next->start);
}

/* ============================================ */
/* Routine  : Cpool_malloc                      */
/* Arguments: size to allocate                  */
/* -------------------------------------------- */
/* Output   : address allocated                 */
/* -------------------------------------------- */
/* History:                                     */
/* 11-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is a wrapper around malloc      */
/* ============================================ */
void DLL_DECL *Cpool_malloc(file,line,size)
	char *file;
	int line;
	size_t size;
{
	struct Cmalloc_t *current  = &Cmalloc;
	struct Cmalloc_t *previous = &Cmalloc;
	char             *dummy;

	if (Cthread_environment() != CTHREAD_MULTI_PROCESS) {
		/* We are in multi-threaded mode : memory is shared    */
		/* and there is no need to try to know every allocated */
		/* memory in the Cmalloc linked list.                  */
		return(malloc(size));
	}

#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_malloc(%d) called at %s:%d\n",
			_Cpool_self(),_Cthread_self(),(int) size, file, line);
#endif

	/* We search the last element */
	while (current->next != NULL) {
		previous = current;
		current = current->next;
	}

	/* If there is no entry yet, then previous will be equal to &Cmalloc */

	/* We create an element */
	if ((previous->next = malloc(sizeof(struct Cmalloc_t))) == NULL) {
		return(NULL);
	}
	/* We create the requested memory */
	if ((previous->next->start = malloc(size)) == NULL) {
		free(previous->next);
		previous->next = NULL;
		return(NULL);
	}
	/* Unfortunately "current->end += (nmemb * size)" is not ANSI-C and */
	/* will be rejected by a lot of compilers (exception I know is gcc) */
	dummy  = (char *) previous->next->start;
	dummy += (size - 1);
	previous->next->end  = (void *) dummy;
	previous->next->next = NULL;

	/* We return the result of _malloc */
	return(previous->next->start);
}

/* ============================================ */
/* Routine  : Cpool_free                        */
/* Arguments: address to free                   */
/* -------------------------------------------- */
/* Output   :                                   */
/* -------------------------------------------- */
/* History:                                     */
/* 11-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is a wrapper around free        */
/* ============================================ */
void DLL_DECL Cpool_free(file,line,ptr)
	char *file;
	int line;
	void *ptr;
{
	struct Cmalloc_t *current  = &Cmalloc;
	struct Cmalloc_t *previous = NULL;
	int               n = 1;
	/* We test to see if the user wants to */
	/* to free something really allocated  */

	if (Cthread_environment() != CTHREAD_MULTI_PROCESS) {
		free(ptr);
		return;
	}

#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_free(0x%lx) called at %s:%d\n",
			_Cpool_self(),_Cthread_self(),(unsigned long) ptr, file, line);
#endif

	while (current->next != NULL) {
		previous = current;
		current = current->next;

		if (current->start == ptr) {
			n = 0;
			break;
		}
	}

	if (n) {
		/* ERROR */
		errno = EINVAL;
		return;
	}

	free(ptr);

	/* We update pointers */
	if (previous != NULL) {
		previous->next = current->next;
	} else {
		/* No more entry... */
		Cmalloc.next = NULL;
	}
	free(current);
	return;
}

/* ============================================ */
/* Routine  : Cpool_realloc                     */
/* Arguments: address to realloc, its size      */
/* -------------------------------------------- */
/* Output   :                                   */
/* -------------------------------------------- */
/* History:                                     */
/* 11-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
/* Notes:                                       */
/* This routine is a wrapper around realloc     */
/* ============================================ */
void DLL_DECL *Cpool_realloc(file,line,ptr,size)
	char *file;
	int line;
	void *ptr;
	size_t size;
{
	struct Cmalloc_t *current  = &Cmalloc;
	void             *result;
	int               n = 1;
	char             *dummy;

	if (Cthread_environment() != CTHREAD_MULTI_PROCESS) {
		return(realloc(ptr,size));
	}

#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_realloc(0x%lx,%d) called at %s:%d\n",
			_Cpool_self(),_Cthread_self(),(unsigned long) ptr, (int) size, file, line);
#endif

	/* We search the correct element */
	while (current->next != NULL) {
		current = current->next;
		if (current->start == ptr) {
			n = 0;
			break;
		}
	}

	if (n) {
		/* Error */
		errno = EINVAL;
		return(NULL);
	}

	if ((result = realloc(ptr,size)) == NULL) {
		return(NULL);
	}

	/* We update the pointer */
	current->start = result;
	/* Unfortunately "current->end += (nmemb * size)" is not ANSI-C and */
	/* will be rejected by a lot of compilers (exception I know is gcc) */
	dummy          = result;
	dummy         += (size - 1);
	current->end   = dummy;

	/* We return the result */
	return(result);
}
#endif /* _WIN32 */

/* ============================================ */
/* Routine  : Cpool_assign                      */
/* Arguments: pool number                       */
/*            starting routine                  */
/*            arguments address                 */
/*            timeout                           */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 17-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cpool_assign(poolnb,startroutine,arg,timeout)
	int poolnb;
	void *(*startroutine) _PROTO((void *));
	void *arg;
	int timeout;
{
	return(Cpool_assign_ext(poolnb,NULL,startroutine,arg,timeout));
}

/* ============================================ */
/* Routine  : Cpool_assign_ext                  */
/* Arguments: pool number                       */
/*            pool address                      */
/*            starting routine                  */
/*            arguments address                 */
/*            timeout                           */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* -------------------------------------------- */
/* History:                                     */
/* 17-MAY-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* 04-SEP-2003       Added pooladdr             */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cpool_assign_ext(poolnb,pooladdr,startroutine,arg,timeout)
	int poolnb;
	void *pooladdr;
	void *(*startroutine) _PROTO((void *));
	void *arg;
	int timeout;
{

	/* We makes sure that Cthread pakage is initalized */
	Cthread_init();
  
#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign(%d,0x%lx,0x%lx,%d)\n",
			_Cpool_self(),_Cthread_self(),poolnb, (unsigned long) startroutine, (unsigned long) arg, timeout);
#endif
    
#ifndef _WIN32
	/* THIS ROUTINE IS EXPLICITELY SPLITTED IN TWO PARTS    */
	/* - The _NOCTHREAD one (using pipes)                   */
	/* - The _CTHREAD one (using shared mem. and cond. var. */
	if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
		/*----------------------- */
		/* Non-Thread only        */
		/*----------------------- */
		struct Cpool_t *current = &Cpool;
		int    found;
		size_t length_vars;
		struct timeval          tv;
		int                     maxfd;
		int                     i;
		fd_set                  readlist;
		int                     ready;
    
		if (poolnb < 0) {
			serrno = EINVAL;
			return(-1);
		}
    
		/* In case of a non-thread environment we check that */
		/* the argument is in a windows allocated memory or  */
		/* not.                                              */
		found = 0;
		if (arg != NULL) {
			struct Cmalloc_t *current  = &Cmalloc;
			struct Cmalloc_t *previous = NULL;
      
			while (current->next != NULL) {
				previous = current;
				current = current->next;
        
				if (current->start <= arg && arg <= current->end) {
					found = 1;
					length_vars  = (size_t) current->end;
					length_vars -= (size_t) arg;
					length_vars++;
					break;
				}
			}
			if (! found) {
				/* Not in a known window : we assume it is not longer   */
				/* than the maximum size of any typedef, itself assumed */
				/* to be 8 bytes.                                       */
				length_vars = 8;
			}
		} else {
			length_vars = 0;
		}
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : length_vars set to %d\n",
				_Cpool_self(),_Cthread_self(),(int) length_vars);
#endif
    
		found = 0;
		if (pooladdr != NULL) {
			found = 1;
			current = pooladdr;
		} else {
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : lock on &Cpool\n",
					_Cpool_self(),_Cthread_self(),(int) length_vars);
#endif
			if (Cthread_mutex_lock(&Cpool) != 0) {
				return(-1);
			}
			/* We search the corresponding pool */
			current = &Cpool;
			while ((current = current->next) != NULL) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Wanted pool is %d, Got pool No %d\n",
						_Cpool_self(),_Cthread_self(),poolnb,current->poolnb);
#endif
				if (current->poolnb == poolnb) {
					found = 1;
					break;
				}
			}
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self(),(int) length_vars);
#endif
			Cthread_mutex_unlock(&Cpool);
		}
    
		if (found == 0) {
			serrno = EINVAL;
			return(-1);
		}
    
		/* We collect all the fd's to read from */
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"[Cpool  [%2d][%2d]] Wait flag on ",
				_Cpool_self(),_Cthread_self());
#endif
		maxfd = 0;
		FD_ZERO(&readlist);
		for (i = 0; i < current->nbelem; i++) {
			/* We take care of any previous call to Cpool_next_index */
			if (current->forceid != -1) {
				if (i != current->forceid) {
					continue;
				}
			}
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"%d ",current->readfd[i]);
#endif
			if (current->readfd[i] > maxfd)
				maxfd = current->readfd[i];
			FD_SET(current->readfd[i],&readlist);
		}
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"\n");
#endif
    
		/* We wait for a flag of any of the child */
		if (timeout > 0) {
			tv.tv_sec  = timeout;
			tv.tv_usec = 0;
			/* Possible warning on hpux 10 because cma_select expects "int" */
			/* instead of "fd_set"                                          */
			if (select(maxfd+1, (_cpool_fd_set *) &readlist, NULL, NULL, &tv) <= 0) {
				/* Error or timeout */
				/* ... We reset any previous call to Cpool_next_index */
				current->forceid = -1;
				serrno = SETIMEDOUT;
				return(-1);
			}
		} else {
			/* Possible warning on hpux 10 because cma_select expects "int" */
			/* instead of "fd_set"                                          */
			if (select(maxfd+1, (_cpool_fd_set *) &readlist, NULL, NULL, NULL) < 0) {
				/* Error */
				/* ... We reset any previous call to Cpool_next_index */
				current->forceid = -1;
				serrno = SEINTERNAL;
				return(-1);
			}
		}
    
		/* We got one, let's know which one it is */
		i = 0;
		for (i = 0; i <= current->nbelem; i++) {
			if (FD_ISSET(current->readfd[i],&readlist)) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] Got flag on %d\n",
						_Cpool_self(),_Cthread_self(),current->readfd[i]);
#endif
				/* We read the flag */
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] Read the flag on %d\n",
						_Cpool_self(),_Cthread_self(),current->readfd[i]);
#endif
				_Cpool_readn(current->readfd[i], (void *) &ready, sizeof(int));
				/* And we send the arguments */
				/* ... address of the start routine */
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] Write routine address = 0x%lx on %d\n",
						_Cpool_self(),_Cthread_self(),(unsigned long) startroutine,current->writefd[i]);
#endif
				_Cpool_writen(current->writefd[i],(void *) &startroutine,sizeof(void *));
				/* In a non-thread environment we write the argument length and content */
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] Write argument length = %d on %d\n",
						_Cpool_self(),_Cthread_self(),(int) length_vars,current->writefd[i]);
#endif
				_Cpool_writen(current->writefd[i],(void *) &length_vars,sizeof(size_t));
				/* And the arguments themselves */
				if (length_vars > 0) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0)
						log(LOG_INFO,"[Cpool  [%2d][%2d]] Write argument content of size %d on %d\n",
							_Cpool_self(),_Cthread_self(),(int) length_vars,current->writefd[i]);
#endif
					_Cpool_writen(current->writefd[i],arg,length_vars);
				}
				break;
			}
		}
    
		/* ... We reset any previous call to Cpool_next_index */
		current->forceid = -1;

		/* Return OK */
		return(0);

	} else {
#endif /* _WIN32 */
		{
			/*----------------------- */
			/* Thread only            */
			/*----------------------- */
			struct Cpool_t *current = &Cpool;
			int    found;
			int    i;
			void *lock_parent_cthread_structure = NULL;
		  
			if (poolnb < 0) {
				errno = EINVAL;
				return(-1);
			}
		  
			found = 0;
			if (pooladdr != NULL) {
				current = pooladdr;
				found = 1;
				lock_parent_cthread_structure = current->lock_parent_cthread_structure;
			} else {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : lock on &Cpool\n",
						_Cpool_self(),_Cthread_self());
#endif
				if (Cthread_mutex_lock(&Cpool) != 0) {
					return(-1);
				}
				/* We search the corresponding pool */
				current = &Cpool;
				while ((current = current->next) != NULL) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Wanted pool is %d, Got pool No %d\n",
							_Cpool_self(),_Cthread_self(),poolnb,current->poolnb);
					}
#endif
					if (current->poolnb == poolnb) {
						found = 1;
						lock_parent_cthread_structure = current->lock_parent_cthread_structure;
						break;
					}
				}
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on &Cpool\n",
						_Cpool_self(),_Cthread_self());
#endif
				Cthread_mutex_unlock(&Cpool);
			}
		  
			if (found == 0) {
				serrno = EINVAL;
				return(-1);
			}
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : lock on lock_parent_cthread_structure\n",
					_Cpool_self(),_Cthread_self());
			}
#endif
		  
			/* We assume that we will not have to wait   */
			if (Cthread_mutex_lock_ext(lock_parent_cthread_structure) != 0) {
				return(-1);
			}
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Setting current->flag to -2 (parent not waiting)\n",
					_Cpool_self(),_Cthread_self());
			}
#endif

			current->flag = -2;
		  
			/* We check if there is one thread available */
			found = 0;
		  
			for (i = 0; i < current->nbelem; i++) {

				if (current->forceid != -1) {
					if (i != current->forceid) {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : current->forceid=%d, skipped index %d\n",
								_Cpool_self(),_Cthread_self(), current->forceid, i);
						}
#endif
						continue;
					}
				}
#ifdef CPOOL_DEBUG
				if (current->forceid == -1) {
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : try-lock on current->state_cthread_structure[%d]\n",
							_Cpool_self(),_Cthread_self(), i);
					}
				} else {
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : lock on current->state_cthread_structure[%d], timeout=%d\n",
							_Cpool_self(),_Cthread_self(), i, timeout);
					}
				}
#endif
				if (((current->forceid == -1) ? Cthread_mutex_trylock_ext(current->state_cthread_structure[i]) : Cthread_mutex_timedlock_ext(current->state_cthread_structure[i], timeout)) == 0) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : current->state[%d]=%d, current->forceid=%d\n",
							_Cpool_self(),_Cthread_self(), i, current->state[i], current->forceid);
					}
#endif
					if (current->state[i] == 0) {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Found thread at index %d\n",
								_Cpool_self(),_Cthread_self(),i);
						}
#endif
						
						/* Okay: We change the predicate */
						/* value on which the thread is  */
						/* waiting for to be changed     */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Setting current->state[%d] to 1\n",
								_Cpool_self(),_Cthread_self(),i);
						}
#endif
						current->state[i] = 1;
						/* We put the routine and its    */
						/* arguments                     */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Setting current->start = 0x%lx, i = %d, stored at 0x%lx\n",
								_Cpool_self(),_Cthread_self(),
								(unsigned long) current->start,i,
								(unsigned long) (current->start + (i * sizeof(void *))));
						}
#endif
						current->start[i] = (void *(*) _PROTO((void *))) startroutine;
						current->arg[i] = (void *) arg;
						/* We signal the thread          */
						
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->forceid to -1\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
						/* ... We reset any call to Cpool_next_index */
						current->forceid = -1;
						
						/* Note: Only one thread can wait on that condition variable */
						/* It means that we are sure that Cthread will internal use */
						/* a pure cond_signal instead of a cond_broadcast - this is */
						/* better */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : cond-signal on current->state_cthread_structure[%d]\n",
								_Cpool_self(),_Cthread_self(), i);
						}
#endif
						if (Cthread_cond_signal_ext(current->state_cthread_structure[i]) != 0) {
#ifdef CPOOL_DEBUG
							if (Cpool_debug != 0) {
								log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Oooups... Cthread_cond_signal_ext(current->state_cthread_structure[%d]) error (%s)\n",
									_Cpool_self(),_Cthread_self(),i,strerror(errno));
							}
#endif
#ifdef CPOOL_DEBUG
							if (Cpool_debug != 0) {
								log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on current->state_cthread_structure[%d]\n",
									_Cpool_self(),_Cthread_self(),i);
							}
#endif
							Cthread_mutex_unlock_ext(current->state_cthread_structure[i]);
#ifdef CPOOL_DEBUG
							if (Cpool_debug != 0) {
								log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure\n",
									_Cpool_self(),_Cthread_self());
							}
#endif
							Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
							return(-1);
						}
						
						/* We remember for the immediate out of this loop... */
						found = 1;
				  
						/* And we exit of the loop       */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on current->state_cthread_structure[%d]\n",
								_Cpool_self(),_Cthread_self(), i);
						}
#endif
						Cthread_mutex_unlock_ext(current->state_cthread_structure[i]);
						break;
					} else {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on current->state_cthread_structure[%d]\n",
								_Cpool_self(),_Cthread_self(), i);
						}
#endif
						Cthread_mutex_unlock_ext(current->state_cthread_structure[i]);
					}
#ifdef CPOOL_DEBUG
				} else {
					if (current->forceid == -1) {
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : try-lock on current->state_cthread_structure[%d] failed: %s\n",
								_Cpool_self(),_Cthread_self(), i, strerror(errno));
						}
					} else {
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : lock on current->state_cthread_structure[%d] failed: %s\n",
								_Cpool_self(),_Cthread_self(), i, strerror(errno));
						}
					}
#endif
				}
			}
		  
			if (found != 0) {
				/* We found at least one thread available: */
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
		  
				Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
				return(0);
			} else {
				/* This is an error if a there was a previous call to Cpool_next_index(): then Cpool_assign() did try to */
				/* assign the current->forceid and only to it. Suppose that user gave a timeout of zero: it is possible */
				/* that Cpool_assign() tried to get lock on current->state_cthread_structure[] before the child putted */
				/* itself in condition wait. Then error is EBUSY. A safe way to work is to call: */
				/* Cpool_next_index() with a timeout */
				/* Cpool_assign() with no timeout (or a timeout large enough (?)) */
				if (current->forceid != -1) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->forceid to -1\n",
							_Cpool_self(),_Cthread_self());
					}
#endif
					current->forceid = -1;
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure\n",
							_Cpool_self(),_Cthread_self());
					}
#endif
		  
					Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
					return(-1);
				}
			}
		  
			/* We did not found one thread available */
			/* We say that we are waiting            */
		  
			if (timeout == 0) {
				/* No thread immediately available, and timeout == 0 */
				/* So we exit immediately                            */
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->forceid to -1\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				current->forceid = -1;
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure and setting SEWOULDBLOCK\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				
				Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
				serrno = SEWOULDBLOCK;
				return(-1);
			}
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Setting current->flag to -1 (parent waiting)\n",
					_Cpool_self(),_Cthread_self());
			}
#endif
		  
			current->flag = -1;
		  
			/* And we wait on our predicate          */
			while (current->flag == -1) {
				if (timeout > 0) {
				  
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : cond-timedwait on lock_parent_cthread_structure with timeout = %d seconds until current->flag != -1\n",
							_Cpool_self(),_Cthread_self(),timeout);
					}
#endif
				  
					if (Cthread_cond_timedwait_ext(lock_parent_cthread_structure,timeout) != 0) {
					  
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Waiting condition failed, reset current->flag to -2 (parent not waiting)\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
					  
						current->flag = -2;
					  
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
					  
						Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
					  
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->forceid to -1\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
						/* ... We reset any call to Cpool_next_index */
						current->forceid = -1;
					  
						return(-1);
					}
				} else {
					/* timeout < 0 : we wait, wait, wait... */
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : cond-wait on lock_parent_cthread_structure until current->flag != -1\n",
							_Cpool_self(),_Cthread_self());
					}
#endif
				  
					if (Cthread_cond_wait_ext(lock_parent_cthread_structure) != 0) {
					  
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Waiting condition failed, reset current->flag to -2 (parent not waiting)\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
					  
						current->flag = -2;
					  
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->forceid to -1\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
						/* ... We reset any call to Cpool_next_index */
						current->forceid = -1;
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
					  
						Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
					  					  
						return(-1);
					}
				}
			}
		  
			/* Yes... */
			/* current->flag contains the index */
			/* of the thread that tell us it is */
			/* available                        */
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Waiting condition okay, current->flag=%d\n",
					_Cpool_self(),_Cthread_self(),current->flag);
			}
#endif
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : lock on current->state_cthread_structure[%d]\n",
					_Cpool_self(),_Cthread_self(), current->flag);
			}
#endif
		  
			/* Shall we use timeout again here !? The problem is that timeout (user parameter) has already */
			/* been used in the condition wait. In theory, now, the procedure must not fail, but it is possible */
			/* that we take some real time do do the following, time for the child to put itself on cond-wait */
			if (Cthread_mutex_lock_ext(current->state_cthread_structure[current->flag]) != 0) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : lock on current->state_cthread_structure[%d] error: %s\n",
						_Cpool_self(),_Cthread_self(), current->flag, strerror(errno));
				}
#endif
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Reset current->flag to -2 (parent not waiting)\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				current->flag = -2;
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->forceid to -1\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				current->forceid = -1;
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
				return(-1);
			}
			/* We change child predicate        */
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Thread that answered has index %d, Setting predicate current->state[%d] to 1\n",
					_Cpool_self(),_Cthread_self(),current->flag,current->flag);
			}
#endif
		  
			current->state[current->flag] = 1;
			/* We put arguments                 */
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Setting current->start[%d] to 0x%lx and current->arg[%d] to 0x%lx\n",
					_Cpool_self(),_Cthread_self(),current->flag,(unsigned long) startroutine,current->flag,
					arg != NULL ? (unsigned long) arg : 0);
			}
#endif
		  
			/* We put the routine and its    */
			/* arguments                     */
			current->start[current->flag] = (void *(*) _PROTO((void *))) startroutine;
			current->arg[current->flag] = (void *) arg;
		  
			/* We signal the child       */
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : cond-signal on current->state_cthread_structure[%d]\n",
					_Cpool_self(),_Cthread_self(), current->flag);
			}
#endif
		  
			if (Cthread_cond_signal_ext(current->state_cthread_structure[current->flag]) != 0) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Signalling on current->state_cthread_structure[%d] error: %s\n",
						_Cpool_self(),_Cthread_self(), current->flag, strerror(errno));
				}
#endif
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on current->state_cthread_structure[%d]\n",
						_Cpool_self(),_Cthread_self(), current->flag);
				}
#endif
				Cthread_mutex_unlock_ext(current->state_cthread_structure[current->flag]);
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Reset current->flag to -2 (parent not waiting)\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				current->flag = -2;
				  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->forceid to -1\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				current->forceid = -1;

#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
				return(-1);
			}
			/* We release our lock on his predicate */
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on current->state_cthread_structure[%d]\n",
					_Cpool_self(),_Cthread_self(), current->flag);
			}
#endif
		  
			Cthread_mutex_unlock_ext(current->state_cthread_structure[current->flag]);
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->flag to -2 (parent not waiting)\n",
					_Cpool_self(),_Cthread_self());
			}
#endif
			current->flag = -2;
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : Set current->forceid to -1\n",
					_Cpool_self(),_Cthread_self());
			}
#endif
			current->forceid = -1;
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign : un-lock on lock_parent_cthread_structure\n",
					_Cpool_self(),_Cthread_self());
			}
#endif
			Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
			return(0);
		}
#ifndef _WIN32
	}
#endif
}

/* ============================================ */
/* Routine  : Cpool_next_index_timeout          */
/* Arguments: pool number                       */
/*            timeout (<= 0 means no timeout)   */
/* -------------------------------------------- */
/* Output   : index >= 0 (OK) -1 (ERROR)        */
/* -------------------------------------------- */
/* History:                                     */
/* 08-JUN-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cpool_next_index_timeout(poolnb,timeout)
	int poolnb;
	int timeout;
{
	return(Cpool_next_index_timeout_ext(poolnb,NULL,timeout));
}
/* ============================================ */
/* Routine  : Cpool_next_index_timeout_ext      */
/* Arguments: pool number                       */
/*            pool address                      */
/*            timeout (<= 0 means no timeout)   */
/* -------------------------------------------- */
/* Output   : index >= 0 (OK) -1 (ERROR)        */
/* -------------------------------------------- */
/* History:                                     */
/* 08-JUN-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* 04-SEP-2003       Added pooladdr             */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL Cpool_next_index_timeout_ext(poolnb,pooladdr,timeout)
	int poolnb;
	void *pooladdr;
	int timeout;
{

	/* We makes sure that Cthread pakage is initalized */
	Cthread_init();
  
#ifdef CPOOL_DEBUG
	if (Cpool_debug != 0)
		log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext(%d,0x%lx,%d)\n",
			_Cpool_self(),_Cthread_self(),poolnb,(unsigned long) pooladdr, timeout);
#endif
    
#ifndef _WIN32
	/* THIS ROUTINE IS EXPLICITELY SPLITTED IN TWO PARTS    */
	/* - The _NOCTHREAD one (using pipes)                   */
	/* - The _CTHREAD one (using shared mem. and cond. var. */
	if (Cthread_environment() == CTHREAD_MULTI_PROCESS) {
		/*----------------------- */
		/* Non-Thread only        */
		/*----------------------- */
		struct Cpool_t *current = &Cpool;
		int    found;
		int                     maxfd;
		int                     i;
		fd_set                  readlist;
		int                     ready;
		struct timeval timeval;               /* For timeout */
		int select_rc;

		if (poolnb < 0) {
			serrno = EINVAL;
			return(-1);
		}
    
		found = 0;
		if (pooladdr != NULL) {
			found = 1;
			current = pooladdr;
		} else {
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			if (Cthread_mutex_lock(&Cpool) != 0) {
				return(-1);
			}
			/* We search the corresponding pool */
			current = &Cpool;
			while ((current = current->next) != NULL) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Wanted pool is %d, Got pool No %d\n",
						_Cpool_self(),_Cthread_self(),poolnb,current->poolnb);
#endif
				if (current->poolnb == poolnb) {
					found = 1;
					break;
				}
			}
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : un-lock on &Cpool\n",
					_Cpool_self(),_Cthread_self());
#endif
			Cthread_mutex_unlock(&Cpool);
		}
    
		if (found == 0) {
			errno = EINVAL;
			return(-1);
		}
    
		/* We collect all the fd's to read from */
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Wait flag on ",
				_Cpool_self(),_Cthread_self());
#endif
		/* We build the timeout if needed */
		if (timeout > 0) {
			timeval.tv_sec = timeout;
			timeval.tv_usec = 0;
		}
		maxfd = 0;
		FD_ZERO(&readlist);
		for (i = 0; i < current->nbelem; i++) {
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0)
				log(LOG_INFO,"%d ",current->readfd[i]);
#endif
			if (current->readfd[i] > maxfd)
				maxfd = current->readfd[i];
			FD_SET(current->readfd[i],&readlist);
		}
#ifdef CPOOL_DEBUG
		if (Cpool_debug != 0)
			log(LOG_INFO,"\n");
#endif
    
		/* We wait for a flag of any of the child */
		if ((select_rc = select(maxfd+1, (_cpool_fd_set *) &readlist, NULL, NULL, (timeout > 0 ? &timeval : NULL))) <= 0) {
			/* Error */
			serrno = (select_rc == 0 ? SETIMEDOUT : SEINTERNAL);
			return(-1);
		}
    
		/* We got one, let's know which one it is */
		i = 0;
		for (i = 0; i <= current->nbelem; i++) {
			if (FD_ISSET(current->readfd[i],&readlist)) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Got flag on %d\n",
						_Cpool_self(),_Cthread_self(),current->readfd[i]);
#endif
				/* We read the flag */
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Read the flag on %d\n",
						_Cpool_self(),_Cthread_self(),current->readfd[i]);
#endif
				_Cpool_readn(current->readfd[i], (void *) &ready, sizeof(int));
				/* And we send a wait flag */
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Write sleep flag on %d\n",
						_Cpool_self(),_Cthread_self(),current->writefd[i]);
#endif
				_Cpool_writen(current->writefd[i],(void *) &_cpool_sleep_flag,sizeof(void *));
				/* We remember it for the next assignment */
				current->forceid = i;
				return(i);
			}
		}
    
		/* We should not be there */
		errno = SEINTERNAL;
		return(-1);
    
	} else {
#endif /* _WIN32 */
		{
			/*----------------------- */
			/* Thread only            */
			/*----------------------- */
			struct Cpool_t *current = &Cpool;
			int    found;
			int    i;
			void *lock_parent_cthread_structure = NULL;
		  
			if (poolnb < 0) {
				errno = EINVAL;
				return(-1);
			}
		  
			found = 0;
			if (pooladdr != NULL) {
				current = pooladdr;
				found = 1;
				lock_parent_cthread_structure = current->lock_parent_cthread_structure;
			} else {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : lock on &Cpool\n",
						_Cpool_self(),_Cthread_self());
#endif
				if (Cthread_mutex_lock(&Cpool) != 0) {
					return(-1);
				}
				/* We search the corresponding pool */
				current = &Cpool;
				while ((current = current->next) != NULL) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Got pool No %d\n",
							_Cpool_self(),_Cthread_self(),current->poolnb);
					}
#endif
					if (current->poolnb == poolnb) {
						found = 1;
						lock_parent_cthread_structure = current->lock_parent_cthread_structure;
						break;
					}
				}
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0)
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : un-lock on &Cpool\n",
						_Cpool_self(),_Cthread_self());
#endif
				Cthread_mutex_unlock(&Cpool);
			}
		  
			if (found == 0) {
				errno = EINVAL;
				return(-1);
			}
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : lock on lock_parent_cthread_structure\n",
					_Cpool_self(),_Cthread_self());
			}
#endif
		  
			/* We assume that we will not have to wait   */
			if (Cthread_mutex_lock_ext(lock_parent_cthread_structure) != 0) {
				return(-1);
			}
		  
			for (i = 0; i < current->nbelem; i++) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : try-lock on current->state_cthread_structure[%d]\n",
						_Cpool_self(),_Cthread_self(), i);
				}
#endif
				if (Cthread_mutex_trylock_ext(current->state_cthread_structure[i]) == 0) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : current->state[%d] is %d\n",
							_Cpool_self(),_Cthread_self(), poolnb, current->state[i]);
					}
#endif
					if (current->state[i] == 0) {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Found thread at index %d\n",
								_Cpool_self(),_Cthread_self(),i);
						}
#endif
						
						/* Okay: We will return this index */
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : un-lock on current->state_cthread_structure[%d], set current->forceid to %d\n",
								_Cpool_self(),_Cthread_self(), i, i);
						}
#endif
						Cthread_mutex_unlock_ext(current->state_cthread_structure[i]);
						current->forceid = i;
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : un-lock on lock_parent_cthread_structure\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
						
						Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Return %d\n",
								_Cpool_self(),_Cthread_self(),i);
						}
#endif
						return(i);
					} else {
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : un-lock on current->state_cthread_structure[%d]\n",
								_Cpool_self(),_Cthread_self(), i);
						}
#endif
						Cthread_mutex_unlock_ext(current->state_cthread_structure[i]);
					}
#ifdef CPOOL_DEBUG
				} else {
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : try-lock on current->state_cthread_structure[%d] error: %s\n",
							_Cpool_self(),_Cthread_self(), i, strerror(errno));
					}
#endif
				}
			}
		  
			/* We did not found one thread available */
			/* We say that we are waiting            */
		  
			while (1) {
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : set current->flag to -1\n",
						_Cpool_self(),_Cthread_self());
				}
#endif
				current->flag = -1;
			  
				/* And we wait on our predicate          */
				while (current->flag == -1) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : cond-timedwait on lock_parent_cthread_structure\n",
							_Cpool_self(),_Cthread_self());
					}
#endif
				  
					if (Cthread_cond_timedwait_ext(lock_parent_cthread_structure,timeout) != 0) {
					  
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Waiting condition failed, reset current->flag to zero\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
					  
						current->flag = -2;
					  
#ifdef CPOOL_DEBUG
						if (Cpool_debug != 0) {
							log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : un-lock on lock_parent_cthread_structure\n",
								_Cpool_self(),_Cthread_self());
						}
#endif
					  
					  
						Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
						return(-1);
					}
				}
			  
				/* Yes... */
				/* current->flag contains the index */
				/* of the thread that tell us it is */
				/* available                        */
			  
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Waiting condition okay, returns current->flag=%d\n",
						_Cpool_self(),_Cthread_self(),current->flag);
				}
#endif
#ifdef CPOOL_DEBUG
				if (Cpool_debug != 0) {
					log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : current->state[current->flag=%d] is %d\n",
						_Cpool_self(),_Cthread_self(), current->flag, current->state[current->flag]);
				}
#endif
			  
				/* We check that this thread is REALLY in a READY status */
				if (current->state[current->flag] != 0) {
#ifdef CPOOL_DEBUG
					if (Cpool_debug != 0) {
						log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Thread No %d's state is not ready for assignment. Continue.\n",
							_Cpool_self(),_Cthread_self(),current->flag);
					}
#endif
					continue;
				}
				break;
			}
		  
		  
			/* ... And remember it to force assignment */
			i = current->forceid = current->flag;
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : un-lock on lock_parent_cthread_structure\n",
					_Cpool_self(),_Cthread_self());
			}
#endif
			Cthread_mutex_unlock_ext(lock_parent_cthread_structure);
		  
#ifdef CPOOL_DEBUG
			if (Cpool_debug != 0) {
				log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext : Return %d\n",
					_Cpool_self(),_Cthread_self(),i);
			}
#endif
		  
			return(i);
		}
#ifndef _WIN32
	}
#endif
}

/* ============================================ */
/* Routine  : _Cpool_self                       */
/* Arguments:                                   */
/* -------------------------------------------- */
/* Output   : pool number >= 0 (OK) -1 (ERROR)  */
/* -------------------------------------------- */
/* History:                                     */
/* 12-JUL-1999       First implementation       */
/*                   Jean-Damien.Durand@cern.ch */
/* ============================================ */
int DLL_DECL _Cpool_self() {
	struct Cpool_t *current = NULL;
	int             i;
	int             cid;

	/* Nota: There is no call to Cthread_mutex_lock
	   nor Cthread_mutex_unlock on &Cpool because
	   this would block Cpool execution.
	*/

	/* We makes sure that Cthread pakage is initalized */
	Cthread_init();
  
	/* We get current Cthread ID */
	if ((cid = _Cthread_self()) < 0) {
		return(-1);
	}
  
	/* We search the corresponding pool */
	current = &Cpool;
	while ((current = current->next) != NULL) {
		for (i = 0; i < current->nbelem; i++) {
			if (current->cid[i] == cid) {
				return(current->poolnb);
			}
		}
	}
  
	/* We do not set serrno for _Cpool_self() */
	/* serrno = EINVAL; */
	return(-1);
}





