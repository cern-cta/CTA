/*
 * Copyright (C) 1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* ------------------------------------ */
/* For the what command                 */
/* ------------------------------------ */

#include <Cpool_api.h>
/* We do these #undef because we want to create entry points */
/* for old application not recompiled using shared library */
/* and the #define's would cause is some problem... */
#undef Cpool_create
#undef Cpool_assign
#undef Cpool_next_index
#undef Cpool_next_index_timeout
#include <serrno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
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

/* ------------------------------------ */
/* Linked list of pools                 */
/* ------------------------------------ */
struct Cpool_t {
  int                     lock_parent;
  int                     poolnb;   /* Pool number                        */
  int                     nbelem;   /* Nb of elems                        */
  int                    *cid;      /* Elements Cthread ID                */
  int                     forceid;  /* Index forcing the assignment to    */
  int                    *writefd;  /* Parent->Child (only on unix)       */
  int                    *readfd;   /* Child->Parent (only on unix)       */
  int                    *state;    /* Elements current status (0=READY)  */
  /*                        (1=RUNNING) */
  /*                       (-1=STARTED) */
	
  int                     flag;     /* Parent flag (-1=WAITING)           */
  void                  *(**start) (void *); /* Start routines             */
  void                  **arg;
	
  void                   *lock_parent_cthread_structure;
  void                   **state_cthread_structure;

  struct Cpool_t         *next;     /* Next pool                          */
};

static struct Cpool_t Cpool = { -1, 0, 0, NULL, -1, NULL, NULL, NULL, 0, NULL, NULL, NULL, NULL, NULL};

/* ------------------------------------ */
/* Typedefs                             */
/* ------------------------------------ */
typedef void    Sigfunc (int);
typedef fd_set _cpool_fd_set;

/* ------------------------------------ */
/* Prototypes                           */
/* ------------------------------------ */
void    *_Cpool_starter (void *);
size_t   _Cpool_writen (int, void *, size_t);
size_t   _Cpool_readn (int, void *, size_t);
#ifdef CPOOL_DEBUG
size_t   _Cpool_writen_timeout (char *, int, int, void *, size_t, int);
size_t   _Cpool_readn_timeout (char *, int, int, void *, size_t, int);
#else
size_t   _Cpool_writen_timeout (int, void *, size_t, int);
size_t   _Cpool_readn_timeout (int, void *, size_t, int);
#endif
void     _Cpool_alarm (int);
Sigfunc *_Cpool_signal (int, Sigfunc *);
int     _Cpool_self();

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
int Cpool_create(int nbreq,
                 int *nbget)
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
int Cpool_create_ext(int nbreq,
                     int *nbget,
                     void **pooladdr)
{
  struct Cpool_t         *current          = &Cpool;
  struct Cpool_t         *previous         = &Cpool;
  int                     i;
  int                     j;
  int                     k;
  int                     poolnb, nbcreated;
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
  current->next = NULL;
  /* We tell that there is no dispatch at this time */
  current->flag = -2;
    
  nbcreated = j = k = 0;
  /* We create the pools */
  for (i = 0; i < nbreq; i++) {
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
    if (current->cid[i] >= 0) {
      /* We count number of created processes */
      ++nbcreated;
    }
  }

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
void *_Cpool_starter(void *arg)
{
#ifdef CPOOL_DEBUG
  if (Cpool_debug != 0)
    log(LOG_INFO,"[Cpool  [%2d][%2d]] In _Cpool_starter(0x%lx)\n",
	_Cpool_self(),_Cthread_self(),(unsigned long) arg);
#endif
  {
    /*----------------------- */
    /* Thread only			  */
    /*----------------------- */
    struct Cpool_t *current;
    int 			index;
    char		   *dummy;
    void		  *(*start) (void *);
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
      start    = (void *(*) (void *)) current->start[index];
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
}

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
size_t _Cpool_writen(int fd,
                     void *vptr,
                     size_t n)
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
size_t _Cpool_readn(int fd,
                    void *vptr,
                    size_t n)
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
void _Cpool_alarm(int signo)
{
  (void)signo;
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
Sigfunc *_Cpool_signal(int signo,
                       Sigfunc *func)
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
/* Routine  : Cpool_calloc                      */
/* Arguments: nb of members, size of each       */
/* -------------------------------------------- */
/* Output   : address allocated                 */
/* ============================================ */
void *Cpool_calloc(char *file,
                   int line,
                   size_t nmemb,
                   size_t size)
{
  (void)file;
  (void)line;
  return(calloc(nmemb,size));
}

/* ============================================ */
/* Routine  : Cpool_malloc                      */
/* Arguments: size to allocate                  */
/* -------------------------------------------- */
/* Output   : address allocated                 */
/* ============================================ */
void *Cpool_malloc(char *file,
                   int line,
                   size_t size)
{
  (void)file;
  (void)line;
  return(malloc(size));
}

/* ============================================ */
/* Routine  : Cpool_free                        */
/* Arguments: address to free                   */
/* -------------------------------------------- */
/* Output   :                                   */
/* ============================================ */
void Cpool_free(char *file,
                int line,
                void *ptr)
{
  (void)file;
  (void)line;
  free(ptr);
}

/* ============================================ */
/* Routine  : Cpool_realloc                     */
/* Arguments: address to realloc, its size      */
/* -------------------------------------------- */
/* Output   :                                   */
/* ============================================ */
void *Cpool_realloc(char *file,
                    int line,
                    void *ptr,
                    size_t size)
{
  (void)file;
  (void)line;
  return(realloc(ptr,size));
}

/* ============================================ */
/* Routine  : Cpool_assign                      */
/* Arguments: pool number                       */
/*            starting routine                  */
/*            arguments address                 */
/*            timeout                           */
/* -------------------------------------------- */
/* Output   : 0 (OK) -1 (ERROR)                 */
/* ============================================ */
int Cpool_assign(int poolnb,
                 void *(*startroutine) (void *),
                 void *arg,
                 int timeout)
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
int Cpool_assign_ext(int poolnb,
                     void *pooladdr,
                     void *(*startroutine) (void *),
                     void *arg,
                     int timeout)
{

  /* We makes sure that Cthread package is initialized */
  Cthread_init();
  
#ifdef CPOOL_DEBUG
  if (Cpool_debug != 0)
    log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_assign(%d,0x%lx,0x%lx,%d)\n",
	_Cpool_self(),_Cthread_self(),poolnb, (unsigned long) startroutine, (unsigned long) arg, timeout);
#endif
    
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
	current->start[i] = (void *(*) (void *)) startroutine;
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
  current->start[current->flag] = (void *(*) (void *)) startroutine;
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
int Cpool_next_index_timeout(int poolnb,
                             int timeout)
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
int Cpool_next_index_timeout_ext(int poolnb,
                                 void *pooladdr,
                                 int timeout)
{

  /* We makes sure that Cthread pakage is initalized */
  Cthread_init();
  
#ifdef CPOOL_DEBUG
  if (Cpool_debug != 0)
    log(LOG_INFO,"[Cpool  [%2d][%2d]] In Cpool_next_index_timeout_ext(%d,0x%lx,%d)\n",
	_Cpool_self(),_Cthread_self(),poolnb,(unsigned long) pooladdr, timeout);
#endif

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
int _Cpool_self() {
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
