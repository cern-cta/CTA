/*
 * $id$
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
/* 
 * Cmonit_stageclient.c - Sens monitoring information to Cmonitd
 */

#include <stddef.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

#include <sys/stat.h>
#include <stage_server_struct.h>
#include "marshall.h"
#include "Cmonit_api.h"
#include "serrno.h"
#include "Cnetdb.h"
#include "common.h"

extern int stglogit _PROTO((char *, char *, ...));

/* Functions used only in this module */
int Cmonit_write_pool(struct pool *pool_p, char **p);
int Cmonit_write_migrator(struct migrator *migrator_p, char **p);

/*
 *  Method that send a disk monitoring message to the monitoring server 
 * This method is used by the stager daemon to report its status
 */
int DLL_DECL Cmonit_send_stager_status(time_t last_init_time)
{ 
  int sd, nb_sent; 
  int data_left, space_left;
  char buf[CA_MAXUDPPLEN * 2];
  char *p, *p_message_length, *p_start_content;

  /* Structures containing the Stager status */
  time_t this_time = time(NULL);
  extern struct pool *pools; /* Extern link to the sturcture with all pools */
  extern int nbpool; /* Extern variable with the number of declared pools */
  extern struct migrator *migrators;
  extern int nbmigrator;

  /* Init */
  struct pool *pool_p = pools;
  struct migrator *migrator_p = migrators;
  int current_pool_nb = 0;
  int current_migrator_nb = 0;
  struct stat st;
  char nomorestage = 0;


  /* Checking the initialization */
  if (Cmonit_init() == -1) {
    return(-1);
  }

  /* Creating the socket used for subsequent sends */
  sd = socket(AF_INET, SOCK_DGRAM, 0); 
  if (sd == -1) {
    serrno = EMON_SYSTEM;
    return(-1);
  }

  /* Creating the packet header */
  p = (char*)buf;
  marshall_LONG(p, MREC_MAGIC);
  marshall_LONG(p, MREC_STAGER);
  p_message_length = p;
  marshall_LONG(p, 0);
  marshall_TIME_T(p, this_time);
  /* Inserting the last stgdaemon init time in the daemon */
  marshall_TIME_T(p, last_init_time);
  
  /* Check whether a NOMORESTAGE file has been created on the stager */
  if (stat (NOMORESTAGE, &st) == 0) {
    nomorestage = 1;
  }

  marshall_BYTE(p, nomorestage); 

 p_start_content = p;
  
  /* The amount of data depends on the number of pools and migrators to send */
  data_left = ( (nbpool + nbmigrator) > 0 );

 /*   stglogit("Monitor", "Data left:%d\n", data_left); */

  while (data_left) {
    p = p_start_content; /* Always overwriting the same buffer */
    space_left = 1;

    while (space_left && data_left) {

      if (current_pool_nb < nbpool) {
/*  	 stglogit("Monitor", "Writing pool:%d\n", current_pool_nb); */
	Cmonit_write_pool(pool_p, &p);      
	pool_p++;
	current_pool_nb++;
      } else if (current_migrator_nb < nbmigrator) {
	Cmonit_write_migrator(migrator_p, &p);
	migrator_p++;
	current_migrator_nb++;
      } else {
	/* Nothing else to write */
	data_left = 0;
      }
      space_left = ( (p - buf) < CA_MAXUDPPLEN);
      /* This allows for packets slightly bigger the UDP_MAXLEN but will be fixed later */
    } /* End while (space_left && data_left) */

    /* Setting the packet length, in records to read */
    marshall_LONG(p_message_length, (p - p_start_content) );
 
    /*  stglogit("Monitor", "Sending %d bytes, %d, %d\n", p-buf, nbpool, nbmigrator); */
 
    /* Sending the packet */
    nb_sent = Cmonit_send(sd, buf, (p - buf));
    if (nb_sent <= 0) {
      close(sd);
      return(-1);
    }

  } /* End While data left */
  /* Closing the socket and returning */
  close(sd); 
  return(0);
}
 

/*
 * Function that writes pool data into the send buffer
 */
int Cmonit_write_pool(struct pool *pool_p, char **p) {
  struct pool_element *elemp;
  int j;

  if ((p == NULL) || (pool_p == NULL)) {
    return(-1);
  }

  marshall_BYTE(*p, STYPE_POOL);
  marshall_STRING(*p, pool_p->name);
  marshall_STRING(*p, pool_p->gc);
  marshall_LONG(*p, pool_p->gc_start_thresh);
  marshall_LONG(*p, pool_p->gc_stop_thresh);
  marshall_LONG(*p,  pool_p->ovl_pid);
  marshall_TIME_T(*p,  pool_p->cleanreqtime);
  marshall_HYPER(*p, pool_p->capacity);
  marshall_HYPER(*p, pool_p->free);
  marshall_LONG(*p, pool_p->mig_start_thresh);
  marshall_LONG(*p, pool_p->mig_stop_thresh);
  marshall_STRING(*p, pool_p->migr_name);
  marshall_LONG(*p, pool_p->nbelem); 
  for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
    marshall_STRING(*p, elemp->server);
    marshall_STRING(*p, elemp->dirpath);
    marshall_HYPER(*p, elemp->capacity);
    marshall_HYPER(*p, elemp->free);
  } /* End for(j = 0... */
  return(0);
}

/*
 * Function that writes migrator data into the send buffer
 */
int Cmonit_write_migrator(struct migrator *migrator_p, char **p) {

  if ((p == NULL) || (migrator_p == NULL)) {
    return(-1);
  }

  marshall_BYTE(*p, STYPE_MIGRATOR);
  marshall_STRING(*p, migrator_p->name);
  marshall_LONG(*p,  migrator_p->mig_pid);
  marshall_TIME_T(*p,  migrator_p->migreqtime);
  marshall_TIME_T(*p,  migrator_p->migreqtime_last_start);
  marshall_TIME_T(*p,  migrator_p->migreqtime_last_end);

  marshall_LONG(*p, migrator_p->global_predicates.nbfiles_canbemig);
  marshall_HYPER(*p, migrator_p->global_predicates.space_canbemig);
  marshall_LONG(*p, migrator_p->global_predicates.nbfiles_delaymig);  
  marshall_HYPER(*p, migrator_p-> global_predicates.space_delaymig);
  marshall_LONG(*p, migrator_p->global_predicates.nbfiles_beingmig); 
  marshall_HYPER(*p, migrator_p-> global_predicates.space_beingmig);

  return(0);
}
