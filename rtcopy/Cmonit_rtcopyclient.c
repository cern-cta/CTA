/*
 * $id$
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/* 
 * Cmonit_rtcopyclient.c - Send monitoring info to Cmonitd
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

#include "marshall.h"
#include "Cmonit_api.h"
#include "serrno.h"
#include "Cnetdb.h"
#include "common.h"

/*
 * This method sends a packet to the Monitoring daemon with the state of all the drives in the server.
 */
int DLL_DECL Cmonit_send_rtcopy_status(int   subtype,
				       uid_t uid,
				       gid_t gid,
				       int   jid,
				       int   stgreqid,
				       char  reqtype,
				       char  *ifce,
				       char  *vid,
				       int   size,
				       int   retryn,
				       int   exitcode,
				       char  *clienthost,
				       char  *dsksrvr,
				       int   fseq,
				       char  *errmsgtxt,
				       char *drive)
{
  char buf[CA_MAXUDPPLEN * 2];
  char *sbp;
  char *nbpos, *start_content;
  int send_data = 1;
  int cur_tp_drive = 0;
  int nb_sent, sd;
  
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
  
  sbp = buf;
  
  marshall_LONG(sbp, MREC_MAGIC);
  marshall_LONG(sbp, MREC_RTCOPY);
  nbpos = sbp;
  marshall_LONG(sbp, 0);
  start_content = sbp;
    
  marshall_LONG (sbp, subtype);
  marshall_LONG (sbp, uid);
  marshall_LONG (sbp, gid);
  marshall_LONG (sbp, jid);
  marshall_LONG (sbp, stgreqid);
  marshall_BYTE (sbp, reqtype);
  marshall_STRING (sbp, ifce);
  marshall_STRING (sbp, vid);
  marshall_LONG (sbp, size);
  marshall_LONG (sbp, retryn);
  marshall_LONG (sbp, exitcode);
  marshall_STRING (sbp, clienthost);
  marshall_STRING (sbp, dsksrvr);
  marshall_STRING (sbp, drive);
  marshall_LONG (sbp, fseq);
  marshall_STRING (sbp, errmsgtxt);


  /* Writing the message length */
  marshall_LONG(nbpos, sbp - start_content );

  /* Sending the packet */
  nb_sent = Cmonit_send(sd, buf, (sbp - buf));
  if (nb_sent <= 0) {
    close(sd);
    return(-1);
  }
  close(sd);
  return(0);
}


/*
 * This method sends a packet to the Monitoring daemon with the state of a file transfered
 */
int DLL_DECL Cmonit_send_transfer_info(int   jid,
				       u_signed64 transfered,
				       u_signed64 total,
				       u_signed64 rate,
				       int current_file,
				       int nb_files)
{
  char buf[CA_MAXUDPPLEN * 2];
  char *sbp;
  char *nbpos, *start_content;
  int send_data = 1;
  int cur_tp_drive = 0;
  int nb_sent, sd;
  
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
  
  sbp = buf;
  
  marshall_LONG(sbp, MREC_MAGIC);
  marshall_LONG(sbp, MREC_FTRANSFER);
  nbpos = sbp;
  marshall_LONG(sbp, 0);
  start_content = sbp;
    
  marshall_LONG (sbp, jid);
  marshall_HYPER(sbp, transfered);
  marshall_HYPER(sbp, total);
  marshall_HYPER(sbp, rate);
  marshall_LONG(sbp, current_file);
  marshall_LONG(sbp, nb_files);

  /* Writing the message length */
  marshall_LONG(nbpos, sbp - start_content );

  /* Sending the packet */
  nb_sent = Cmonit_send(sd, buf, (sbp - buf));
  if (nb_sent <= 0) {
    close(sd);
    return(-1);
  }
  close(sd);
  return(0);
}






