/******************************************************************************
 *                      stage_api.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * This is an old file from the CASTOR1 times that had been dropped.
 * A minimum set of empty functions was readded as a transitionnal measure,
 * so that applications linking with both CASTOR1 and CASTOR2, but only using
 * CASTOR2 are not broken by the dropping of CASTOR1 support.
 * The application we have in mind here is actually ROOT and its
 * TCastorFile class.
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef H_STAGE_API_H
#define H_STAGE_API_H 1

#include <pwd.h>
#include <sys/types.h>
#include "osdep.h"
#include "Castor_limits.h"
#include "stage_constants.h"

EXTERN_C int stage_setoutbuf(char *, int);
EXTERN_C int stage_seterrbuf(char *, int);

struct stgcat_entry {
  int blksize;
  char filler[2];
  char charconv;
  char keep;
  int lrecl;
  int nread;
  char poolname[CA_MAXPOOLNAMELEN+1];
  char recfm[CA_MAXRECFMLEN+1];
  u_signed64 size;
  char ipath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
  char t_or_d;
  char group[CA_MAXGRPNAMELEN+1];
  char user[CA_MAXUSRNAMELEN+1];
  uid_t uid;
  gid_t gid;
  mode_t mask;
  int reqid;
  int status;
  u_signed64 actual_size;
  time_t c_time;
  time_t a_time;
  int nbaccesses;
  union {
    struct {
      char den[CA_MAXDENLEN+1];
      char dgn[CA_MAXDGNLEN+1];
      char fid[CA_MAXFIDLEN+1];
      char filstat;
      char fseq[CA_MAXFSEQLEN+1];
      char lbl[CA_MAXLBLTYPLEN+1];
      int  retentd;
      int  side;
      char tapesrvr[CA_MAXHOSTNAMELEN+1];
      char E_Tflags;
      char vid[MAXVSN][CA_MAXVIDLEN+1];
      char vsn[MAXVSN][CA_MAXVSNLEN+1];
    } t;
    struct {
      char xfile[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
      char Xparm[23];
    } d;
    struct {
      char xfile[STAGE_MAX_HSMLENGTH+1];
    } m;
    struct {
      char xfile[STAGE_MAX_HSMLENGTH+1];
      char server[CA_MAXHOSTNAMELEN+1];
      u_signed64 fileid;
      short fileclass;
      char tppool[CA_MAXPOOLNAMELEN+1];
      int  retenp_on_disk;
      int  mintime_beforemigr;
      short   flag;
    } h;
  } u1;
};

struct stgpath_entry {
  int reqid;
  char upath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
};

EXTERN_C int stage_iowc (int,char,u_signed64,int,mode_t,char *,char *,int,
                         struct stgcat_entry *,int *,struct stgcat_entry **,
                         int, struct stgpath_entry *);

#define stage_in_hsm(flags,openflags,hostname,pooluser,nstcp_input,stcp_input, \
                     nstcp_output,stcp_output,nstpp_input,stpp_input)   \
  stage_iowc(STAGE_IN, 'm',flags,openflags,(mode_t) 0,hostname,pooluser, \
             nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input, \
             stpp_input)
#define stage_out_hsm(flags,openflags,openmode,hostname,pooluser,nstcp_input, \
                      stcp_input,nstcp_output,stcp_output,nstpp_input,  \
                      stpp_input)                                       \
  stage_iowc(STAGE_OUT,'m',flags,openflags,openmode,hostname,pooluser,  \
             nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input, \
             stpp_input)

struct stage_hsm {
  char *xfile;
  char *upath;
  u_signed64 size;
  struct stage_hsm *next;
};
typedef struct stage_hsm stage_hsm_t;

EXTERN_C int stage_updc_filchg (char *, stage_hsm_t *);

#endif // H_STAGE_API_H
