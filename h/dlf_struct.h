/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: dlf_struct.h,v $ $Revision: 1.5 $ $Date: 2005/03/23 11:16:20 $ CERN IT-ADC Vitaly Motyakov
 */


#ifndef _DLFSTRUCT_H_
#define _DLFSTRUCT_H_

#include "Cuuid.h"
#include "osdep.h"
#include "Castor_limits.h"
#include "Cns_api.h"

#include "dlf.h"

struct dlf_api_thread_info
{
  char*           errbufp;
  int             errbuflen;
  int             initialized;
  int             vm_errno;
};

struct _dlf_level {
  int lvl_id;
  char *lvl_name;
};

typedef struct _dlf_log_dst
{
  struct _dlf_log_dst* next;
  int dst_type;
  int port;
  int descr;    /* Socket or file descriptor for persistent connection */
  char name[CA_MAXPATHLEN + 1];
  int severity_mask;

} dlf_log_dst_t;

typedef struct _dlf_log_dst_slist
{
  dlf_log_dst_t* head;
  dlf_log_dst_t* tail;

} dlf_log_dst_slist_t;


typedef struct _dlf_msg_text
{
  struct _dlf_msg_text* next;
  U_SHORT msg_no;
  char* msg_text;

} dlf_msg_text_t;

typedef struct _dlf_msg_text_slist
{
  dlf_msg_text_t* head;
  dlf_msg_text_t* tail;

} dlf_msg_text_slist_t;

typedef struct _dlf_msg_facility
{
  struct _dlf_msg_facility* next;
  U_BYTE fac_no;
  char fac_name[DLF_MAXFACNAMELEN + 1];

} dlf_msg_facility_t;

typedef struct _dlf_msg_facility_slist
{
  dlf_msg_facility_t* head;
  dlf_msg_facility_t* tail;

} dlf_msg_facility_slist_t;

typedef struct _dlf_facility_info
{
  char fac_name[DLF_MAXFACNAMELEN + 1];
  U_BYTE fac_no;
  dlf_msg_text_slist_t text_list;
  dlf_log_dst_slist_t dest_list;
  unsigned short default_port;

} dlf_facility_info_t;

typedef struct _dlf_msg_parameter
{
  struct _dlf_msg_parameter* next;
  struct _dlf_msg_parameter* prev;
  U_BYTE type;
  char name[DLF_MAXPARNAMELEN+1];
  U_BYTE strval[DLF_MAXSTRVALLEN+1];
  HYPER numval;
  double dval;

} dlf_msg_param_t;

typedef struct _dlf_msg_param_list
{
  dlf_msg_param_t* head;
  dlf_msg_param_t* tail;

} dlf_msg_param_list_t;


typedef struct _dlf_log_message
{
  char time[DLF_TIMESTRLEN + 1]; /* Time string in the format YYYYMMDDHHMMSS */
  int time_usec;                 /* Time microseconds */
  Cuuid_t request_id;
  char hostname[CA_MAXHOSTNAMELEN+1];
#ifdef _WIN32
  int pid;
#else
  pid_t pid;
#endif
  int cid;                                /* Cthread identifier */
  struct Cns_fileid ns_fileid;
  U_BYTE facility_no;
  U_BYTE severity;
  U_SHORT message_no;
  dlf_msg_param_list_t param_list;
  int next_subreq_idx;
  int next_tapevid_idx;

} dlf_log_message_t;

typedef struct dlf_write_param
{
  char *name;
  int type;
  union {
    char *par_string;
    int par_int;
    u_signed64 par_u64;
    double par_double;
    Cuuid_t par_uuid;
  } par;
} dlf_write_param_t;

#endif
