/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: dlf_api.h,v $ $Revision: 1.12 $ $Date: 2005/03/23 11:16:12 $ CERN IT-ADC Vitaly Motyakov
 */

#ifndef _DLFAPI_H_
#define _DLFAPI_H_

#include "dlf_struct.h"

#define DLF_PRTBUF_LEN 4094

#define DLF_DST_FILE 1
#define DLF_DST_TCPHOST 2
#define DLF_DST_UDPHOST 3

#define DLF_MSG_PARAM_STR     1
#define DLF_MSG_PARAM_NUM     2
#define DLF_MSG_PARAM_UUID    3
#define DLF_MSG_PARAM_INT     4
#define DLF_MSG_PARAM_INT64   5
#define DLF_MSG_PARAM_FLOAT   6
#define DLF_MSG_PARAM_DOUBLE  7
#define DLF_MSG_PARAM_TPVID   8

#define DLF_LVL_EMERGENCY  1
#define DLF_LVL_ALERT      2
#define DLF_LVL_ERROR      3
#define DLF_LVL_WARNING    4
#define DLF_LVL_AUTH       5
#define DLF_LVL_SECURITY   6
#define DLF_LVL_USAGE      7
#define DLF_LVL_SYSTEM     8
#define DLF_LVL_IMPORTANT  9
#define DLF_LVL_DEBUG     10

#define DLF_LVL_ALL         -1
#define DLF_LVL_SERVICE_ONLY 0

#define CLOSE_RETURN(x) \
        { \
        fclose (in_file); \
        return (x); \
        }

/* Function prototypes */

EXTERN_C int DLL_DECL dlf_init _PROTO((const char*));
EXTERN_C int DLL_DECL dlf_reinit _PROTO((const char*));
EXTERN_C char DLL_DECL * dlf_get_severity_name _PROTO((int));
EXTERN_C char DLL_DECL * dlf_get_token _PROTO((const char*, const char*, char*, char*, int));
EXTERN_C char DLL_DECL * dlf_parse_dst _PROTO((const char*, int*, int*, char*));
EXTERN_C int DLL_DECL dlf_store_log_destinations _PROTO((const char*, int, dlf_log_dst_slist_t*));
EXTERN_C dlf_log_dst_t DLL_DECL * dlf_find_dst _PROTO((int, int, const char*, dlf_log_dst_slist_t*));
EXTERN_C int DLL_DECL dlf_add_to_log_dst _PROTO((int, int, const char*, int, dlf_log_dst_slist_t*));
EXTERN_C dlf_log_dst_t DLL_DECL * dlf_find_server _PROTO((int));
EXTERN_C char DLL_DECL * dlf_get_msg_text _PROTO((int));
EXTERN_C int DLL_DECL dlf_gettexts _PROTO((const char*, unsigned int*, dlf_msg_text_slist_t*));
EXTERN_C int DLL_DECL dlf_gettexts_from_file _PROTO((const char*, unsigned int*, dlf_msg_text_slist_t*));
EXTERN_C int DLL_DECL dlf_add_to_text_list _PROTO((int, const char*, dlf_msg_text_slist_t*));
EXTERN_C char DLL_DECL * dlf_format_str _PROTO((char*, int, const char*, ...));
EXTERN_C int DLL_DECL dlf_write _PROTO((Cuuid_t, int, int, struct Cns_fileid*, int, ...));
EXTERN_C int DLL_DECL dlf_writep _PROTO((Cuuid_t, int, int, struct Cns_fileid *, int, struct dlf_write_param []));
EXTERN_C char DLL_DECL * dlf_uuid2hex _PROTO((const Cuuid_t, char*, int));
EXTERN_C int DLL_DECL dlf_write_to_file _PROTO((dlf_log_dst_t*, dlf_log_message_t*));
EXTERN_C int DLL_DECL dlf_calc_msg_size _PROTO((dlf_log_message_t*));
EXTERN_C int DLL_DECL dlf_send_to_host _PROTO((dlf_log_dst_t*, dlf_log_message_t*, int, int));
EXTERN_C int DLL_DECL islittleendian _PROTO((void));
EXTERN_C dlf_log_message_t DLL_DECL * dlf_new_log_message _PROTO((void));
EXTERN_C void DLL_DECL dlf_free_log_message _PROTO((dlf_log_message_t*));
EXTERN_C void DLL_DECL dlf_delete_param_list _PROTO((dlf_msg_param_list_t*));
EXTERN_C int DLL_DECL dlf_log_message_init _PROTO((dlf_log_message_t*));
EXTERN_C dlf_msg_param_t DLL_DECL * new_dlf_param _PROTO((void));
EXTERN_C int DLL_DECL dlf_add_str_parameter _PROTO((dlf_log_message_t*, const char*, const char*));
EXTERN_C int DLL_DECL dlf_add_int_parameter _PROTO((dlf_log_message_t*, const char*, HYPER));
EXTERN_C int DLL_DECL dlf_add_double_parameter _PROTO((dlf_log_message_t*, const char*, double));
EXTERN_C int DLL_DECL dlf_add_to_param_list _PROTO((dlf_msg_param_list_t*, dlf_msg_param_t*));
EXTERN_C int DLL_DECL dlf_add_subreq_id _PROTO((dlf_log_message_t*, Cuuid_t));
EXTERN_C int DLL_DECL dlf_add_tpvid_parameter _PROTO((dlf_log_message_t*, const char*));
EXTERN_C int DLL_DECL getrep _PROTO((int, char**, int*, int*));
EXTERN_C int DLL_DECL send2dlf _PROTO((int*, dlf_log_dst_t*, char*, int));
EXTERN_C int DLL_DECL dlf_isinteger _PROTO((const char*));
EXTERN_C int DLL_DECL dlf_isfloat _PROTO((const char*));
EXTERN_C int DLL_DECL dlf_hex2uuid _PROTO((const char*, Cuuid_t));
EXTERN_C int DLL_DECL dlf_hexto4bits _PROTO((const int));
EXTERN_C int DLL_DECL dlf_hex2u64 _PROTO((const char*, u_signed64*));

EXTERN_C int DLL_DECL dlf_enterfacility _PROTO((const char*, int));
EXTERN_C int DLL_DECL dlf_entertext _PROTO((const char*, int, const char*));
EXTERN_C int DLL_DECL dlf_modifyfacility _PROTO((const char*, int));
EXTERN_C int DLL_DECL dlf_modifytext _PROTO((const char*, int, const char*));
EXTERN_C int DLL_DECL dlf_deletefacility _PROTO((const char*));
EXTERN_C int DLL_DECL dlf_deletetext _PROTO((const char*, int));

EXTERN_C int DLL_DECL dlf_errmsg _PROTO((const char *, ...));
EXTERN_C int DLL_DECL dlf_seterrbuf _PROTO((char *, int));

#if defined(_WIN32)
EXTERN_C int DLL_DECL getopt _PROTO((int, const char **, const char *));
#endif

#endif /* _DLFAPI_H_ */
