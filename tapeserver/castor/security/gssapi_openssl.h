#ifndef GLOBUS_DONT_DOCUMENT_INTERNAL
/**
 * @author Castor Dev team, castor-dev@cern.ch
 * 
 * $Revision: 1.3 $
 * $Date: 2008/01/16 15:37:29 $
 */
#endif

#pragma once

#include "gssapi.h"
#include "globus_gsi_gss_constants.h"

#include "globus_common.h"
#include "globus_gsi_callback.h"
#include "globus_gsi_proxy.h"
#include "globus_gsi_credential.h"

#include <stdio.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/stack.h>

#define GLOBUS_I_GSI_GSSAPI_IMPL_VERSION            1

#define GSS_I_CTX_INITIALIZED                       1
#define GSS_I_DISALLOW_ENCRYPTION                   2
#define GSS_I_PROTECTION_FAIL_ON_CONTEXT_EXPIRATION 4
#define GSS_I_APPLICATION_WILL_HANDLE_EXTENSIONS    8

#define GSS_C_QOP_GLOBUS_GSSAPI_OPENSSL_BIG 1

/*
 * we need to distinguish between a token
 * created by us using get_mic vs one using
 * the SSL application data
 * We use this in wrap and unwrap
 * Future versions of SSL may use this
 *
 * Our wrapped buffer (integrity only) has
 *
 *  byte  type[1]          = SSL3_RT_GSSAPI_OPENSSL
 *  byte  version_major[1] = 0x03
 *  byte  version_minor[1] = 0
 *  byte  mic_length[2]    = 2 byte length of following mic 
 * 
 *  byte  mic_seq[8]           = 8 byte sequence number
 *  byte  mic_data_length[4]   = 4 byte length of data 
 *  byte  hash[*]          = the hash of variable length
 *
 *  byte  data[*]          = the data being wrapped. 
 */

#define SSL3_RT_GSSAPI_OPENSSL                   26

/* These conversions macros are taken from SSL */

#define L2N(LONG_VAL, CHAR_ARRAY) \
   {  \
       char *                           _char_array_ = CHAR_ARRAY; \
       *(_char_array_++) = (unsigned char) (((LONG_VAL) >> 24) & 0xff); \
       *(_char_array_++) = (unsigned char) (((LONG_VAL) >> 16) & 0xff); \
       *(_char_array_++) = (unsigned char) (((LONG_VAL) >> 8)  & 0xff); \
       *(_char_array_++) = (unsigned char) (((LONG_VAL))       & 0xff); \
   }

#define N2L(CHAR_ARRAY, LONG_VAL) \
   { \
       char *                           _char_array_ = CHAR_ARRAY; \
       (LONG_VAL)  = ((unsigned long) (*(_char_array_++))) << 24; \
       (LONG_VAL) |= ((unsigned long) (*(_char_array_++))) << 16; \
       (LONG_VAL) |= ((unsigned long) (*(_char_array_++))) << 8; \
       (LONG_VAL) |= ((unsigned long) (*(_char_array_++))); \
   }

#define N2S(CHAR_ARRAY, SHORT) \
   { \
       char *                           _char_array_ = CHAR_ARRAY; \
       (SHORT)  = ((unsigned int) (*(_char_array_++))) << 8; \
       (SHORT) |= ((unsigned int) (*(_char_array_++))); \
   }

#define S2N(SHORT, CHAR_ARRAY) \
   { \
       char *                           _char_array_ = CHAR_ARRAY; \
       *(_char_array_++) = (unsigned char) (((SHORT) >> 8) & 0xff); \
       *(_char_array_++) = (unsigned char) ((SHORT) & 0xff); \
   } 

/* Compare OIDs */

#define g_OID_equal(o1, o2) \
        (((o1) == (o2)) || \
         ((o1) && (o2) && \
         ((o1)->length == (o2)->length) && \
         (memcmp((o1)->elements,(o2)->elements,(int) (o1)->length) == 0)))

typedef struct gss_name_desc_struct {
    /* gss_buffer_desc  name_buffer ; */
    gss_OID                             name_oid;
    X509_NAME *                         x509n;
    STACK *                             group;
    ASN1_BIT_STRING *                   group_types;
} gss_name_desc;

typedef struct gss_cred_id_desc_struct {
    globus_gsi_cred_handle_t            cred_handle;
    gss_name_desc *                     globusid;
    gss_cred_usage_t                    cred_usage;
    SSL_CTX *                           ssl_context;
} gss_cred_id_desc;

typedef struct gss_ctx_id_desc_struct{
    globus_mutex_t                      mutex;
    globus_gsi_callback_data_t          callback_data;
    gss_cred_id_desc *                  peer_cred_handle;
    gss_cred_id_desc *                  cred_handle;
    gss_cred_id_desc *                  deleg_cred_handle;
    globus_gsi_proxy_handle_t           proxy_handle;
    OM_uint32                           ret_flags;
    OM_uint32                           req_flags;
    OM_uint32                           ctx_flags;
    int                                 cred_obtained;
    SSL *                               gss_ssl; 
    BIO *                               gss_rbio;
    BIO *                               gss_wbio;
    BIO *                               gss_sslbio;
    gss_con_st_t                        gss_state;
    int                                 locally_initiated;
    gss_delegation_state_t              delegation_state;
} gss_ctx_id_desc;

extern
const gss_OID_desc * const              gss_mech_globus_gssapi_openssl;

extern
const gss_OID_desc * const              gss_proxycertinfo_extension;

extern
globus_thread_once_t                    once_control;

