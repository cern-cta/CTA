/*
 * 
 * Copyright (C) 2004 by CERN/IT/ADC
 * All rights reserved
 *
 * @(#)$RCSfile: expert_api.h,v $ $Revision: 1.1 $ $Date: 2004/06/30 16:09:56 $ CERN IT-ADC/CA Vitaly Motyakov";
 *
 */

#ifndef EXPERT_API_H
#define EXPERT_API_H

#include "expert.h"

/* Function prototypes */

EXTERN_C int DLL_DECL expert_send_request _PROTO((int*, int));
EXTERN_C int DLL_DECL expert_send_data _PROTO((int, const char*, int));
EXTERN_C int DLL_DECL expert_receive_data _PROTO((int, char*, int, int));

#endif
