/*
 * 
 * Copyright (C) 2004 by CERN/IT/ADC
 * All rights reserved
 *
 * @(#)$RCSfile: expert_api.h,v $ $Revision: 1.2 $ $Date: 2008/07/28 16:55:05 $ CERN IT-ADC/CA Vitaly Motyakov";
 *
 */

#ifndef EXPERT_API_H
#define EXPERT_API_H

#include "expert.h"

/* Function prototypes */

EXTERN_C int expert_send_request (int*, int);
EXTERN_C int expert_send_data (int, const char*, int);
EXTERN_C int expert_receive_data (int, char*, int, int);
EXTERN_C int send2expert (int*, char*, int);
EXTERN_C int getexpertrep (int, int*, int*, int*);

#endif
