/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _SMC_STRUCT_H
#define _SMC_STRUCT_H 1

#include "h/Castor_limits.h"

struct robot_info {
  char inquiry[32];
  int transport_start;
  int transport_count;
  int slot_start;
  int slot_count;
  int port_start;
  int port_count;
  int device_start;
  int device_count;
};

struct extended_robot_info {
  int     smc_fd;
  char    smc_ldr[CA_MAXRBTNAMELEN+1];
  int     smc_support_voltag;
  struct robot_info robot_info;
};

struct smc_element_info {
  int element_address;
  int element_type;
  int state;
  unsigned char asc;
  unsigned char ascq;
  int flags;
  int source_address;
  char name[9];
};

struct smc_status {
  unsigned char asc;
  unsigned char ascq;
  int save_errno;
  int rc;		/* return code from send_scsi_cmd */
  unsigned char sensekey;
  int skvalid;	/* sense key is valid */
};

#endif
