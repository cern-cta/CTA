/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 1998-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "Castor_limits.h"

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
