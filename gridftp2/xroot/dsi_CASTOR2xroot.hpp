/******************************************************************************
 *                     dsi_CASTOR2xroot.h
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
 *
 *
 *
 * @author Victor Kotlyar, Victor.Kotlyar@cern.ch
 *****************************************************************************/

#if !defined(DSI_CASTOR2XROOT_HPP)
#define DSI_CASTOR2XROOT_HPP

extern "C" {

#include "globus_gridftp_server.h"

typedef struct globus_l_gfs_CASTOR2xroot_handle_s {
  globus_mutex_t mutex;
  int fd;
  globus_result_t cached_res;
  int outstanding;
  int optimal_count;
  globus_bool_t done;
  globus_off_t blk_length;
  globus_off_t blk_offset;
  globus_size_t block_size;
  globus_gfs_operation_t op;
  /* if use_uuid is true we will use uuid and fullDestPath in the
     file accessing commands */
  globus_bool_t use_uuid;
  /* must be pointers to environment variables */
  char *uuid;
  /* we do not allocate or free memory here    */
  char *fullDestPath;
  char *access_mode;
} globus_l_gfs_CASTOR2xroot_handle_t;

static void globus_l_gfs_file_net_read_cb
(globus_gfs_operation_t,
 globus_result_t,
 globus_byte_t *,
 globus_size_t,
 globus_off_t,
 globus_bool_t,
 void *);

static void
globus_l_gfs_CASTOR2xroot_read_from_net(globus_l_gfs_CASTOR2xroot_handle_t *);
static globus_result_t globus_l_gfs_make_error(const char*, int);

void fill_stat_array(globus_gfs_stat_t *, struct stat, char *);
void free_stat_array(globus_gfs_stat_t * ,int);
int CASTOR2xroot_handle_open(char *, int, int,
                             globus_l_gfs_CASTOR2xroot_handle_t *);
static globus_bool_t globus_l_gfs_CASTOR2xroot_send_next_to_client
(globus_l_gfs_CASTOR2xroot_handle_t *);
static void globus_l_gfs_net_write_cb(globus_gfs_operation_t,
                                      globus_result_t,
                                      globus_byte_t *,
                                      globus_size_t,
                                      void *);
} // end extern "C"

#endif  /* DSI_CASTOR2XROOT_HPP */
