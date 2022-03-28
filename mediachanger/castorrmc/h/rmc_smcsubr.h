/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2003-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "osdep.h"
#include "smc_struct.h"

EXTERN_C int smc_get_geometry(
  const int fd,
  const char *const rbtdev,
  struct robot_info *const robot_info);

EXTERN_C int smc_read_elem_status(
  const int fd,
  const char *const rbtdev,
  const int type,
  const int start,
  const int nbelem,
  struct smc_element_info element_info[]);

EXTERN_C int smc_find_cartridgeWithoutSendVolumeTag (
  const int fd,
  const char *const rbtdev,
  const char *const find_template,
  const int type,
  const int start,
  const int nbelem,
  struct smc_element_info element_info[]);

EXTERN_C int smc_find_cartridge(
  const int fd,
  const char *const rbtdev,
  const char *const find_template,
  const int type,
  const int start,
  const int nbelem,
  struct smc_element_info element_info[],
  struct robot_info *const robot_info);

EXTERN_C int smc_lasterror(
  struct smc_status *const smc_stat,
  const char **const msgaddr);

EXTERN_C int smc_move_medium(
  const int fd,
  const char *const rbtdev,
  const int from,
  const int to,
  const int invert);
