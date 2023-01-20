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

#include "mediachanger/castorrmc/h/osdep.h"

EXTERN_C int rmc_send_scsi_cmd (
  const int tapefd,
  const char *const path,
  const int do_not_open,
  const unsigned char *const cdb,
  const int cdblen,
  unsigned char *const buffer,
  const int buflen,
  char *const sense,
  const int senselen,
  const int flags,
  int *const nb_sense_ret,
  const char **const msgaddr);
