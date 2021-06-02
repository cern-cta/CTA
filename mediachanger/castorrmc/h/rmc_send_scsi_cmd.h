/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
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

#include "osdep.h"

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
