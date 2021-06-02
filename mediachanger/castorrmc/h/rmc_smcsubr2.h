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

#include "osdep.h"
#include "smc_struct.h"

EXTERN_C int smc_dismount (
  const int rpfd,
  const int fd,
  const char *const loader,
  struct robot_info *const robot_info,
  const int drvord,
  const char *const vid);

EXTERN_C int smc_export (
  const int rpfd,
  const int fd,
  const char *const loader,
  struct robot_info *const robot_info,
  const char *const vid);

EXTERN_C int smc_import (
  const int rpfd,
  const int fd,
  const char *const loader,
  struct robot_info *const robot_info,
  const char *const vid);

EXTERN_C int smc_mount (
  const int rpfd,
  const int fd,
  const char *const loader,
  struct robot_info *const robot_info,
  const int drvord,
  const char *const vid,
  const int invert);
