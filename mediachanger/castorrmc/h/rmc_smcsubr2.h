/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#pragma once

#include "h/osdep.h"
#include "h/smc_struct.h"

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

