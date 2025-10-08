/*
 * SPDX-FileCopyrightText: 1998 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "mediachanger/librmc/smc_struct.hpp"

int smc_dismount(const int rpfd,
                 const int fd,
                 const char* const loader,
                 struct robot_info* const robot_info,
                 const int drvord,
                 const char* const vid);

int smc_export(const int rpfd,
               const int fd,
               const char* const loader,
               struct robot_info* const robot_info,
               const char* const vid);

int smc_import(const int rpfd,
               const int fd,
               const char* const loader,
               struct robot_info* const robot_info,
               const char* const vid);

int smc_mount(const int rpfd,
              const int fd,
              const char* const loader,
              struct robot_info* const robot_info,
              const int drvord,
              const char* const vid,
              const int invert);
