/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "mediachanger/librmc/smc_struct.hpp"

int smc_get_geometry(const int fd, const char* const rbtdev, struct robot_info* const robot_info);

int smc_read_elem_status(const int fd,
                         const char* const rbtdev,
                         const int type,
                         const int start,
                         const int nbelem,
                         struct smc_element_info element_info[]);

int smc_find_cartridgeWithoutSendVolumeTag(const int fd,
                                           const char* const rbtdev,
                                           const char* const find_template,
                                           const int type,
                                           const int start,
                                           const int nbelem,
                                           struct smc_element_info element_info[]);

int smc_find_cartridge(const int fd,
                       const char* const rbtdev,
                       const char* const find_template,
                       const int type,
                       const int start,
                       const int nbelem,
                       struct smc_element_info element_info[],
                       struct robot_info* const robot_info);

int smc_lasterror(struct smc_status* const smc_stat, const char** const msgaddr);

int smc_move_medium(const int fd, const char* const rbtdev, const int from, const int to, const int invert);
