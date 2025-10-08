/*
 * SPDX-FileCopyrightText: 2002 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "mediachanger/librmc/smc_struct.hpp"
#include "mediachanger/rmcd/rmc_constants.hpp"
#include "mediachanger/rmcd/smc_constants.hpp"
#include "mediachanger/rmcd/rbtsubr_constants.hpp"

int rmc_dismount(const char* const server, const char* const vid, const int drvord, const int force);

/**
 * Send error message to user-defined client buffer or to stderr
 */
int rmc_errmsg(const char* const func, const char* const msg, ...);

int rmc_export(const char* const server, const char* const vid);
int rmc_find_cartridge(const char* const server,
                       const char* const pattern,
                       const int type,
                       const int startaddr,
                       const int nbelem,
                       struct smc_element_info* const element_info);
int rmc_get_geometry(const char* const server, struct robot_info* const robot_info);
int rmc_import(const char* const server, const char* const vid);
int rmc_mount(const char* const server, const char* const vid, const int side, const int drvord);
int rmc_read_elem_status(const char* const server,
                         const int type,
                         const int startaddr,
                         const int nbelem,
                         struct smc_element_info* const element_info);

/**
 * Set receiving buffer for error messages
 */
void rmc_seterrbuf(const char* const buffer, const int buflen);

int send2rmc(const char* const host,
             const char* const reqp,
             const int reql,
             char* const user_repbuf,
             const int user_repbuf_len);
