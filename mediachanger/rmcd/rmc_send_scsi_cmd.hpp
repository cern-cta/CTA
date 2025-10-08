/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

int rmc_send_scsi_cmd(const int tapefd,
                      const char* const path,
                      const int do_not_open,
                      const unsigned char* const cdb,
                      const int cdblen,
                      unsigned char* const buffer,
                      const int buflen,
                      char* const sense,
                      const int senselen,
                      const int flags,
                      int* const nb_sense_ret,
                      const char** const msgaddr);
