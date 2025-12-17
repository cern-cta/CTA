/*
 * SPDX-FileCopyrightText: 1995 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

constexpr int MAXSENSE = 255;

// Value of the 'flags' field for send_cmd function
enum SendCmdFlags {
  SCSI_IN = 1,
  SCSI_OUT = 2,
  SCSI_IN_OUT = 4,
  SCSI_NONE = 8,
  SCSI_SEL_WITH_ATN = 16,
  SCSI_SYNC = 32,
  SCSI_WIDE = 64
};
