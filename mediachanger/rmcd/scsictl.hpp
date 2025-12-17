/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1995-2025 CERN
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
