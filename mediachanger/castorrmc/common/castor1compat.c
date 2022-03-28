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

#include <stage_api.h>

int use_castor2_api() {
  return 1;
}

int stage_setoutbuf(char* buffer, int buflen) {
  (void)buffer;
  (void)buflen;
  return -1;
 }

int stage_seterrbuf(char* buffer, int buflen) {
  (void) buffer;
  (void) buflen;
  return -1;
}

int stage_iowc(int req_type,
                        char t_or_d,
                        u_signed64 flags,
                        int openflags,
                        mode_t openmode,
                        char *hostname,
                        char *pooluser,
                        int nstcp_input,
                        struct stgcat_entry *stcp_input,
                        int *nstcp_output,
                        struct stgcat_entry **stcp_output,
                        int nstpp_input,
                        struct stgpath_entry *stpp_input) {
  (void)req_type;
  (void)t_or_d;
  (void)flags;
  (void)openflags;
  (void)openmode;
  (void)hostname;
  (void)pooluser;
  (void)nstcp_input;
  (void)stcp_input;
  (void)nstcp_output;
  (void)stcp_output;
  (void)nstpp_input;
  (void)stpp_input;
  return -1;
}

int stage_updc_filchg(char *stghost,
                               stage_hsm_t *hsmstruct) {
  (void)stghost;
  (void)hsmstruct;
 return -1; }
