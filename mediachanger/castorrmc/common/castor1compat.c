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
