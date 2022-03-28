/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1998-2022 CERN
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

#include <string.h>
#include <ctype.h>
#include "spectra_like_libs.h"

#define N_LIBS (sizeof(spectra_like_libs) / sizeof(spectra_like_libs[0]))

struct vendor_product_id_pair {
  char vendor_id[9];
  char product_id[17];
};

static struct vendor_product_id_pair const spectra_like_libs[] = {
  /* To match all products of a specific vendor, set the product_id as empty string */
  {"SPECTRA", ""       }, /*Match all SPECTRA libraries */
  {"IBM"    , "3573-TL"}  /*Match IBM libraries, model 3573-TL*/
};

void trim_trailing_spaces(char * str) {
    int pos;
    pos = strlen(str) - 1;
    for (; pos >= 0 && isspace(str[pos]); pos--) {
        str[pos] = '\0';
    }
}

int is_library_spectra_like(const struct robot_info *const robot_info) {
    int isSpectraLike;
    char vendorId[9];
    char productId[17];
    unsigned int i;

    memcpy (vendorId, robot_info->inquiry, 8);
    memcpy (productId, robot_info->inquiry + 8, 16);
    vendorId[8] = '\0';
    productId[16] = '\0';

    /* Trim any whitespaces in excess */
    trim_trailing_spaces(vendorId);
    trim_trailing_spaces(productId);

    /* find if the vendor (and product) are in the list 'spectra_like_libs' */
    isSpectraLike = 0;
    for (i=0; i < N_LIBS && !isSpectraLike; i++) {
        if (strcasecmp(vendorId, spectra_like_libs[i].vendor_id) == 0) {
            if (
                spectra_like_libs[i].product_id[0] == '\0'
                || strcasecmp(productId, spectra_like_libs[i].product_id) == 0
            ) {
                isSpectraLike = 1;
            }
        }
    }

    return isSpectraLike;
}
