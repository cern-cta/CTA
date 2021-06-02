/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2001-2021 CERN
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

#include "marshall.h"
#include "rmc_marshall_element.h"

int rmc_marshall_element (
	char **const sbpp,
	const struct smc_element_info *const element_info)
{
	char *sbp = *sbpp;

	marshall_WORD (sbp, element_info->element_address);
	marshall_BYTE (sbp, element_info->element_type);
	marshall_BYTE (sbp, element_info->state);
	marshall_BYTE (sbp, element_info->asc);
	marshall_BYTE (sbp, element_info->ascq);
	marshall_BYTE (sbp, element_info->flags);
	marshall_WORD (sbp, element_info->source_address);
	marshall_STRING (sbp, element_info->name);
	*sbpp = sbp;
	return (0);
}
