/******************************************************************************
 *                 castor/tape/tpcp/TapeFseqRange.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TPCP_TAPEFSEQRANGE_HPP
#define CASTOR_TAPE_TPCP_TAPEFSEQRANGE_HPP 1

#include <ostream>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * A range of tape file sequence numbers specified by an inclusive upper and
 * lower set of bounds.
 */
struct TapeFseqRange {
  uint32_t lower;
  uint32_t upper;
};

} // namespace tpcp
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TPCP_TAPEFSEQRANGE_HPP
