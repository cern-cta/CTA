/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "tapeserver/readtp/TapeFseqRange.hpp"
#include "common/exception/InvalidArgument.hpp"

#include <getopt.h>
#include <ostream>

#include <string.h>

namespace cta {
namespace tapeserver {
namespace readtp {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFseqRange::TapeFseqRange() throw() {

  reset();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeFseqRange::TapeFseqRange(const uint32_t lower, const uint32_t upper)  {

  reset(lower, upper);
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void TapeFseqRange::reset() throw() {
  m_isEmpty = true;
  m_lower   = 0; // Ignored
  m_upper   = 0; // Ignored
}


//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void TapeFseqRange::reset(const uint32_t lower,
  const uint32_t upper)  {

  if(lower == 0) {
    exception::InvalidArgument ex;

    ex.getMessage() << "Lower boundary must not be zero";
    throw ex;
  }

  // If the upper boundary is not 0 meaning infinity and the lower boundary is
  // greater than the upper boundary
  if(upper != 0 && lower > upper) {
    exception::InvalidArgument ex;

    ex.getMessage() <<
      "Lower boundary must be less than or equal to the upper boundary"
      ": lower=" << lower << " upper=" << upper;

    throw ex;
  }

  m_isEmpty = false;
  m_lower   = lower;
  m_upper   = upper;
}


//------------------------------------------------------------------------------
// isEmpty
//------------------------------------------------------------------------------
bool TapeFseqRange::isEmpty() const throw() {
  return m_isEmpty;
}

//------------------------------------------------------------------------------
// lower
//------------------------------------------------------------------------------
uint32_t TapeFseqRange::lower() const throw() {

  return m_isEmpty ? 0 : m_lower;
}

//------------------------------------------------------------------------------
// upper
//------------------------------------------------------------------------------
uint32_t TapeFseqRange::upper() const throw() {

  return m_isEmpty ? 0 : m_upper;
}

//------------------------------------------------------------------------------
// size
//------------------------------------------------------------------------------
uint32_t TapeFseqRange::size() const throw() {

  return m_isEmpty || m_upper == 0 ? 0 : m_upper - m_lower + 1;
}

} // namespace readtp
} // namespace tapeserver
} // namespace cta

//------------------------------------------------------------------------------
// ostream << operator for cta::tapeserver::readtp::TapeFseqRange
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os,
  const cta::tapeserver::readtp::TapeFseqRange &value) {

  if(value.isEmpty()) {
    os << "EMPTY";
  } else {
    uint32_t lower = 0;
    uint32_t upper = 0;

    try {
      lower = value.lower();
      upper = value.upper();

      os << lower << "-";

      // An upper value of 0 means END of tape
      if(upper !=0) {
        os << upper;
      } else {
        os << "END";
      }
    } catch(cta::exception::Exception &ex) {
      os << "ERROR";
    }
  }

  return os;
}
