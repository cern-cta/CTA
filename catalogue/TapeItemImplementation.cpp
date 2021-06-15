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

#include "catalogue/TapeItemWritten.hpp"
#include "catalogue/TapeItemWrittenPointer.hpp"
#include <iostream>

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
TapeItemWritten::TapeItemWritten() :
  fSeq(0) {
}

//------------------------------------------------------------------------------
// operator==
//------------------------------------------------------------------------------
bool TapeItemWritten::operator==(const TapeItemWritten &rhs) const {
  return 
      fSeq == rhs.fSeq && 
      vid == rhs.vid; 
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool TapeItemWritten::operator<(const TapeItemWritten &rhs) const {
  return fSeq < rhs.fSeq;
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream &operator<<(std::ostream &os, const TapeItemWritten &obj) {
  os <<
  "{"
  "vid=" << obj.vid << ","
  "fSeq=" << obj.fSeq << ","
  "tapeDrive=" << obj.tapeDrive <<
  "}";
  return os;
}

bool operator<(const TapeItemWrittenPointer& a, const TapeItemWrittenPointer& b) { return *a < *b; }


} // namespace catalogue
} // namespace cta