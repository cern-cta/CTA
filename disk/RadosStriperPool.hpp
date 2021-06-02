/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#pragma once

#include "common/threading/Mutex.hpp"

#include <map>
#include <memory>
#include <radosstriper/libradosstriper.hpp>

namespace cta {
namespace disk {    
/**
 * Utility singleton managing the rados stripers connections by name.
 * The destructor will implicitly release the pool connections.
 */
class RadosStriperPool{
public:

  /** constructor */
  RadosStriperPool() : m_maxStriperIdx(0), m_striperIdx(0) {};

  /**
   * Get pointer to a connection to the rados user (or one from the cache).
   * This function throws exceptions in case of problem.
   */
  libradosstriper::RadosStriper * throwingGetStriper(const std::string & userAtPool);

  /**
   * Get pointer to a connection to the rados user (or one from the cache).
   * This function returns NULL in case of problem.
   */
  libradosstriper::RadosStriper * getStriper(const std::string & userAtPool);

  /**
   * Clear the map of all connections
   */
  void disconnectAll();

  /** Destructor that will delete the held objects (needed in SLC6, see
   * m_stripers declaration. */
  virtual ~RadosStriperPool();

private:

  /// Accessor to next striper pool index
  unsigned int getStriperIdxAndIncrease();

private:

  // We use a map of pointers instead of maps of unique_ptr who do not work in
  // gcc 4.4 (in SLC 6)
  typedef std::map<std::string, libradosstriper::RadosStriper *> StriperDict;
  /// striper pool
  std::vector<StriperDict> m_stripers;
  /// mutex protecting the striper pool
  cta::threading::Mutex m_mutex;
  /// size of the Striper pool
  unsigned int m_maxStriperIdx;
  /// index of current striper pool to be used
  unsigned int m_striperIdx;
};

} // namespace disk
} // namespace cta
