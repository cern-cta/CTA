/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/threading/Mutex.hpp"

#include <map>
#include <memory>
#include <radosstriper/libradosstriper.hpp>

namespace cta::disk {

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
   * This function returns nullptr in case of problem.
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

} // namespace cta::disk
