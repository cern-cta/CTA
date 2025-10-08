/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

namespace cta::threading {

class RWLock;

/**
 * A scoped read lock on an RWLock
 */
class RWLockRdLocker {
public:
  /**
   * Constructor
   *
   * Takes a read lock on the specified read-write lock
   *
   * @param lock The read-write lock on which to take a read-lock
   */
  explicit RWLockRdLocker(RWLock& lock);

  /**
   * Destructor
   *
   * Releases the read lock
   */
  ~RWLockRdLocker();

private:
  /**
   * The read-write lock
   */
  RWLock& m_lock;
};

} // namespace cta::threading
