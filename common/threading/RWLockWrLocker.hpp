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
class RWLockWrLocker {
public:
  /**
   * Constructor
   *
   * Takes a write lock on the specified read-write lock
   *
   * @param lock The read-write lock on which to take a read-lock
   */
  explicit RWLockWrLocker(RWLock& lock);

  /**
   * Destructor
   *
   * Releases the write lock
   */
  ~RWLockWrLocker();

private:
  /**
   * The read-write lock.
   */
  RWLock& m_lock;
};

} // namespace cta::threading
