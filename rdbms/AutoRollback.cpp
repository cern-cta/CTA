/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "rdbms/AutoRollback.hpp"
#include "rdbms/Conn.hpp"

namespace cta::rdbms {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
AutoRollback::AutoRollback(Conn &conn):
  m_cancelled(false),
  m_conn(conn) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
AutoRollback::~AutoRollback() {
  try {
    if(!m_cancelled) {
      m_conn.rollback();
    }
  } catch(...) {
    // Prevent destructor from throwing
  }
}

//------------------------------------------------------------------------------
// cancel
//------------------------------------------------------------------------------
void AutoRollback::cancel() {
  m_cancelled = true;
}

} // namespace cta::rdbms
