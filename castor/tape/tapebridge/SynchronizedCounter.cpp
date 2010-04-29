/******************************************************************************
 *                castor/tape/tapebridge/SynchronizedCounter.cpp
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

#include "castor/exception/Internal.hpp"
#include "castor/tape/tapebridge/SynchronizedCounter.hpp"
#include "castor/tape/utils/ScopedLock.hpp"
#include "castor/tape/utils/utils.hpp"

#include <sstream>
#include <string.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::SynchronizedCounter::SynchronizedCounter(
  const int32_t count) throw(castor::exception::Exception) : m_count(count) {
  const int rc = pthread_mutex_init(&m_mutex, NULL);

  if(rc) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to create SynchronizedCounter object"
      ": Failed to initialize mutex"
      ": " << sstrerror(rc));
  }

  reset(count);
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::SynchronizedCounter::reset(
  const int32_t count) throw(castor::exception::Exception) {
  utils::ScopedLock scopedLock(m_mutex);

  m_count = count;
}


//-----------------------------------------------------------------------------
// next
//-----------------------------------------------------------------------------
int32_t castor::tape::tapebridge::SynchronizedCounter::next()
  throw(castor::exception::Exception) {

  return next(1);
}


//-----------------------------------------------------------------------------
// next
//-----------------------------------------------------------------------------
int32_t castor::tape::tapebridge::SynchronizedCounter::next(
  const int32_t increment) throw(castor::exception::Exception) {
  utils::ScopedLock scopedLock(m_mutex);

  return m_count += increment;
}
