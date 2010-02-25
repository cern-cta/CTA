/******************************************************************************
 *                castor/tape/tapeserver/SynchronizedCounter.cpp
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
#include "castor/tape/tapeserver/Constants.hpp"
#include "castor/tape/tapeserver/SynchronizedCounter.hpp"
#include "castor/tape/utils/utils.hpp"

#include <sstream>
#include <string.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapeserver::SynchronizedCounter::SynchronizedCounter(
  const int32_t count) throw(castor::exception::Exception) : m_count(count) {
  const int rc = pthread_mutex_init(&m_mutex, NULL);

  if(rc) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to initialize mutex: " << sstrerror(rc));
  }

  reset(count);
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::tape::tapeserver::SynchronizedCounter::reset(
  const int32_t count) throw(castor::exception::Exception) {
  int rc = 0;

  rc = pthread_mutex_lock(&m_mutex);
  if(rc) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to lock counter mutex: " << sstrerror(rc));
  }

  m_count = count;

  rc = pthread_mutex_unlock(&m_mutex);
  if(rc) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to unlock counter mutex: " << sstrerror(rc));
  }
}


//-----------------------------------------------------------------------------
// next
//-----------------------------------------------------------------------------
int32_t castor::tape::tapeserver::SynchronizedCounter::next()
  throw(castor::exception::Exception) {

  return next(1);
}


//-----------------------------------------------------------------------------
// next
//-----------------------------------------------------------------------------
int32_t castor::tape::tapeserver::SynchronizedCounter::next(
  const int32_t increment) throw(castor::exception::Exception) {

  int rc = 0;

  rc = pthread_mutex_lock(&m_mutex);
  if(rc) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to lock counter mutex: " << sstrerror(rc));
  }

  m_count += increment;
  const int32_t result = m_count;

  rc = pthread_mutex_unlock(&m_mutex);
  if(rc) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to unlock counter mutex: " << sstrerror(rc));
  }

  return(result);
}
