/******************************************************************************
 *                castor/tape/aggregator/SynchronizedCounter.cpp
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
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/SynchronizedCounter.hpp"
#include "castor/tape/utils/utils.hpp"

#include <sstream>
#include <string.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::SynchronizedCounter::SynchronizedCounter(
  const int32_t count) throw(castor::exception::Exception) : m_count(count) {
  const int rc = pthread_mutex_init(&m_mutex, NULL);

  if(rc) {
    char strerrbuf[STRERRORBUFLEN];
    utils::setBytes(strerrbuf, '\0');
    strerror_r(rc, strerrbuf, sizeof(strerrbuf));
    strerrbuf[sizeof(strerrbuf)-1] = '\0';

    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to initialize mutex: " << strerrbuf);
  }

  reset(count);
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::tape::aggregator::SynchronizedCounter::reset(
  const int32_t count) throw(castor::exception::Exception) {
  int rc = 0;

  rc = pthread_mutex_lock(&m_mutex);
  if(rc) {
    char strerrbuf[STRERRORBUFLEN];
    utils::setBytes(strerrbuf, '\0');
    strerror_r(rc, strerrbuf, sizeof(strerrbuf));
    strerrbuf[sizeof(strerrbuf)-1] = '\0';

    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to lock counter mutex: " << strerrbuf);
  }

  m_count = count;

  rc = pthread_mutex_unlock(&m_mutex);
  if(rc) {
    char strerrbuf[STRERRORBUFLEN];
    utils::setBytes(strerrbuf, '\0');
    strerror_r(rc, strerrbuf, sizeof(strerrbuf));
    strerrbuf[sizeof(strerrbuf)-1] = '\0';

    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to unlock counter mutex: " << strerrbuf);
  }
}


//-----------------------------------------------------------------------------
// next
//-----------------------------------------------------------------------------
int32_t castor::tape::aggregator::SynchronizedCounter::next()
  throw(castor::exception::Exception) {

  return next(1);
}


//-----------------------------------------------------------------------------
// next
//-----------------------------------------------------------------------------
int32_t castor::tape::aggregator::SynchronizedCounter::next(
  const int32_t increment) throw(castor::exception::Exception) {

  int rc = 0;

  rc = pthread_mutex_lock(&m_mutex);
  if(rc) {
    char strerrbuf[STRERRORBUFLEN];
    utils::setBytes(strerrbuf, '\0');
    strerror_r(rc, strerrbuf, sizeof(strerrbuf));
    strerrbuf[sizeof(strerrbuf)-1] = '\0';

    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to lock counter mutex: " << strerrbuf);
  }

  const int32_t result = ++m_count;

  rc = pthread_mutex_unlock(&m_mutex);
  if(rc) {
    char strerrbuf[STRERRORBUFLEN];
    utils::setBytes(strerrbuf, '\0');
    strerror_r(rc, strerrbuf, sizeof(strerrbuf));
    strerrbuf[sizeof(strerrbuf)-1] = '\0';

    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to unlock counter mutex: " << strerrbuf);
  }

  return(result);
}
