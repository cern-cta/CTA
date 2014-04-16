/******************************************************************************
 *                      AtomicCounter.hpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/


#pragma once

#include "castor/tape/tapeserver/threading/Threading.hpp"

namespace castor {
namespace tape {
namespace threading {
/**
* A helper class managing a thread safe message counter (we need it thread
* safe as the ClientInterface class will be used by both the getting of
* the work to be done and the reporting of the completed work, in parallel
*/
template <class T> struct AtomicCounter{
  AtomicCounter(T init): m_val(init) {};
      T operator ++ () {
        threading::MutexLocker ml(&m_mutex);
        return ++m_val;
      }
      T operator ++ (int) {
        threading::MutexLocker ml(&m_mutex);
        return m_val++;
      }
      T operator -- () {
        threading::MutexLocker ml(&m_mutex);
        return --m_val;
      }
      operator T() const {
        threading::MutexLocker ml(&m_mutex);
        return m_val;
      }
     void reset()  {
        threading::MutexLocker ml(&m_mutex);
        m_val=0;
      }
    private:
      T m_val;
      mutable threading::Mutex m_mutex;
};

}}}

