/******************************************************************************
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

#include "castor/server/Threading.hpp"

namespace castor {
namespace server {
/**
* A helper class managing a thread safe message counter
 * When C++11 will be used, just delete it to use std::atomic
*/
template <class T> struct AtomicCounter{
  AtomicCounter(T init = 0): m_val(init) {};
      T operator ++ () {
        MutexLocker ml(&m_mutex);
        return ++m_val;
      }
      T operator ++ (int) {
        MutexLocker ml(&m_mutex);
        return m_val++;
      }
      T operator -- () {
        MutexLocker ml(&m_mutex);
        // Store the return value in a local variable
        // instead of returning --m_val, which is a reference
        // to the value. If this reference to value gets cast into value
        // and evaluated after the destruction of ml, we have a race 
        // condition (which we seemed to experience under rare circumstances).
        T ret = --m_val;
        return ret;
      }
      operator T() const {
        MutexLocker ml(&m_mutex);
        return m_val;
      }
      
     T getAndReset(){
       MutexLocker ml(&m_mutex);
        T old =m_val;
        m_val=0;
        return old;
     }
    private:
      T m_val;
      mutable Mutex m_mutex;
};

}}

