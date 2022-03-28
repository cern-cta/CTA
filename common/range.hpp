/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <limits>
#include "common/exception/Exception.hpp"

namespace cta {
template <typename T>
class range: public std::initializer_list<T> {
public:
  range (T begin, T end, T step=1): m_begin(begin), m_end(end), m_step(step) {}
  range (T end): range(0, end) {}

  class iterator {
  private:
    friend range;
    enum class direction: bool { forward, backward };
    iterator(T begin, T end, T step, iterator::direction dir): m_val(begin), m_limit(end), m_step(step), m_dir(dir) {}
  public:
    CTA_GENERATE_EXCEPTION_CLASS(DereferencingPastEnd);
    T operator*() { if (m_val == m_limit) throw DereferencingPastEnd("In range::operator*(): dereferencing out of bounds"); return m_val; }
    iterator & operator++() { doInc(); return *this; }
    iterator operator++(int) { iterator ret(*this); doInc(); return ret; }
  private:
    void doInc() { 
      switch (m_dir) { 
        // Increment/decrement variable, preventing over/underflow, and overshooting the limit.
        case direction::forward:
          // Prevent overflow
          if (m_step > std::numeric_limits<T>::max() - m_val) {
            m_val=m_limit;
          } else {
            m_val+=m_step;
            // Prevent overshoot.
            if (m_val > m_limit)
              m_val=m_limit;
          }
          break;
        case direction::backward: 
          // Prevent underflow
          if (m_step > m_val - std::numeric_limits<T>::max() ) {
            m_val=m_limit;
          } else {
            m_val-=m_step;
            if (m_val < m_limit) 
              m_val = m_limit; 
          }
          break;
      }
    }
  public:
    bool operator==(const iterator& other) { checkIteratorsComparable(other); return m_val == other.m_val; }
    bool operator!=(const iterator& other) { checkIteratorsComparable(other); return m_val != other.m_val; }
    CTA_GENERATE_EXCEPTION_CLASS(NonComparableIterators);
  private:
    void checkIteratorsComparable(const iterator & other) const {
      if (m_limit != other.m_limit || m_step != other.m_step || m_dir != other.m_dir)
        throw NonComparableIterators("In range::checkIteratorsComparable(): comparing iterators from different ranges.");
    }
    
    T m_val;
    const T m_limit, m_step;
    direction m_dir;
  };

  iterator cbegin() const {
    return iterator(m_begin, m_end, m_step, m_begin<=m_end?iterator::direction::forward:iterator::direction::backward);
  }

  iterator cend() const {
    return iterator(m_end, m_end, m_step, m_begin<=m_end?iterator::direction::forward:iterator::direction::backward);
  }
  
  iterator begin() const { return cbegin(); }
  iterator end() const { return cend(); }
private:
  const T m_begin;
  const T m_end;
  const T m_step;
};
} // namesapce cta
