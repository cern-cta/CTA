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

#include <utility>      // std::move
#include <type_traits>  // std::is_trivially_destructible
#include <stdexcept>    // std::logic_error

namespace cta {

struct nullopt_t {
    nullopt_t() {}
    friend std::ostream& operator<<(std::ostream& os, const nullopt_t &nopt)
    {
        return os;
    }
};

extern const nullopt_t nullopt;

template <class T> class optional
{
public:
  //constructors
  optional();
  optional(nullopt_t);
  optional(const optional& other);
  optional(optional&& other);
  optional(const T& value);
  optional(T&& value);
  //destructor
  ~optional();
  //assignment
  optional& operator=(nullopt_t);
  optional& operator=(optional& other);
  optional& operator=(const optional& other);
  optional& operator=(optional&& other);
  template<class U> optional& operator=(U&& value);
  //accessing the value
  const T* operator->() const;
  T* operator->();
  const T& operator*() const&;
  T& operator*() &;
  const T&& operator*() const&&;
  T&& operator*() &&;
  //bool operator
  explicit operator bool() const;
  //returns the contained value
  T& value() &;
  const T & value() const &;
  T&& value() &&;
  const T&& value() const &&;
  //swap
  void swap(optional& other);
private:
  bool is_set;
  T val;
};

//constructors
template <class T> optional<T>::optional(): is_set(false) {
}
template <class T> optional<T>::optional(nullopt_t): is_set(false) {
}
template <class T> optional<T>::optional(const optional& other) {
  if(other.is_set) {
    is_set=true;
    val=other.val;
  }
  else {
    is_set=false;
  }
}
template <class T> optional<T>::optional(optional&& other) {
  if(other.is_set) {
    is_set=true;
    val=std::move(other.val);
  }
  else {
    is_set=false;
  }
}
template <class T> optional<T>::optional(const T& v): is_set(true), val(v) {
}
template <class T> optional<T>::optional(T&& v): is_set(true), val(std::move(v)) {
}
//destructor
template <class T> optional<T>::~optional() {
}
//assignment
template <class T> optional<T>& optional<T>::operator=(nullopt_t) {
  if(is_set) {
    is_set=false;
  }
  return *this;
}
template <class T> optional<T>& optional<T>::operator=(const optional& other) {
  if(!is_set && !other.is_set) {
  }
  else if(is_set && !other.is_set) {
    is_set=false;
  }
  else { //if(other.is_set)
    is_set=true;
    val=other.val;
  }
  return *this;
}
template <class T> optional<T>& optional<T>::operator=(optional& other) {
  if(!is_set && !other.is_set) {
  }
  else if(is_set && !other.is_set) {
    is_set=false;
  }
  else { //if(other.is_set)
    is_set=true;
    val=other.val;
  }
  return *this;
}
template <class T> optional<T>& optional<T>::operator=(optional&& other) {
  if(!is_set && !other.is_set) {
  }
  else if(is_set && !other.is_set) {
    is_set=false;
  }
  else { //if(other.is_set)
    is_set=true;
    val=std::move(other.val);
  }
  return *this;
}
template <class T> template<class U> optional<T>& optional<T>::operator=(U&& v) {
  if(!is_set) {
    is_set=true;
    val=v;
  }
  else {
    val=v;
  }
  return *this;
}
//accessing the value
template <class T> const T* optional<T>::operator->() const {
  return &val;
}
template <class T> T* optional<T>::operator->() {
  return &val;
}
template <class T> const T& optional<T>::operator*() const& {
  return val;
}
template <class T> T& optional<T>::operator*() & {
  return val;
}
template <class T> const T&& optional<T>::operator*() const&& {
  return val;
}
template <class T> T&& optional<T>::operator*() && {
  return val;
}
//bool operator
template <class T> optional<T>::operator bool() const {
  return is_set;
}
//returns the contained value
template <class T> T& optional<T>::value() & {
  if(is_set) {
    return val;
  }
  throw std::logic_error("");
}
template <class T> const T & optional<T>::value() const & {
  if(is_set) {
    return val;
  }
  throw std::logic_error("");
}
template <class T> T&& optional<T>::value() && {
  if(is_set) {
    return std::move(val);
  }
  throw std::logic_error("");
}
template <class T> const T&& optional<T>::value() const && {
  if(is_set) {
    return std::move(val);
  }
  throw std::logic_error("");
}
//swap
template <class T> void optional<T>::swap(optional& other) {
  if(!is_set && !other.is_set) {
  }
  else if(is_set && !other.is_set) {
    is_set=false;
    other.is_set=true;
    other.val=std::move(val);
  }
  else if(!is_set && other.is_set) {
    is_set=true;
    other.is_set=false;
    val=std::move(other.val);
  }
  else { //both are set
    using std::swap;
    swap(**this, *other);
  }
}

//compare two optional objects		
template<class T> bool operator==(const optional<T>& lhs, const optional<T>& rhs) {
  if(bool(lhs)!=bool(rhs)) {
    return false;
  }
  else if(bool(lhs) == false) {
    return true;
  }
  return *lhs == *rhs;
}
template<class T> bool operator!=(const optional<T>& lhs, const optional<T>& rhs) {
  return !(lhs == rhs);
}
template<class T> bool operator<(const optional<T>& lhs, const optional<T>& rhs) {
  if(bool(rhs) == false) {
    return false;
  }
  else if(bool(lhs) == false) {
    return true;
  }
  else {
    return *lhs < *rhs;
  }
}
template<class T> bool operator<=(const optional<T>& lhs, const optional<T>& rhs) {
  return !(rhs < lhs);
}
template<class T> bool operator>(const optional<T>& lhs, const optional<T>& rhs) {
  return rhs < lhs;
}
template<class T> bool operator>=(const optional<T>& lhs, const optional<T>& rhs) {
  return !(lhs < rhs);
}
//compare an optional object with a nullopt
template<class T> bool operator==(const optional<T>& opt, nullopt_t) {
  return !opt;
}
template<class T> bool operator==(nullopt_t, const optional<T>& opt) {
  return !opt;
}
template<class T> bool operator!=(const optional<T>& opt, nullopt_t) {
  return bool(opt);
}
template<class T> bool operator!=(nullopt_t, const optional<T>& opt) {
  return bool(opt);
}
template<class T> bool operator<(const optional<T>& opt, nullopt_t) {
  return false;
}
template<class T> bool operator<(nullopt_t, const optional<T>& opt) {
  return bool(opt);
}
template<class T> bool operator<=(const optional<T>& opt, nullopt_t) {
  return !opt;
}
template<class T> bool operator<=(nullopt_t, const optional<T>& opt) {
  return true;
}
template<class T> bool operator>(const optional<T>& opt, nullopt_t) {
  return bool(opt);
}
template<class T> bool operator>(nullopt_t, const optional<T>& opt) {
  return false;
}
template<class T> bool operator>=(const optional<T>& opt, nullopt_t) {
  return true;
}
template<class T> bool operator>=(nullopt_t, const optional<T>& opt) {
  return !opt;
}
//compare an optional object with a T		
template<class T> bool operator==(const optional<T>& opt, const T& value) {
  return bool(opt) ? *opt == value : false;
}
template<class T> bool operator==(const T& value, const optional<T>& opt) {
  return bool(opt) ? value == *opt : false;
}
template<class T> bool operator!=(const optional<T>& opt, const T& value) {
  return bool(opt) ? !(*opt == value) : true;
}
template<class T> bool operator!=(const T& value, const optional<T>& opt) {
  return bool(opt) ? !(value == *opt) : true;
}
template<class T> bool operator<(const optional<T>& opt, const T& value) {
  return bool(opt) ? *opt < value  : true;
}
template<class T> bool operator<(const T& value, const optional<T>& opt) {
  return bool(opt) ? value < *opt  : false;
}
template<class T> bool operator<=(const optional<T>& opt, const T& value) {
  return !(opt > value);
}
template<class T> bool operator<=(const T& value, const optional<T>& opt) {
  return !(value > opt);
}
template<class T> bool operator>(const optional<T>& opt, const T& value) {
  return bool(opt) ? value < *opt  : false;
}
template<class T> bool operator>(const T& value, const optional<T>& opt) {
  return bool(opt) ? *opt < value  : true;
}
template<class T> bool operator>=(const optional<T>& opt, const T& value) {
  return !(opt < value);
}
template<class T> bool operator>=(const T& value, const optional<T>& opt) {
  return !(value < opt);
}
//std::swap
template<class T> void swap(optional<T>& lhs, optional<T>& rhs) {
  lhs.swap(rhs);
}
}
