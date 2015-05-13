/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "ObjectOps.hpp"
#include "Agent.hpp"
#include "utils/exception/Exception.hpp"
#include <cxxabi.h>

namespace cta { namespace objectstore {

class FIFO: public ObjectOps<serializers::FIFO> {
public:
  FIFO(Backend & os): ObjectOps<serializers::FIFO>(os) {}
  FIFO(const std::string & name, Backend & os):
  ObjectOps<serializers::FIFO>(os, name) {}
 
  void initialize();
  
  class FIFOEmpty: public cta::exception::Exception {
  public:
    FIFOEmpty(const std::string & context): cta::exception::Exception(context) {}
  };
  
  std::string peek(); 

  void pop();
  
  void push(std::string name);
  
  void pushIfNotPresent (std::string name);
  
  std::string dump();
  
  uint64_t size();
  
  std::list<std::string> getContent();

private:
  
  void compact();
  static const size_t c_compactionSize;
};

}}