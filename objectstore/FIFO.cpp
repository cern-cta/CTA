/**
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

#include "FIFO.hpp"
#include "ProtcolBuffersAlgorithms.hpp"

const size_t cta::objectstore::FIFO::c_compactionSize = 50;

void cta::objectstore::FIFO::initialize() {
  ObjectOps<serializers::FIFO>::initialize();
  m_payload.set_readpointer(0);
  m_payloadInterpreted = true;
}


std::string cta::objectstore::FIFO::peek() {
  checkPayloadReadable();
  if (m_payload.readpointer() >= (size_t)m_payload.name_size())
    throw FIFOEmpty("In FIFO::Transaction::peek: FIFO empty");
  return m_payload.name(m_payload.readpointer());
}

void cta::objectstore::FIFO::pop() {
  checkPayloadWritable();
  if (m_payload.readpointer() >= (size_t)m_payload.name_size())
    throw FIFOEmpty("In FIFO::Transaction::popAndUnlock: FIFO empty");
  m_payload.set_readpointer(m_payload.readpointer() + 1);
  if ((uint64_t)m_payload.readpointer() > c_compactionSize) {
    compact();
  }
}

void cta::objectstore::FIFO::compact() {
  uint64_t oldReadPointer = m_payload.readpointer();
  uint64_t oldSize = m_payload.name_size();
  // Copy the elements at position oldReadPointer + i to i (squash all)
  // before the read pointer
  for (int i = oldReadPointer; i < m_payload.mutable_name()->size(); i++) {
    *m_payload.mutable_name(i - oldReadPointer) = m_payload.name(i);
  }
  // Shorten the name array by oldReadPointer elements
  for (uint64_t i = 0; i < oldReadPointer; i++) {
    m_payload.mutable_name()->RemoveLast();
  }
  // reset the read pointer
  m_payload.set_readpointer(0);
  // Check the size is as expected
  if ((uint64_t) m_payload.name_size() != oldSize - oldReadPointer) {
    std::stringstream err;
    err << "In FIFO::compactCurrentState: wrong size after compaction: "
        << "oldSize=" << oldSize << " oldReadPointer=" << oldReadPointer
        << " newSize=" << m_payload.name_size();
    throw cta::exception::Exception(err.str());
  }
}


void cta::objectstore::FIFO::push(std::string name) {
  checkPayloadWritable();
  m_payload.add_name(name);
}

void cta::objectstore::FIFO::pushIfNotPresent(std::string name) {
  checkPayloadWritable();
  try {
    serializers::findStringFrom(m_payload.mutable_name(), m_payload.readpointer(),
      name);
  } catch (serializers::NotFound &) {
    m_payload.add_name(name);
  }
}

uint64_t cta::objectstore::FIFO::size() {
  checkPayloadReadable();
  uint64_t ret = m_payload.name_size() - m_payload.readpointer();
  return ret;
}

std::string cta::objectstore::FIFO::dump() {
  checkPayloadReadable();
  std::stringstream ret;
  ret << "<<<< FIFO dump start: " << getNameIfSet() << std::endl
      << "Read pointer=" << m_payload.readpointer() << std::endl
      << "Array size=" << m_payload.name_size() << std::endl;
  for (int i = m_payload.readpointer(); i < m_payload.name_size(); i++) {
    ret << "name[phys=" << i << " ,log=" << i - m_payload.readpointer()
        << "]=" << m_payload.name(i) << std::endl;
  }
  ret << ">>>> FIFO dump end for " << getNameIfSet() << std::endl;
  return ret.str();
}

std::list<std::string> cta::objectstore::FIFO::getContent() {
  checkPayloadReadable();
  std::list<std::string> ret;
  for (int i = m_payload.readpointer(); i < m_payload.name_size(); i++) {
    ret.push_back(m_payload.name(i));
  }
  return ret;
}
