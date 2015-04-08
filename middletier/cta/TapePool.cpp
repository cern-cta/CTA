#include "cta/TapePool.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapePool::TapePool():
  m_nbDrives(0),
  m_nbPartialTapes(0),
  m_creationTime(time(NULL)) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::TapePool::TapePool(
  const std::string &name,
  const uint16_t nbDrives,
  const uint32_t nbPartialTapes,
  const UserIdentity &creator,
  const time_t creationTime,
  const std::string &comment):
  m_name(name),
  m_nbDrives(nbDrives),
  m_nbPartialTapes(nbPartialTapes),
  m_creationTime(creationTime),
  m_creator(creator),
  m_comment(comment) {
}

//------------------------------------------------------------------------------
// operator<
//------------------------------------------------------------------------------
bool cta::TapePool::operator<(const TapePool &rhs) const throw() {
  return m_name < rhs.m_name;
}

//------------------------------------------------------------------------------
// getName
//------------------------------------------------------------------------------
const std::string &cta::TapePool::getName() const throw() {
  return m_name;
}

//------------------------------------------------------------------------------
// getNbDrives
//------------------------------------------------------------------------------
uint16_t cta::TapePool::getNbDrives() const throw() {
  return m_nbDrives;
}

//------------------------------------------------------------------------------
// getNbPartialTapes
//------------------------------------------------------------------------------
uint32_t cta::TapePool::getNbPartialTapes() const throw() {
  return m_nbPartialTapes;
}

//------------------------------------------------------------------------------
// getCreationTime
//------------------------------------------------------------------------------
time_t cta::TapePool::getCreationTime() const throw() {
  return m_creationTime;
}

//------------------------------------------------------------------------------
// getCreator
//------------------------------------------------------------------------------
const cta::UserIdentity &cta::TapePool::getCreator()
  const throw() {
  return m_creator;
}

//------------------------------------------------------------------------------
// getComment
//------------------------------------------------------------------------------
const std::string &cta::TapePool::getComment() const throw() {
  return m_comment;
}
