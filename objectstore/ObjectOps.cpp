/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ObjectOps.hpp"

namespace cta::objectstore {

ObjectOpsBase::~ObjectOpsBase()  {
  if (m_lockForSubObject) m_lockForSubObject->dereferenceSubObject(*this);
}

} // namespace cta::objectstore
