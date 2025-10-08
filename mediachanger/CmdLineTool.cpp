/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/CmdLineTool.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::mediachanger::CmdLineTool::CmdLineTool(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream,
  MediaChangerFacade &mc):
  m_in(inStream),
  m_out(outStream),
  m_err(errStream),
  m_mc(mc),
  m_debugBuf(outStream),
  m_dbg(&m_debugBuf) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::mediachanger::CmdLineTool::~CmdLineTool() = default;

//------------------------------------------------------------------------------
// bool2Str
//------------------------------------------------------------------------------
std::string cta::mediachanger::CmdLineTool::bool2Str(const bool value)
  const {
  return value ? "TRUE" : "FALSE";
}
