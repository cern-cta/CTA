/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

namespace castor::tape {

namespace tapeserver::drive {
class DriveInterface;
}

namespace tapeFile {

/**
  * Class that labels a tape.  This class assumes the tape to be labeled is
  * already mounted and rewound.
  */
class LabelSession {
public:
  /**
    * Constructor of the LabelSession. This constructor will label the
    * tape in the specified drive.  This constructor assumes the tape to be
    * labeled is already mounted and rewound.
    * @param drive: drive object to which we bind the session
    * @param vid: volume name of the tape we would like to read from
    * @param lbp The boolean variable for logical block protection mode.
    *            If it is true than the label will be written with LBP or
    *            without otherwise.
    *
    */
  static void label(tapeserver::drive::DriveInterface *drive, const std::string &vid, const bool lbp);
};

}}  // namespace castor::tape::tapeFile
