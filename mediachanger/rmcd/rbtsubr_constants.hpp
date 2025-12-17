/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

enum RbtSubrReturnCode {
  RBT_OK = 0,           //!< Ok or error should be ignored
  RBT_NORETRY = 1,      //!< Unrecoverable error (just log it)
  RBT_SLOW_RETRY = 2,   //!< Should release drive & retry in 600 seconds
  RBT_FAST_RETRY = 3,   //!< Should retry in 60 seconds
  RBT_DMNT_FORCE = 4,   //!< Should do first a demount force
  RBT_CONF_DRV_DN = 5,  //!< Should configure the drive down
  RBT_OMSG_NORTRY = 6,  //!< Should send a msg to operator and exit
  RBT_OMSG_SLOW_R = 7,  //!< Ops msg (nowait) + release drive + slow retry
  RBT_OMSGR = 8,        //!< Should send a msg to operator and wait
  RBT_UNLD_DMNT = 9     //!< Should unload the tape and retry demount
};
