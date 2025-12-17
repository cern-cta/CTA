/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <common/dataStructures/DriveStatus.hpp>

#include "cta_admin.pb.h"

namespace cta::admin {

inline common::dataStructures::DriveStatus ProtobufToDriveStatus(DriveLsItem::DriveStatus driveStatus) {
  using namespace common::dataStructures;

  switch (driveStatus) {
    case DriveLsItem::DOWN:
      return DriveStatus::Down;
    case DriveLsItem::UP:
      return DriveStatus::Up;
    case DriveLsItem::PROBING:
      return DriveStatus::Probing;
    case DriveLsItem::STARTING:
      return DriveStatus::Starting;
    case DriveLsItem::MOUNTING:
      return DriveStatus::Mounting;
    case DriveLsItem::TRANSFERRING:
      return DriveStatus::Transferring;
    case DriveLsItem::UNLOADING:
      return DriveStatus::Unloading;
    case DriveLsItem::UNMOUNTING:
      return DriveStatus::Unmounting;
    case DriveLsItem::DRAINING_TO_DISK:
      return DriveStatus::DrainingToDisk;
    case DriveLsItem::CLEANING_UP:
      return DriveStatus::CleaningUp;
    case DriveLsItem::SHUTDOWN:
      return DriveStatus::Shutdown;
    case DriveLsItem::UNKNOWN_DRIVE_STATUS:
    default:
      return DriveStatus::Unknown;
  }
}

inline DriveLsItem::DriveStatus DriveStatusToProtobuf(common::dataStructures::DriveStatus driveStatus) {
  using namespace common::dataStructures;

  switch (driveStatus) {
    case DriveStatus::Down:
      return DriveLsItem::DOWN;
    case DriveStatus::Up:
      return DriveLsItem::UP;
    case DriveStatus::Probing:
      return DriveLsItem::PROBING;
    case DriveStatus::Starting:
      return DriveLsItem::STARTING;
    case DriveStatus::Mounting:
      return DriveLsItem::MOUNTING;
    case DriveStatus::Transferring:
      return DriveLsItem::TRANSFERRING;
    case DriveStatus::Unloading:
      return DriveLsItem::UNLOADING;
    case DriveStatus::Unmounting:
      return DriveLsItem::UNMOUNTING;
    case DriveStatus::DrainingToDisk:
      return DriveLsItem::DRAINING_TO_DISK;
    case DriveStatus::CleaningUp:
      return DriveLsItem::CLEANING_UP;
    case DriveStatus::Shutdown:
      return DriveLsItem::SHUTDOWN;
    case DriveStatus::Unknown:
      return DriveLsItem::UNKNOWN_DRIVE_STATUS;
  }
  return DriveLsItem::UNKNOWN_DRIVE_STATUS;
}

}  // namespace cta::admin
