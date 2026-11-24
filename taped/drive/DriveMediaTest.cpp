/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DriveGeneric.hpp"
#include "taped/system/Wrapper.hpp"

#include <chrono>
#include <cstring>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <memory>
#include <thread>

namespace cta::tape::drive {
namespace {

using testing::_;
using testing::An;
using testing::Return;
using namespace std::chrono_literals;

// Script the actual ioctl boundary, including the driver's cached status, without replacing DriveGeneric logic.
class DriveMediaTest : public testing::Test {
protected:
  testing::StrictMock<System::mockWrapper> m_wrapper;
  std::unique_ptr<DriveLTO> m_drive;
  unsigned m_opens = 0;
  unsigned m_closes = 0;

  void SetUp() override {
    EXPECT_CALL(m_wrapper, open(_, _)).WillRepeatedly([&](const char*, int flags) {
      EXPECT_EQ(O_RDWR | O_NONBLOCK, flags);
      ++m_opens;
      return 7;
    });
    EXPECT_CALL(m_wrapper, close(7)).WillRepeatedly([&](int) {
      ++m_closes;
      return 0;
    });
    SCSI::DeviceInfo device {};
    device.nst_dev = "/dev/nst-test";
    m_drive = std::make_unique<DriveLTO>(device, m_wrapper);
  }

  static int sense(sg_io_hdr_t* command, uint8_t key, uint8_t asc, uint8_t ascq, bool descriptor = false) {
    command->status = SCSI::Status::CHECK_CONDITION;
    command->driver_status = SCSI::DriverStatus::SENSE;
    std::memset(command->sbp, 0, command->mx_sb_len);
    command->sb_len_wr = descriptor ? 8 : 18;
    command->sbp[0] = descriptor ? 0x72 : 0x70;
    command->sbp[descriptor ? 1 : 2] = key;
    command->sbp[descriptor ? 2 : 12] = asc;
    command->sbp[descriptor ? 3 : 13] = ascq;
    command->sbp[7] = descriptor ? 0 : 10;
    return 0;
  }

  void expectStatus(bool online) {
    EXPECT_CALL(m_wrapper, ioctl(7, MTIOCGET, An<mtget*>())).WillOnce([=](int, unsigned long, mtget* status) {
      *status = {};
      status->mt_gstat = online ? GMT_ONLINE(0xffffffff) : GMT_DR_OPEN(0xffffffff);
      return 0;
    });
  }

  void expectDriverUnloadFailure(int error = ENOMEDIUM) {
    EXPECT_CALL(m_wrapper, ioctl(7, MTIOCTOP, An<mtop*>())).WillOnce([=](int, unsigned long, mtop* command) {
      EXPECT_EQ(MTUNLOAD, command->mt_op);
      errno = error;
      return -1;
    });
  }

  static void checkUnloadCommand(const sg_io_hdr_t* command) {
    ASSERT_EQ(6, command->cmd_len);
    EXPECT_EQ(SCSI::Commands::START_STOP, command->cmdp[0]);
    for (unsigned i = 1; i < 6; ++i) {
      EXPECT_EQ(0, command->cmdp[i]);
    }
    EXPECT_EQ(SG_DXFER_NONE, command->dxfer_direction);
    EXPECT_GT(command->timeout, 0);
    EXPECT_LE(command->timeout, SCSI::defaultTimeout);
  }
};

TEST_F(DriveMediaTest, ReadyProbeRefreshesDriverBeforeMediaAccess) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    EXPECT_EQ(SCSI::Commands::TEST_UNIT_READY, command->cmdp[0]);
    EXPECT_LE(command->timeout, 30000);
    return 0;
  });
  expectStatus(true);
  EXPECT_TRUE(m_drive->waitForMediaReady(std::chrono::steady_clock::now() + 1s));
  EXPECT_EQ(2, m_opens);
  EXPECT_EQ(1, m_closes);
}

TEST_F(DriveMediaTest, UnloadedMediaDoesNotPollOrReopen) {
  for (const auto& [asc, ascq] : {
         std::pair {0x3a, 0x00},
         {0x3a, 0x04},
         {0x04, 0x02}
  }) {
    for (bool descriptor : {false, true}) {
      EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
        .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, asc, ascq, descriptor); });
      EXPECT_FALSE(m_drive->waitForMediaReady(std::chrono::steady_clock::now() + 1s));
    }
  }
  EXPECT_EQ(1, m_opens);
  EXPECT_EQ(0, m_closes);
}

TEST_F(DriveMediaTest, UnitAttentionIsRetriedBeforeObservingReadiness) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .WillOnce([](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 6, 0x28, 0); })
    .WillOnce([](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, 0x3a, 0); });
  EXPECT_FALSE(m_drive->waitForMediaReady(std::chrono::steady_clock::now() + 1s));
}

TEST_F(DriveMediaTest, ExpiredBudgetOnlyProbesWithoutRetryOrReopen) {
  for (const auto key : {SCSI::senseKeys::unitAttention, SCSI::senseKeys::notReady}) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      EXPECT_EQ(1, command->timeout);
      return sense(command, key, key == SCSI::senseKeys::unitAttention ? 0x29 : 0x04, 1);
    });
    EXPECT_FALSE(m_drive->waitForMediaReady(std::chrono::steady_clock::now() - 1s));
  }
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, TemporaryNotReadyPollsUntilMediaCanBeAccessed) {
  for (const auto& [asc, ascq] : {
         std::pair {0x04, 0x01},
         {0x04, 0x04},
         {0x04, 0x07},
         {0x04, 0x13},
         {0x00, 0x16},
         {0x30, 0x03}
  }) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
      .WillOnce([=](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 2, asc, ascq); })
      .WillOnce(Return(0));
    expectStatus(true);
    EXPECT_TRUE(m_drive->waitForMediaReady(std::chrono::steady_clock::now() + 5s));
  }
}

TEST_F(DriveMediaTest, UnrecognizedNotReadyAndHardwareFailuresAreNotAbsence) {
  for (const auto& [key, asc] : {
         std::pair {2, 0x53},
         {4, 0x44}
  }) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      return sense(command, key, asc, 0);
    });
    EXPECT_THROW(m_drive->waitForMediaReady(std::chrono::steady_clock::now() + 1s), exception::Exception);
  }
}

TEST_F(DriveMediaTest, InvalidSenseIsNeverAbsence) {
  for (unsigned variant = 0; variant < 5; ++variant) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      sense(command, 2, 0x3a, 0, variant == 3);
      switch (variant) {
        case 0:
          command->sb_len_wr = 0;
          break;
        case 1:
          command->sb_len_wr = 13;
          break;
        case 2:
          command->sbp[7] = 0;
          break;
        case 3:
          command->sb_len_wr = 7;
          break;
        case 4:
          command->sbp[0] = 0x7f;
          break;
      }
      return 0;
    });
    EXPECT_THROW(m_drive->waitForMediaReady(std::chrono::steady_clock::now() + 1s), exception::Exception);
  }
}

TEST_F(DriveMediaTest, TransportAndDeferredErrorsAreNotAbsence) {
  for (unsigned variant = 0; variant < 3; ++variant) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      sense(command, 2, 0x3a, 0);
      if (variant == 0) {
        command->host_status = 1;
      }
      if (variant == 1) {
        command->driver_status = SCSI::DriverStatus::TIMEOUT;
      }
      if (variant == 2) {
        command->sbp[0] = 0x71;
      }
      return 0;
    });
    EXPECT_THROW(m_drive->waitForMediaReady(std::chrono::steady_clock::now() + 1s), exception::Exception);
  }
}

TEST_F(DriveMediaTest, RefreshedOfflineStatusIsNotReportedAsElapsedTimeout) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce(Return(0));
  expectStatus(false);
  try {
    m_drive->waitUntilReady(900);
    FAIL() << "Expected driver-state error";
  } catch (const TapeDriverStateError& ex) {
    EXPECT_THAT(ex.getMessageValue(), testing::HasSubstr("offline after successful TEST UNIT READY"));
    EXPECT_THAT(ex.getMessageValue(), testing::Not(testing::HasSubstr("900 seconds")));
  }
}

TEST_F(DriveMediaTest, LoadingPollsTransitionalStatesAndCapsCommandTimeouts) {
  std::vector<unsigned> timeouts;
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .WillOnce([&](int, unsigned long, sg_io_hdr_t* command) {
      timeouts.push_back(command->timeout);
      return sense(command, 2, 4, 1);
    })
    .WillOnce([&](int, unsigned long, sg_io_hdr_t* command) {
      timeouts.push_back(command->timeout);
      return 0;
    });
  expectStatus(true);
  m_drive->waitUntilReady(1);
  ASSERT_EQ(2, timeouts.size());
  EXPECT_LE(timeouts.front(), 1000);
  EXPECT_LT(timeouts.back(), timeouts.front());
}

TEST_F(DriveMediaTest, LoadingDoesNotPollTerminalMediaStates) {
  for (const auto& [asc, ascq] : {
         std::pair {0x3a, 0   },
         {0x04, 0x02},
         {0x3a, 0x04},
         {0x53, 0   }
  }) {
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      return sense(command, 2, asc, ascq);
    });
    EXPECT_THROW(m_drive->waitUntilReady(900), exception::Exception);
  }
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, LoadingTimeoutUsesRemainingBudgetAndDoesNotReopen) {
  unsigned previous = 1000;
  unsigned calls = 0;
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .WillRepeatedly([&](int, unsigned long, sg_io_hdr_t* command) {
      EXPECT_GT(command->timeout, 0);
      EXPECT_LE(command->timeout, previous);
      previous = command->timeout;
      ++calls;
      return sense(command, 2, 4, 1);
    });
  EXPECT_THROW(m_drive->waitUntilReady(1), exception::TimeOut);
  EXPECT_GT(calls, 1);
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, ZeroBudgetMakesOneBoundedProbeWithoutRetry) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    EXPECT_EQ(1, command->timeout);
    return sense(command, 6, 0x29, 0);
  });
  EXPECT_THROW(m_drive->waitUntilReady(0), exception::TimeOut);
}

TEST_F(DriveMediaTest, LateSuccessfulCommandDoesNotRestartLoadingBudget) {
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t*) {
    std::this_thread::sleep_for(1100ms);
    return 0;
  });
  EXPECT_THROW(m_drive->waitUntilReady(1), exception::TimeOut);
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, SuccessfulDriverUnloadNeedsNoRecoveryCommands) {
  EXPECT_CALL(m_wrapper, ioctl(7, MTIOCTOP, An<mtop*>())).WillOnce(Return(0));
  EXPECT_NO_THROW(m_drive->unloadTape());
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, CachedNonReadyDriverCanRecoverAnUnthreadedCartridge) {
  expectDriverUnloadFailure(EIO);
  expectStatus(false);
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    checkUnloadCommand(command);
    return 0;
  });
  EXPECT_NO_THROW(m_drive->unloadTape());
  EXPECT_EQ(3, m_opens);
  EXPECT_EQ(2, m_closes);
}

TEST_F(DriveMediaTest, RepeatedEmptyUnloadIsAcceptedInBothSenseFormats) {
  for (bool descriptor : {false, true}) {
    expectDriverUnloadFailure();
    expectStatus(false);
    EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([=](int, unsigned long, sg_io_hdr_t* command) {
      checkUnloadCommand(command);
      return sense(command, 2, 0x3a, 0, descriptor);
    });
    EXPECT_NO_THROW(m_drive->unloadTape());
  }
}

TEST_F(DriveMediaTest, RecoveryUnloadRetriesUnitAttention) {
  expectDriverUnloadFailure();
  expectStatus(false);
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .WillOnce([](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 6, 0x28, 0); })
    .WillOnce(Return(0));
  EXPECT_NO_THROW(m_drive->unloadTape());
}

TEST_F(DriveMediaTest, RecoveryUnloadDoesNotAcceptMamAccessibleAsAbsent) {
  expectDriverUnloadFailure();
  expectStatus(false);
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce([](int, unsigned long, sg_io_hdr_t* command) {
    return sense(command, 2, 0x3a, 4);
  });
  EXPECT_THROW(m_drive->unloadTape(), TapeDriverStateError);
  EXPECT_EQ(3, m_opens);
}

TEST_F(DriveMediaTest, RecoveryUnitAttentionIsBoundedAndRefreshesDriverOnFailure) {
  expectDriverUnloadFailure();
  expectStatus(false);
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>()))
    .Times(3)
    .WillRepeatedly([](int, unsigned long, sg_io_hdr_t* command) { return sense(command, 6, 0x29, 0); });
  EXPECT_THROW(m_drive->unloadTape(), TapeDriverStateError);
  EXPECT_EQ(3, m_opens);
}

TEST_F(DriveMediaTest, OnlineDriverUnloadFailureIsNotSilentlyRetried) {
  expectDriverUnloadFailure(EIO);
  expectStatus(true);
  EXPECT_THROW(m_drive->unloadTape(), TapeDriverStateError);
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, BufferOrStatusFailurePreventsRawRecovery) {
  expectDriverUnloadFailure(EIO);
  EXPECT_CALL(m_wrapper, ioctl(7, MTIOCGET, An<mtget*>())).WillOnce([](int, unsigned long, mtget*) {
    errno = EIO;
    return -1;
  });
  EXPECT_THROW(m_drive->unloadTape(), TapeDriverStateError);
  EXPECT_EQ(1, m_opens);
}

TEST_F(DriveMediaTest, CloseFailurePreventsRawRecoveryAndDoubleClose) {
  expectDriverUnloadFailure();
  expectStatus(false);
  EXPECT_CALL(m_wrapper, close(7)).Times(1).WillOnce([](int) {
    errno = EIO;
    return -1;
  });
  EXPECT_THROW(m_drive->unloadTape(), TapeDriverStateError);
  m_drive.reset();
}

TEST_F(DriveMediaTest, RecoverySuccessDoesNotHideReopenFailure) {
  expectDriverUnloadFailure();
  expectStatus(false);
  EXPECT_CALL(m_wrapper, ioctl(7, SG_IO, An<sg_io_hdr_t*>())).WillOnce(Return(0));
  EXPECT_CALL(m_wrapper, open(_, _)).WillOnce(Return(7)).WillOnce([](const char*, int) {
    errno = EIO;
    return -1;
  });
  EXPECT_THROW(m_drive->unloadTape(), TapeDriverStateError);
  EXPECT_EQ(2, m_closes);
  m_drive.reset();
  EXPECT_EQ(2, m_closes);
}

}  // namespace
}  // namespace cta::tape::drive
