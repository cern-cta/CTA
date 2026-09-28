# Media Initialisation

Prepare new tape media for use by CTA. For the underlying concepts, see [Tape Media](../../concepts/tape/media/index.md).

!!! info "Documentation outline"
    The workflow below will be expanded with detailed steps and validation checks.

## Prerequisites and registration

Document checking cartridge identity and compatibility, confirming that no data must be preserved, and registering the tape with the appropriate media type, logical library, and tape pool.

## Media preparation

Document any initialisation required by the media generation and drive, how to recognise its progress, and when the cartridge is ready for labelling.

## Labeling a tape

!!! danger
    Labelling a tape is destructive: it writes new labels at the beginning and establishes a new logical end of data (EOD). Old records beyond that point are no longer accessible through normal tape reads, even if their bytes have not all been physically overwritten. Never label a tape containing data that must be preserved. Labelling is not a full-media secure erase.

Before a tape can be written to by CTA, it must have the CTA format applied by *labeling* the tape.
The labeling procedure applies the CTA tape format, and specifically the VOL1 descriptor containing the tape's VID, to the beginning of the tape.
This VID field is used to verify that the tape's content is what one expected, based on the VID printed on the physical cartridge.
Individual tapes may be labeled using the `cta-tape-label` command line tool.
However, we strongly recommend using the wrapper command [cta-ops-admin tape label](../tools/cta-ops-admin.md), which supports bulk-labeling a number of tapes sequentially, and performs safety checks before doing destructive actions.

## Verify and release for use

Document verification of the recorded VID and catalogue entry, checks for preparation or labelling failures, and the criteria for making the tape available for archival.
