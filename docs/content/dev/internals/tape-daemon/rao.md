# RAO Internals

Implementation background for retrieval ordering in the tape daemon. Start with [RAO concepts](../../../concepts/tape/rao.md); operational settings belong in [RAO configuration](../../../ops/configuration/tape-daemon.md#recommended-access-order).

!!! info "Documentation outline"
    Detailed procedures and examples will be completed during the content review.

## Algorithm selection and fallback

Trace `taped/rao/RAOAlgorithmFactoryFactory.cpp` and `RAOManager.cpp`: drive capability, hardware limits, configured software algorithms, and linear fallback. Build the selection diagram from current code rather than the old enterprise-only distinction.

## SLTF and media geometry

Document the cost heuristic, wrap and LPOS estimates, catalogue media-type inputs, and how the algorithm chooses the next file.

## Drive interfaces and SCSI position data

Document hardware RAO requests and results, REQUEST SENSE and READ END OF WRAP POSITION (REOWP), position interpretation, and capability handling.

## Tests and performance investigations

Cover selection, unavailable geometry, exceptions, fallback, and ordering correctness. Review the v4 LTO reports and experiments before adding research references; separate measurement assumptions from supported configuration.
