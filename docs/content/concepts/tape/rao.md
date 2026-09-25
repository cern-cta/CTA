# Recommended Access Order (RAO)

Retrieving files in a different order can reduce tape positioning time and improve retrieval throughput. Recommended Access Order (RAO) determines an order for a batch of files on the same tape; it does not replace CTA mount policies or queue scheduling.

!!! note "RAO does not replace good data collocation"
    RAO can improve retrieval throughput and drive efficiency, but it cannot compensate for poorly collocated data. It reorders reads on a tape; it does not change where files are stored or eliminate mounts when files commonly retrieved together are spread across many tapes. Good data placement remains important.

Tape is written in wraps that run along its length in alternating directions. File sequence numbers describe the order in which files were written, rather than the physical distance between them. Following those numbers is therefore not always the fastest way to retrieve a subset of files: RAO aims to reduce the movement between reads.

## Hardware RAO

Hardware RAO asks a capable drive to recommend the retrieval order. CTA supplies the batch of files, receives the drive's ordering, and uses it to schedule the reads within that batch. The drive can use its knowledge of the tape layout and positioning behaviour to optimise the order.

Hardware RAO is the preferred option when supported by the drive. Check the installed drive's capabilities and the [RAO configuration guidance](../../ops/configuration/tape-daemon.md#recommended-access-order) when commissioning it.

## Software RAO

Software RAO determines the read order in CTA and was developed for drives without hardware RAO. Three ordering options are available:

- **Linear:** reads files in tape file-sequence order.
- **Random:** randomises the read order as a comparison baseline.
- **Shortest locate time first (SLTF):** uses estimated file positions and a model of tape movement to select the next file expected to be quickest to reach.

Only SLTF actively optimises estimated positioning time; linear and random ordering provide baselines for comparison.

!!! warning "Prefer hardware RAO"
    Software RAO should generally not be used when hardware RAO is available: hardware RAO generally provides faster retrievals. Treat software RAO as an alternative for drives without hardware support, and validate its benefit for the intended workload.

For the algorithm and performance background, see the presentation [LTO performance: Make Tape Reading Great Again](https://indico.cern.ch/event/730908/contributions/3153156). Its measurements describe the hardware and workloads studied at the time.

## Related guides

Operators can find prerequisites, configuration, and diagnosis in [RAO configuration](../../ops/configuration/tape-daemon.md#recommended-access-order). The [retrieval lifecycle](../data-management/retrieval.md) explains the surrounding workflow.
