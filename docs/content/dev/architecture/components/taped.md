!!! info "Component being refactored"
    The tape daemon is undergoing a major refactoring. The documentation will be updated once the new design is final and close to being merged into main. For the time being you can check the [development ticket to see the progress](https://gitlab.cern.ch/cta/CTA/-/issues/805).

# CTA Tape Daemon (`cta-taped`)

The Tape Daemon is the process that interacts directly with the drive. It is responsible for getting jobs from the scheduler, executing those jobs and reporting back.

## Drive Process

