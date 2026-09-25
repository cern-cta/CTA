# Branches

All development work is done on branches, which are merged into `main` via [merge requests](merge-requests.md).

- **Start from an issue.** All work must be tracked in a CTA GitLab issue. Follow the [issue guidelines](issues.md) and apply the required labels.
- **Create development branches from the issue via “Create merge request”.** This enforces naming, opens an MR immediately, and the branch will be **auto-deleted** on merge.
- **Create non-merging branches via “Create branch”.** Use this for benchmarking/testing spikes. It links to the issue but **does not** open an MR; **you must delete** the branch when finished.

!!! info

    Ensure you frequently rebase your branches to prevent having to do a potentially complicated rebase at the very end.


**Naming**

- Allowed characters: `a–z`, `0–9`, `-`, `/`, `.` (lowercase).
- Pattern:
  ```txt
  <issue-number>-<issue-title-slug>
  ```

Creating the branch from the issue applies this automatically.

- Rarely, a branch may exist without an issue (must be justified). Use a clear, descriptive name. For test/benchmark work tied to an issue, reuse the base name with a descriptive suffix, and clean it up when done.

**Forks**

Prefer branches in the main repo. Use forks only when required (e.g., permission constraints).
