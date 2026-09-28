# Container Image Conventions

- Secrets MUST NOT be included in a Dockerfile (e.g., passwords, API keys, tokens).
- Configuration values MUST NOT be hardcoded in a Dockerfile. They MUST be provided via environment variables, mounted configuration files, or secret management tools.
- Build steps that can be performed at image build time SHOULD NOT be deferred to runtime (e.g., installing system packages).
- Conditional logic (e.g., `if`, `case`) SHOULD NOT be used in Dockerfile instructions. If different variants are needed, prefer separate Dockerfiles.
- Dockerfiles MUST be named `[<purpose>.]Dockerfile`.
    - If there is only one image, `Dockerfile` is sufficient.
    - Otherwise, prefix the purpose/name (e.g., `worker.Dockerfile`).
- Multi-stage builds SHOULD be used if they help keep the runtime image lightweight.
- Oracle RPMs MUST NOT be installed in any image that might ever be publicly available.
- Related instructions (e.g., installing multiple packages) SHOULD be grouped into a single layer. Overly complex `RUN` commands SHOULD be avoided.
- Instructions that are likely to change frequently (e.g., application source code, last steps of setup) SHOULD appear later in the Dockerfile to maximize build cache reuse.
- Images SHOULD run as a non-root user unless root privileges are strictly required.
- Dependencies SHOULD be pinned to specific versions to ensure reproducibility.
- Base images SHOULD be chosen from official and minimal sources.

!!! tip

    - Add comments in Dockerfiles to explain non-trivial steps or design decisions.
    - Keep image sizes as small as possible for faster build and deployment.
    - Prefer explicit, stable image tags over `latest` to avoid accidental upgrades.
