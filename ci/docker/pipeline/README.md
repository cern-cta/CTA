# Pipeline images

These images, along with the `build.Dockerfile` and `test.Dockerfile` under `docker/cta/$PLATFORM` make up the pipeline images used in the GitLab CI.

All Docker images in this directory must be platform agnostic. That is, if the pipeline is ran with a different platform (e.g. el10 instead of el9), then any job relying on an image from this directory should not break.
