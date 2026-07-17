# Real BWCE app image (linux/amd64) = the Rest_Sample_Multiple_Endpoints EAR on
# the BWCE 6.12 base. BWCE 6.12 ships an x86-64 JRE + native launcher, so this
# image is amd64 and runs on the arm64 minikube nodes via Rosetta emulation
# (the nodes expose a `rosetta` binfmt handler). Build with:
#   docker build --platform linux/amd64 -t bwce-reqresp:amd64 .
# It must be built FROM an amd64 BWCE base (bwce-base:6.12-amd64), built from
# <tibco_home>/bw6/6.12/bw/6.12/docker with --platform linux/amd64.
FROM --platform=linux/amd64 bwce-base:6.12-amd64
LABEL maintainer="adidas-log-collection-usecase"
ADD Rest_Sample_Multiple_Endpoints.application_1.0.0.ear /
EXPOSE 8080
