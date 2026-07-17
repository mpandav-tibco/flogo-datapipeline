# Runtime image for the flogo-datapipeline log collector (linux/arm64).
# The engine binary is prebuilt (native arm64, CGO) by build-image.sh and copied
# into this build context, so this stage needs no Go toolchain.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY flogo-dp-linux-arm64 /app/flogo-datapipeline
RUN chmod +x /app/flogo-datapipeline
# Args (the pipeline file) are supplied by the DaemonSet.
ENTRYPOINT ["/app/flogo-datapipeline"]
