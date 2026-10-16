# syntax=docker/dockerfile:1.7
FROM ubuntu:22.04 AS builder
ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive \
    PATH=/root/.cargo/bin:$PATH \
    CARGO_PROFILE_RELEASE_DEBUG=0 \
    CARGO_INCREMENTAL=0
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates clang cmake curl git libclang-dev libssl-dev \
    pkg-config unzip zlib1g-dev && rm -rf /var/lib/apt/lists/*
# Match upstream's protobuf release; Ubuntu 22.04's protoc is too old.
RUN case "$TARGETARCH" in amd64) arch=x86_64 ;; arm64) arch=aarch_64 ;; *) exit 1 ;; esac && \
    curl --fail --location --retry 3 -o /tmp/protoc.zip \
    "https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protoc-29.3-linux-${arch}.zip" && \
    unzip /tmp/protoc.zip -d /usr/local && rm /tmp/protoc.zip && \
    protoc --version | grep -Fx 'libprotoc 29.3'
RUN curl --proto '=https' --tlsv1.2 --fail --silent --show-error https://sh.rustup.rs -o /tmp/rustup.sh && \
    sh /tmp/rustup.sh -y --profile minimal --default-toolchain nightly-2026-03-21 && rm /tmp/rustup.sh
WORKDIR /source
RUN git init && git remote add origin https://github.com/GreptimeTeam/greptimedb.git && \
    git fetch --depth 1 origin 179ff8e5099d35e217a5ffeb6e4096405824dd6c && \
    git checkout --detach FETCH_HEAD && \
    test "$(git rev-parse HEAD)" = 179ff8e5099d35e217a5ffeb6e4096405824dd6c && \
    git submodule update --init --recursive --depth 1
ARG BUILD_JOBS=2
ARG PATCH_SHA256
COPY greptime_local.patch /tmp/greptime_local.patch
RUN test -n "$PATCH_SHA256" && \
    echo "$PATCH_SHA256  /tmp/greptime_local.patch" | sha256sum --check --strict && \
    git apply --check /tmp/greptime_local.patch && git apply /tmp/greptime_local.patch
RUN --mount=type=cache,id=greptime-local-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=greptime-local-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=greptime-local-target-${TARGETARCH},target=/source/target,sharing=locked \
    case "$BUILD_JOBS" in 1|2|3|4) ;; *) exit 1 ;; esac && \
    cargo +nightly-2026-03-21 build --locked --release -p cmd --bin greptime --jobs "$BUILD_JOBS" && \
    mkdir /out && cp target/release/greptime /out/greptime && \
    /out/greptime standalone start --help | grep -F -- '--require-local-sst-mirror-v1'

FROM builder AS native-tests
RUN --mount=type=cache,id=greptime-local-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=greptime-local-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=greptime-local-target-${TARGETARCH},target=/source/target,sharing=locked \
    cargo +nightly-2026-03-21 test --locked --release -p mito2 --lib \
      mirror --jobs "$BUILD_JOBS" -- --test-threads=1 && \
    cargo +nightly-2026-03-21 test --locked --release -p mito2 --lib \
      compaction::compactor::tests::test_strict --jobs "$BUILD_JOBS" -- --test-threads=1 && \
    cargo +nightly-2026-03-21 test --locked --release -p session -p frontend \
      test_local_sst --jobs "$BUILD_JOBS" -- --test-threads=1 && \
    cargo +nightly-2026-03-21 test --locked --release -p servers --lib \
      test_local_action_is_opt_in_and_preserves_flight_encoding --jobs "$BUILD_JOBS" -- --test-threads=1 && \
    cargo +nightly-2026-03-21 test --locked --release -p metric-engine --lib \
      test_recovery_metadata_requires_mirror_but_user_reads_do_not --jobs "$BUILD_JOBS" -- --test-threads=1

FROM ubuntu:22.04 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libssl3 zlib1g && \
    rm -rf /var/lib/apt/lists/*
ARG PATCH_SHA256
LABEL org.opencontainers.image.source="https://github.com/GreptimeTeam/greptimedb" \
    org.opencontainers.image.revision="179ff8e5099d35e217a5ffeb6e4096405824dd6c" \
    org.datalake.greptime.patch-sha256="$PATCH_SHA256" \
    org.datalake.greptime.local-sst-mirror="v1"
COPY --from=builder /out/greptime /greptime/bin/greptime
WORKDIR /greptime
ENTRYPOINT ["/greptime/bin/greptime"]
