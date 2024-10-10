
# Configuration variables
ARG RUST_VERSION=1.81
ARG FLATBUFFERS_VERSION=23.5.26

###############################################################################
# Builder stage ###############################################################
###############################################################################


FROM rust:${RUST_VERSION} AS builder

# Install dependencies
# TODO: Maybe we also want to build protobuf from source?
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    git \
    cmake \
    make \
    clang

# Can't just install the flatbuffers packages because they are too
# old. Instead, we need to build them from source.
# Install FlatBuffers
WORKDIR /flatbuffers_build
ARG FLATBUFFERS_VERSION
RUN git clone https://github.com/google/flatbuffers.git && \
    cd flatbuffers && \
    git checkout v${FLATBUFFERS_VERSION} && \
    cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release && \
    make -j$(nproc) && \
    make install

# Build chardonnay
WORKDIR /chardonnay_build

# Copy the entire project
COPY . .

# Build the project
RUN cargo build --release

###############################################################################
# rangeserver #################################################################
###############################################################################

FROM debian:bookworm AS rangeserver

# Copy the built executable from the builder stage
COPY --from=builder /chardonnay_build/target/release/rangeserver /usr/bin/rangeserver

# Set the entrypoint
ENTRYPOINT ["rangeserver"]

###############################################################################
# warden ######################################################################
###############################################################################

FROM debian:bookworm AS warden

# Copy the built executable from the builder stage
COPY --from=builder /chardonnay_build/target/release/warden /usr/bin/warden

# Set the entrypoint
ENTRYPOINT ["warden"]

###############################################################################
# epoch_publisher #############################################################
###############################################################################

FROM debian:bookworm AS epoch_publisher

# Copy the built executable from the builder stage
COPY --from=builder /chardonnay_build/target/release/epoch_publisher /usr/bin/epoch_publisher

# Set the entrypoint
ENTRYPOINT ["epoch_publisher"]


###############################################################################
# epoch_service ###############################################################
###############################################################################

FROM debian:bookworm AS epoch

# Copy the built executable from the builder stage
COPY --from=builder /chardonnay_build/target/release/epoch /usr/bin/epoch

# Set the entrypoint
ENTRYPOINT ["epoch"]

###############################################################################
# universe ####################################################################
###############################################################################

FROM debian:bookworm AS universe

# Copy the built executable from the builder stage
COPY --from=builder /chardonnay_build/target/release/universe /usr/bin/universe

# Set the entrypoint
ENTRYPOINT ["universe"]
