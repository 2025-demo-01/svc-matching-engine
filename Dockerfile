# Build
FROM rust:1.79-bullseye AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock /src/
RUN mkdir -p /src/src && echo "fn main(){}" > /src/src/main.rs && cargo build --release || true
COPY src /src/src
RUN cargo build --release

# Runtime (distroless-like)
FROM gcr.io/distroless/cc-debian12
WORKDIR /app
COPY --from=build /src/target/release/matching-engine /app/matching-engine
USER 65532:65532
EXPOSE 9100
ENTRYPOINT ["/app/matching-engine"]
