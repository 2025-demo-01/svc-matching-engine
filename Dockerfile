FROM rust:1.81 AS build
WORKDIR /src
COPY . .
RUN cargo build --release

FROM gcr.io/distroless/cc-debian12
COPY --from=build /src/target/release/svc-matching-engine /app
USER 65532:65532
ENTRYPOINT ["/app"]
