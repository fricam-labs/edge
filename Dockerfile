FROM golang:1.25-alpine AS build
WORKDIR /src
RUN apk add --no-cache build-base
COPY go.mod go.sum *.go app-icon.svg ./
ARG RUN_RACE_TESTS=1
RUN if [ "$RUN_RACE_TESTS" = "1" ]; then go test -race ./...; else go test ./...; fi
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fricam-edge .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/fricam-edge /fricam-edge
USER nonroot:nonroot
ENTRYPOINT ["/fricam-edge"]
