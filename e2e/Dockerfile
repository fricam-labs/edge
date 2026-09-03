FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum main.go ./
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/edge-talk-e2e .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/edge-talk-e2e /edge-talk-e2e
USER nonroot:nonroot
ENTRYPOINT ["/edge-talk-e2e"]
