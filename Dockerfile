FROM golang:1.25-alpine AS build
WORKDIR /src
RUN apk add --no-cache build-base
COPY go.mod go.sum main.go main_test.go relay.go pairing.go app-icon.svg ./
RUN go test -race ./...
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/fricam-edge .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/fricam-edge /fricam-edge
USER nonroot:nonroot
ENTRYPOINT ["/fricam-edge"]
