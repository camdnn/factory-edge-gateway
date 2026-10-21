FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /edge ./cmd/edge && mkdir /state

# static image with CA certs (the agent is HTTPS) and a non-root user
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /edge /edge
COPY --from=build --chown=65532:65532 /state /var/lib/edge
ENTRYPOINT ["/edge"]
