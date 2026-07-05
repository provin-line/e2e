# Allow-all PDP stub image for compose-runtime scenarios.
# Build from the e2e repo root:
#   docker build -t provin-line/pdpstub:local -f cmd/pdpstub/Dockerfile .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download || true
COPY cmd/pdpstub cmd/pdpstub
RUN CGO_ENABLED=0 go build -trimpath -o /out/pdpstub ./cmd/pdpstub

FROM alpine:3.22
COPY --from=build /out/pdpstub /usr/local/bin/pdpstub
EXPOSE 9091
ENTRYPOINT ["pdpstub"]
