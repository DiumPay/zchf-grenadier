# build stage — alpine + pure Go, no CGO
FROM golang:1.26-alpine3.23 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 \
    go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/grenadier \
        .

FROM gcr.io/distroless/static-debian13:nonroot

WORKDIR /data
COPY --from=build /out/grenadier /grenadier

ENV GRENADIER_BIND=0.0.0.0
ENV GRENADIER_PROXY_HOPS=2

EXPOSE 8080
VOLUME ["/data"]
USER nonroot:nonroot
ENTRYPOINT ["/grenadier"]