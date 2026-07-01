FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/xunfei-retry-proxy .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates
RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=build /out/xunfei-retry-proxy /usr/local/bin/xunfei-retry-proxy

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/xunfei-retry-proxy"]
