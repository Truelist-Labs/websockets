ARG GO_VERSION=1.27.3

FROM golang:${GO_VERSION} AS builder

WORKDIR /src

COPY ./go.mod ./go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath -o /app .

# Static binary on a minimal base: CA certificates and a nonroot user, no shell.
FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=builder /app /app

EXPOSE 8080

ENTRYPOINT ["/app"]
