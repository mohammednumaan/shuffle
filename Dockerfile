FROM golang:1.26.2-alpine AS build

WORKDIR /app

ADD . /app

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o mapreduce .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root

COPY --from=build /app/mapreduce .

ENTRYPOINT ["./mapreduce"]
