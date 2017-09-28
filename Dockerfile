FROM golang:latest
ADD . /go/src/ethtx
RUN go install ethtx
ENTRYPOINT ["/go/bin/ethtx"]
