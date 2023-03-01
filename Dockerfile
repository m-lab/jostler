FROM golang:1.19 AS build
# VERSION should be specified via the --build-arg flag as a branch,
# a tag, or a short git commit.
ARG VERSION=unspecified
# Copy source files into the working directory and build a static binary.
WORKDIR /go/src/github.com/m-lab/jostler
COPY . .
ENV CGO_ENABLED 0
RUN go get -v ./...
RUN go install -v \
               -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h) \
               -X main.Version=$VERSION \
               -X main.GitCommit=$(git log -1 --format=%H)" \
      ./cmd/jostler

FROM alpine:3.15
# By default, alpine has no root certs. Add them so jostler can use PKI to
# verify that Google Cloud Storage is actually Google Cloud Storage.
RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/jostler /
WORKDIR /
# Make sure jostler can run (i.e., has no missing external dependencies).
RUN ./jostler -h 2> /dev/null

# To set the command line flags and arguments, add them to the end of the
# docker run command.  For flags only, you can specify them by setting
# their corresponding environment variables but it's better to explicitly
# specify the flags on the command line.
ENTRYPOINT ["/jostler"]
