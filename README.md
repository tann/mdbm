# MDBM Go binding

[![GoDoc](https://godoc.org/github.com/tann/mdbm?status.svg)](https://godoc.org/github.com/tann/mdbm)

Go binding for **mdbm**

# Installation

Follow [MDBM installation instructions](https://github.com/yahoo/mdbm/blob/master/README.build), then 

`go get github.com/tann/mdbm`

By default, CFLAGS and LDFLAGS point to `/tmp/install/include` and `/tmp/install/lib`respectively (default `prefix` to MDMB `configure` is `/tmp/install`). If your MDBM library is installed in a diferrent location, use environment variables CGO_CFLAGS and CGO_LDFLAGS to append it to the provided default values.

`go build .`

`go install`
