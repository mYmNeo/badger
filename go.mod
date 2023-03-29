module github.com/dgraph-io/badger

go 1.20

require (
	github.com/AndreasBriese/bbloom v0.0.0-20190825152654-46b345b51c96
	github.com/dgraph-io/ristretto v0.0.2
	github.com/dustin/go-humanize v1.0.0
	github.com/golang/protobuf v1.3.1
	github.com/klauspost/compress v1.16.4
	github.com/pkg/errors v0.9.1
	github.com/spf13/cobra v0.0.5
	github.com/stretchr/testify v1.4.0
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
	golang.org/x/sys v0.0.0-20221010170243-090e33056c14
)

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/kr/pretty v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.2.2 // indirect
)

replace github.com/dgraph-io/ristretto => github.com/mYmNeo/ristretto v0.0.0-20230118015648-bed7e29b5354
