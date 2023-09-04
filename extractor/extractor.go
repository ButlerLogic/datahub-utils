package extractor

import "dhs/extractor/doc"

type Extractor interface {
	SetConnectionString(str string) error
	Extract() (*doc.Doc, error)
	Type() string
}
