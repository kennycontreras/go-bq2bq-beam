package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

type Row struct {
	Text string `bigquery:"text"`
}

const query = ``
const delimiter = ";"

func extractFn(line string, delimiter string, emit func([]string)) {
	split := strings.Split(line, delimiter)
	emit(split)
}

func SplitLines(s beam.Scope, lines beam.PCollection, delimiter string) beam.PCollection {
	s = s.Scope("Split Lines")
	col := beam.ParDo(s, extractFn, lines)
	return col
}

func formatFn(w string) string {
	return fmt.Sprintf("%s", w)
}

func main() {

	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()
	project := gcpopts.GetProject(ctx)

	if err := beamx.Run(ctx, p); err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}

	rows := bigqueryio.Query(s, project, query,
		reflect.TypeOf(Row{}), bigqueryio.UseStandardSQL())

	lines := SplitLines(s, rows, delimiter)
	formatted := beam.ParDo(s, formatFn, lines)

	textio.Write(s, "/tmp/output.csv", formatted)

	bigqueryio.Write(s, project, "", rows)
}
