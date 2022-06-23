package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"log"
	"reflect"
	"strings"
)

type CommentRow struct {
	Text string `bigquery:"text"`
}

const query = `SELECT text FROM ` + "`bigquery-public-data.hacker_news.comments`" + `
WHERE time_ts BETWEEN '2013-01-01' AND '2014-01-01'
LIMIT 1000`

const delimiter = ";"

func init() {
	beam.RegisterFunction(extractFn)
	beam.RegisterFunction(formatFn)
}

func extractFn(row CommentRow, emit func([]string)) {
	fmt.Print(row.Text)
	split := strings.Split(row.Text, delimiter)
	emit(split)
}

func SplitLines(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("Split Lines")
	col := beam.ParDo(s, extractFn, lines)
	return col
}

func formatFn(w []string) string {
	return fmt.Sprintf("%s", strings.Join(w, delimiter))
}

func main() {

	flag.Parse()
	beam.Init()

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()
	project := gcpopts.GetProject(ctx)

	rows := bigqueryio.Query(s, project, query,
		reflect.TypeOf(CommentRow{}), bigqueryio.UseStandardSQL())

	lines := SplitLines(s, rows)
	formatted := beam.ParDo(s, formatFn, lines)

	textio.Write(s, "/tmp/output.csv", formatted)

	if err := beamx.Run(context.Background(), p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}

	//bigqueryio.Write(s, project, "", rows)
}
