package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/bigqueryio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/gcpopts"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"log"
	"reflect"
	"time"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*CommentRow)(nil)))
}

var (
	output = flag.String("output", "", "Output file (required)")
)

const query = `SELECT ` + "`by`" + `, author, time_ts, text FROM ` + "`bigquery-public-data.hacker_news.comments`" + `
WHERE time_ts BETWEEN '2013-01-01' AND '2014-01-01'
LIMIT 1000`

const delimiter = ";"

type CommentRow struct {
	By     string    `bigquery:"by"`
	Author string    `bigquery:"author"`
	TimeTs time.Time `bigquery:"time_ts"`
	Text   string    `bigquery:"text"`
}

func (f *CommentRow) ProcessElement(ctx context.Context, line CommentRow, emit func(string)) {
	out, err := json.Marshal(line)

	if err != nil {
		panic(err)
	}

	fmt.Println(string(out))
	emit(string(out))
}

func ParseLines(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("Parse Lines")
	col := beam.ParDo(s, &CommentRow{}, lines)
	return col
}

func main() {

	flag.Parse()
	beam.Init()

	if *output == "" {
		log.Fatal("Output filename required")
	}

	ctx := context.Background()
	p := beam.NewPipeline()
	s := p.Root()
	project := gcpopts.GetProject(ctx)

	rows := bigqueryio.Query(s, project, query, reflect.TypeOf(CommentRow{}), bigqueryio.UseStandardSQL())

	lines := ParseLines(s, rows)

	textio.Write(s, *output, lines)

	if err := beamx.Run(ctx, p); err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}

}
