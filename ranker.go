package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	Lost = iota // This guy is mostly a dummy value
	Draw
	Win = 3
)

type (
	Record struct {
		RowIdx uint64
		Teams  []string
		Scores []int
	}

	Digestion struct {
		Team   string
		Points int
	}

	RankComparator func(digAlpha, digBeta Digestion) bool

	RankingJob struct {
		discard    chan interface{}
		begins     time.Time
		ends       time.Time
		Err        error
		Digs       []Digestion
		Comparator RankComparator
	}

	// Represents a contract to write an extractor mechanism
	ExtractorEngine interface {
		Pull(j *RankingJob, onFail func(error)) <-chan *Record
		Release()
	}

	// An extractor that stands for CSV file
	Trivial struct {
		File *os.File
	}
)

func (s RankingJob) Swap(i, j int) {

	s.Digs[i], s.Digs[j] = s.Digs[j], s.Digs[i]
}

func (s RankingJob) Len() int {

	return len(s.Digs)
}

func (s RankingJob) Less(i, j int) bool {

	return s.Comparator(s.Digs[i], s.Digs[j])
}

func digest(in <-chan *Record) []Digestion {

	var digs []Digestion
	board := make(map[string]int)

	for rec := range in {

		switch {
		case rec.Scores[0] > rec.Scores[1]:
			board[rec.Teams[0]] += Win
			board[rec.Teams[1]] += Lost
		case rec.Scores[0] < rec.Scores[1]:
			board[rec.Teams[1]] += Win
			board[rec.Teams[0]] += Lost
		default:
			board[rec.Teams[0]] += Draw
			board[rec.Teams[1]] += Draw
		}
	}

	//Sum up board
	for team, points := range board {

		digs = append(digs, Digestion{team, points})
	}

	return digs
}

func demo(ext ExtractorEngine, j *RankingJob) {

	defer func() {
		j.ends = time.Now()
		ext.Release()
	}()

	j.begins = time.Now()
	j.discard = make(chan interface{})

	/* Utilizes the circuit breaker as the onFail event handler of Pull
	   for termination of job as expected */
	shockAbsorber := func(err error) {

		j.Err = err
		close(j.discard)
	}

	j.Digs = digest(ext.Pull(j, shockAbsorber))

	if j.Err == nil {

		j.Comparator = func(digAlpha, digBeta Digestion) bool {

			if digAlpha.Points == digBeta.Points {

				// We carry out a lexicographically comparison
				// In this situation we assume that
				// there is not any likelihood of having
				// two digestion with the same name, So..
				// The solely possible results expected are 1 and -1
				return strings.Compare(digAlpha.Team, digBeta.Team) == -1
			}

			return digAlpha.Points > digBeta.Points
		}

		sort.Sort(j)
	}
}

func pull(rd io.Reader, discard chan interface{}, onFail func(error)) <-chan *Record {

	out := make(chan *Record)

	regColCont := regexp.MustCompile(`(?P<Team>[a-zA-Z]+\s?[a-zA-Z]+) (?P<Score>\d+)`)
	TeamIdx := regColCont.SubexpIndex("Team")
	ScoreIdx := regColCont.SubexpIndex("Score")

	populate := func(gs *Record, col []string) error {

		var maxColumn int = len(col)

		if !(maxColumn > 0) {

			return fmt.Errorf("Missing columns at line %d", gs.RowIdx)
		}

		explodeCol := func(colText string) (string, int, error) {

			matches := regColCont.FindStringSubmatch(strings.TrimSpace(colText))

			if len(matches) > 0 {

				numScore, _ := strconv.Atoi(matches[ScoreIdx])

				return matches[TeamIdx], numScore, nil
			}

			return "", 0, fmt.Errorf("A few elements were not found at line %d", gs.RowIdx)
		}

		{
			var err error
			gs.Teams = make([]string, maxColumn)
			gs.Scores = make([]int, maxColumn)

			for i := 0; i < maxColumn; i++ {

				if gs.Teams[i], gs.Scores[i], err = explodeCol(col[i]); err != nil {

					return err
				}
			}
		}

		return nil
	}

	go func() {

		defer close(out)

		r := csv.NewReader(rd)
		var counter uint64 = 0

		for {
			rec, err := r.Read()

			counter++

			if err == io.EOF {

				if counter == 1 {

					onFail(fmt.Errorf("No records to fetch"))
				}

				break
			}

			if err != nil {

				onFail(fmt.Errorf("Record could not be extracted from line %d", counter))
				break
			}

			gs := Record{}
			gs.RowIdx = counter

			if err = populate(&gs, rec); err != nil {

				onFail(err)
				break
			}

			select {
			case out <- &gs:
			case <-discard:
				return
			}
		}
	}()

	return out
}

// Turns every row of a csv file into a magic Score
func (t *Trivial) Pull(j *RankingJob, onFail func(error)) <-chan *Record {

	// CSV support till now
	return pull(t.File, j.discard, onFail)
}

// Release the resources of the Extractor
func (t *Trivial) Release() {

	t.File.Close()
}

func main() {

	file, _ := os.Open("input.csv")

	j := RankingJob{}

	demo(&Trivial{file}, &j)

	// An overkill block with a primitive flavor to print out
	{
		rowIdx := 0
		rows := len(j.Digs)
		plural := [...]byte{' ', 's'}
		cidx := 0
	gearUpPrint:

		if rowIdx < rows {

			dig := j.Digs[rowIdx]

			if dig.Points == 1 {
				cidx = 0
			} else {
				cidx = 1
			}

			fmt.Printf("%d. %s, %d pt%c\n", rowIdx, dig.Team, dig.Points, plural[cidx])
			rowIdx++
			goto gearUpPrint
		}
	}
}
