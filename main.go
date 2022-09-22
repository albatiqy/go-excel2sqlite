package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	_ "github.com/buger/jsonparser"
	_ "github.com/mattn/go-sqlite3"
	"github.com/schollz/progressbar/v3"
	"github.com/sony/sonyflake"
	"github.com/xuri/excelize/v2"
)

type config struct {
	// servicePort string
	// fsRoot      string
}

type colHeader struct {
	cols    map[string]int
	reverse map[string]string
	maps    map[string]string
	_names  []string
}

func (h *colHeader) scan(xl *excelize.File, sheetName string, rowNum int) {
	if h.reverse != nil {
		return
	}
	rows, err := xl.Rows(sheetName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// skip header
	rowNum-- //to rowIdx
	for i := 0; i < rowNum; i++ {
		if !rows.Next() {
			log.Fatal("baris kosong!")
		}
	}
	if !rows.Next() {
		log.Fatal("baris kosong!")
	}
	//retval := make(map[string]int)
	cols, err := rows.Columns(excelize.Options{RawCellValue: true})
	if err != nil {
		log.Fatal(err)
	}

	reSpaces, err := regexp.Compile(`\s`)
	if err != nil {
		log.Fatal(err)
	}
	reNonAlphaNum, err := regexp.Compile(`[^a-z0-9_]`)
	if err != nil {
		log.Fatal(err)
	}

	h.cols = make(map[string]int)
	h.reverse = make(map[string]string)
	h.maps = make(map[string]string)

	for i, name := range cols {
		origName := name
		if strings.TrimSpace(origName) == "" {
			log.Fatal("nama kolom blank\n")
		}
		if _, ok := h.reverse[origName]; ok {
			log.Fatal("nama kolom tidak unik: %s !\n", origName)
		}
		name = strings.ToLower(name)
		name = reSpaces.ReplaceAllString(name, "_")
		name = reNonAlphaNum.ReplaceAllString(name, "")
		h.cols[name] = i
		h.reverse[origName] = name
		h.maps[name] = origName
	}
	h._names = make([]string, len(h.reverse))
	for i, val := range h.cols {
		h._names[val] = i
	}
}

func (h colHeader) names() []string {
	if h._names == nil {
		log.Fatal("scan header terlebih dahulu!")
	}
	return h._names
}

func (h colHeader) values(id uint64, col []string) []any {
	retval := make([]any, len(h._names)+1)
	if len(col) < len(retval) {
		for j := len(col); j <= len(h._names); j++ {
			col = append(col, "")
		}
	}
	retval[0] = id
	for i, name := range h.names() {
		retval[i+1] = col[h.cols[name]]
	}
	return retval
}

func main() {
	var configFile string
	flag.StringVar(&configFile, "cfg", "", "configuration file")
	flag.Parse()
	// cfg := getConfig(configFile)

	xlFname := "data prajab.xlsx"

	fInfo, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input" tidak ditemukan!`)
		}
	}

	baseFname := strings.TrimSuffix(fInfo.Name(), filepath.Ext(fInfo.Name()))
	tableName := strings.ReplaceAll(baseFname, " ", "_")

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name // jadikan parameter
	headerRowNum := 1
	dataStartRowNum := 2

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)

	sqliteFname := baseFname + ".sqlite"

	os.Remove(sqliteFname)
	db, err := sql.Open("sqlite3", sqliteFname)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// cara string builder
	var sb strings.Builder
	sb.WriteString("id BIGINT PRIMARY KEY NOT NULL,\n")
	for _, val := range header.names() {
		sb.WriteString(val + " TEXT NOT NULL,\n")
	}

	sqlCreateStmt := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.TrimSuffix(sb.String(), ",\n"))

	_, err = db.Exec(sqlCreateStmt)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := xl.Rows(sheetName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			log.Fatal("baris kosong!")
		}
	}

	// cara strings concat2
	cols := make([]string, len(header._names))
	for i, val := range header.names() {
		cols[i] = val
	}
	colsStr := strings.TrimSuffix("id,\n"+strings.Join(cols, ",\n"), ",\n")
	placeHolders := strings.TrimSuffix(strings.Repeat("?,\n", len(header._names)+1), ",\n")
	sqlInsert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, colsStr, placeHolders)

	sf := sonyflake.NewSonyflake(sonyflake.Settings{})

	stmt, err := db.Prepare(sqlInsert)
	if err != nil {
		log.Fatal(err)
	}

	bar := progressbar.NewOptions(-1,
		progressbar.OptionSetDescription("processing"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetWidth(10),
		// progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n") // os.Stderr
		}),
		progressbar.OptionSpinnerType(14),
		// progressbar.OptionFullWidth(),
		// progressbar.OptionSetRenderBlankState(true),
		// progressbar.OptionShowIts(),
	)

	queue := NewJobQueue(runtime.NumCPU())
	queue.Start()
	defer queue.Stop()
	for rows.Next() {
		col, err := rows.Columns(excelize.Options{RawCellValue: true})
		if err != nil {
			log.Fatal(err)
		}

		/*
		if len(col) < len(header._names) {
			y := len(header._names) - len(col)
			for k := 0; k < y; k++ {
				col = append(col, "")
			}
		} else {
			if len(col) > len(header._names) {
				log.Println("overflow rows length")
				break
			}
		}
		*/
		
		if len(col) > len(header._names) {
			log.Println("overflow rows length")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			log.Fatal(err)
		}

		args := header.values(id, col) // hack
		j := insertJob{stmt: stmt, args: args}
		queue.Submit(&j)
		bar.Add(1)
	}

	fmt.Println()
}

type insertJob struct {
	stmt *sql.Stmt
	args []any
}

func (j insertJob) Process() {
	// log.Print(j.args...)
	if _, err := j.stmt.Exec(j.args...); err != nil {
		log.Fatal(err)
	}
}

type Job interface {
	Process()
}

type Worker struct {
	done             sync.WaitGroup
	readyPool        chan chan Job
	assignedJobQueue chan Job

	quit chan bool
}

type JobQueue struct {
	internalQueue     chan Job
	readyPool         chan chan Job
	workers           []*Worker
	dispatcherStopped sync.WaitGroup
	workersStopped    sync.WaitGroup
	quit              chan bool
}

func NewJobQueue(maxWorkers int) *JobQueue {
	workersStopped := sync.WaitGroup{}
	readyPool := make(chan chan Job, maxWorkers)
	workers := make([]*Worker, maxWorkers, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		workers[i] = NewWorker(readyPool, workersStopped)
	}
	return &JobQueue{
		internalQueue:     make(chan Job),
		readyPool:         readyPool,
		workers:           workers,
		dispatcherStopped: sync.WaitGroup{},
		workersStopped:    workersStopped,
		quit:              make(chan bool),
	}
}

func (q *JobQueue) Start() {
	for i := 0; i < len(q.workers); i++ {
		q.workers[i].Start()
	}
	go q.dispatch()
}

func (q *JobQueue) Stop() {
	q.quit <- true
	q.dispatcherStopped.Wait()
}

func (q *JobQueue) dispatch() {
	q.dispatcherStopped.Add(1)
	for {
		select {
		case job := <-q.internalQueue:
			workerChannel := <-q.readyPool
			workerChannel <- job
		case <-q.quit:
			for i := 0; i < len(q.workers); i++ {
				q.workers[i].Stop()
			}
			q.workersStopped.Wait()
			q.dispatcherStopped.Done()
			return
		}
	}
}

func (q *JobQueue) Submit(job Job) {
	q.internalQueue <- job
}

func NewWorker(readyPool chan chan Job, done sync.WaitGroup) *Worker {
	return &Worker{
		done:             done,
		readyPool:        readyPool,
		assignedJobQueue: make(chan Job),
		quit:             make(chan bool),
	}
}

func (w *Worker) Start() {
	go func() {
		w.done.Add(1)
		for {
			w.readyPool <- w.assignedJobQueue
			select {
			case job := <-w.assignedJobQueue:
				job.Process()
			case <-w.quit:
				w.done.Done()
				return
			}
		}
	}()
}

func (w *Worker) Stop() {
	w.quit <- true
}
