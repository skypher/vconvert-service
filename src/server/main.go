package main

import (
    "fmt"
    "time"
    "os"
    "os/exec"
    "sync"
    "strconv"
    "path/filepath"
    "net/http"
    "github.com/gin-gonic/gin"
    "github.com/skypher/jobqueue"
)

// FIXME should move to config file / command-line
const var topDir string = "/home/sky/p/wallmaster/service/encoding"
const var dataDir string = filepath.Join(topDir, "data")
const var inputFileDir string = filepath.Join(dataDir, "videos", "in")
const var outputFileDir string = filepath.Join(dataDir, "videos", "out")
const var jobQueueDir string = filepath.Join(dataDir, "jobqueue")
const var tileScriptPath string = filepath.Join(topDir, "scripts", "tile", "tile.sh")
const var scriptInterpreter string = "/bin/bash"

func createDirs() error {
	var err error
	err = os.MkdirAll(inputFileDir, 0755)
	if err != nil {
		return err
	}
	os.MkdirAll(outputFileDir, 0755)
	if err != nil {
		return err
	}
	return nil
}

// All fields need to be exported for gob serialization.
type TileEncodingJob struct {
	Filename string `form:"filename" binding:"required"`
	WalletAddress string `form:"wallet-address" binding:"required"`
	CreationDate time.Time
}

// Random number state.
// We generate random temporary file names so that there's a good
// chance the file doesn't exist yet - keeps the number of tries in
// TempFile to a minimum.
var rand uint32
var randmu sync.Mutex

func reseed() uint32 {
	return uint32(time.Now().UnixNano() + int64(os.Getpid()))
}

func nextRandom() string {
	randmu.Lock()
	r := rand
	if r == 0 {
		r = reseed()
	}
	r = r*1664525 + 1013904223 // constants from Numerical Recipes
	rand = r
	randmu.Unlock()
	return strconv.Itoa(int(1e9 + r%1e9))[1:]

}
func (job TileEncodingJob) randomizeFilename() error {
	newFilename := nextRandom() + job.Filename
	err := os.Rename(job.Filename, newFilename)
	job.Filename = newFilename
	return err
}

func setupHttpHandlers(router *gin.Engine, jobQueue *jobqueue.JobQueue) {
	router.GET("/jobs/enqueue", func(c *gin.Context) {
		job := TileEncodingJob{CreationDate: time.Now()}
		if err := c.ShouldBindQuery(&job); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		job.randomizeFilename()
		err := jobQueue.EnqueueJob(jobqueue.Job{Metadata: nil, Data: job}, 0)
		if err != nil {
			c.JSON(500, gin.H{"errorMessage": "Couldn't enqueue job", "extendedErrorMessage": err.Error()})
			return
		}
		c.JSON(200, job)
	})
}

func tileEncodeWorker(data interface{}) {
	job, ok := data.(TileEncodingJob)
	if !ok { panic("wonky data passed to worker!") }
	fmt.Println("*** encoding job started", job.Filename)
	inFile := filepath.Join(inputFileDir, job.Filename)
	outFile := filepath.Join(outputFileDir, job.Filename)
	fmt.Println("running: ", scriptInterpreter, tileScriptPath, inFile, outFile)
	cmd := exec.Command(scriptInterpreter, tileScriptPath, inFile, outFile)
	// TODO: catch errors reported by ffmpeg
	err := cmd.Run()
	if err != nil { panic(err) }
	fmt.Println("*** encoding job finished", job.Filename)
}

func makeWorker(metadata interface{}) jobqueue.Worker {
	return tileEncodeWorker
}

func main() {
    var err error

	createDirs()

    jobQueue, err := jobqueue.OpenJobQueue(jobQueueDir)
    if err != nil { panic("cannot open jobqueue data dir") }
    defer jobQueue.Close()

	jobQueue.Start(makeWorker)
	router := gin.Default()
    setupHttpHandlers(router, jobQueue)
    fmt.Println("Starting server on port 3334")
    router.Run(":3334")

}

