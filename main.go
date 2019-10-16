package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strings"
)

var (
	filename          string
	fieldFormatString string
	delimiter         string
	outputLines       int
	blankColString    string
	definedFormat     string
	samplePercentage  int
	displayHeader     bool
)

type PRStruct struct {
	formats    []string
	headerCols []string
	blankCols  []bool
}

type LineStruct struct {
	fields  []string
	lineNum int
}

func HandleDefinedFormat(format string) {
	switch format {
	case "backup":
		delimiter = "|"
		fieldFormatString = "uuid,ad_id_type,app_id,app_id,uuid,user_id,text,text,os,version,am_type,ip_addr,ts_sec,int,ts_sec,uuid,int,int,float,float,cc,text,text,text,int,loc_context,loc_method,text,text,int,int,int,int"
		blankColString = "11,14,15,16,17,20,22,23"
	}
	log.Printf("Acceptable formats: backup")
	log.Printf("field-format: %s\n", fieldFormatString)
	log.Printf("delimeter: '%s' blank-cols: %s\n\n", delimiter, blankColString)
}

func PrintHeaderCols(headerCols []string, formats []string) {
	log.Printf("Header\n")
	log.Printf("------\n")
	for index, col := range headerCols {
		log.Printf("%02d: %s (%s)\n", index, col, formats[index])
	}
	log.Printf("\n")
}

func CreateBlankColArray(blankColString string, numCols int) []bool {
	blankCols := make([]bool, numCols)

	for i := 0; i < numCols; i++ {
		colToFind := fmt.Sprintf("%d(,|$)", i)
		re := regexp.MustCompile(colToFind)
		blankCols[i] = re.MatchString(blankColString)
	}

	return blankCols
}

func CheckFormat(regex string, field string, fieldType string, headerName string, lineNum int, col int) {
	re := regexp.MustCompile(regex)
	if !re.MatchString(field) {
		log.Printf("L:%d C:%d H:%s \"%s\" not formatted as \"%s\" type\n", lineNum, col, headerName, field, fieldType)
	}
}

func Validate(proofreaderData *PRStruct, lineChan <-chan LineStruct) {
	for line := range lineChan {
		lenLine := len(line.fields)
		lenFormat := len(proofreaderData.formats)
		if lenLine != lenFormat {
			log.Printf("Items in line(%d) != Items in format(%d)\n", lenLine, lenFormat)
		}

		for index, field := range line.fields {
			if proofreaderData.blankCols[index] && field == "" {
				continue
			}
			format := proofreaderData.formats[index]
			var regex string
			switch format {
			case "uuid":
				regex = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
			case "app_id":
				regex = "[0-9a-fA-F]{64}"
			case "user_id":
				regex = "[0-9a-fA-F]{32}"
			case "os":
				regex = "(IOS|AND)"
			case "version":
				regex = "(\\d+\\.)?(\\d+\\.)?(\\*|\\d+)"
			case "ad_id_type":
				regex = "(IDFA|AAID)"
			case "am_type":
				regex = "[a-z]{2}"
			case "ip_addr":
				regex = "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}"
			case "ts_sec":
				regex = "[0-9]{10}"
			case "ts_msec":
				regex = "[0-9]{13}"
			case "cc":
				regex = "[A-Z]{2}"
			case "loc_context":
				regex = "(fore|back)ground"
			case "loc_method":
				regex = "(BCN|GPS)"
			case "exchange":
				regex = "(ASX|BIT|BVMF|CPH|Euronext|FWB|LSE|BMAD|NASDAQ|TSX|NYSE|TYO|SIX|OTC Pink|STO)"
			case "ticker":
				regex = ".{7}"
			case "lat":
				regex = "[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)"
			case "lon":
				regex = "[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)"
			case "int":
				regex = "(0|\\d+)"
			case "float":
				regex = "(0|[-+]?[0-9]*\\.?[0-9]+.)"
			case "num":
				regex = "(\\d+(\\.\\d+)?)"
			case "text":
				fallthrough
			case "SKIP":
				break
			default:
				errMsg := "[ERROR] Unrecognized format - " + format
				panic(errMsg)
			}
			CheckFormat(regex, field, format, proofreaderData.headerCols[index], line.lineNum, index)
		}
	}
}

/*
#  arg :dau_cols, 'ad_id, lat, lon column numbers for tracking dau', :alias => :dcarg :blank_cols, 'allow blank values in these columns', :alias => :bc
#  arg :country_code, 'countries to track dau', :alias => :cc, :default => 'US,CA,GB,FR,DE,IT,ES'
arg :skip_lines, "skip first 'x' lines", :alias => :sl, :default => 0
*/
func main() {
	flag.StringVar(&filename, "filename", "input.csv.gz", "filename to proofread")
	flag.StringVar(&fieldFormatString, "field-format", "uuid,ad_id_type,app_id,app_id,uuid,user_id,text,text,os,version,am_type,ip_addr,ts_sec,num,ts_msec,uuid,num,num,ll,cc,num,loc_context,loc_method,text,text", "filename to proofread")
	flag.StringVar(&delimiter, "delimiter", "|", "delimiter")
	flag.IntVar(&outputLines, "output-lines", 100, "output processing output every 'x' lines")
	flag.StringVar(&blankColString, "blank-cols", "", "allow blanks values in specified columns")
	flag.StringVar(&definedFormat, "defined-format", "", "use predefined format and delimter")
	flag.IntVar(&samplePercentage, "sample-percentage", 100, "randomly validate 'x' percentage of data")
	flag.BoolVar(&displayHeader, "display-header", false, "display header of data file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if definedFormat != "" {
		HandleDefinedFormat(definedFormat)
	}

	// Open target file
	f, _ := os.Open(filename)
	gzr, err := gzip.NewReader(f)
	if err != nil {
		log.Printf("[ERROR] Could not find input file %s (%s)\n", filename, err)
		os.Exit(128)
	}
	r := csv.NewReader(gzr)
	r.Comma = rune(delimiter[0])
	header, _ := r.Read()

	var proofreaderData PRStruct
	proofreaderData.formats = strings.Split(fieldFormatString, ",")
	proofreaderData.headerCols = header
	proofreaderData.blankCols = CreateBlankColArray(blankColString, len(proofreaderData.formats))

	if displayHeader {
		PrintHeaderCols(header, proofreaderData.formats)
	}

	lineChan := make(chan LineStruct)
	go Validate(&proofreaderData, lineChan)

	log.Printf("Start processing - %s", filename)
	lineNum := 2
	// Read data file
	for {
		if (lineNum % outputLines) == 0 {
			log.Printf("Reading line number %d", lineNum)
		}
		line, error := r.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		if rand.Intn(100) <= samplePercentage {
			lineChan <- LineStruct{line, lineNum}
		}
		lineNum += 1
	}

	close(lineChan)
}
