package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"golang.org/x/crypto/sha3"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

const (
	EmailColType     string = "email"
	TextColType      string = "text"
	JsonColType      string = "json"
	TextArrayColType string = "text_array"
)

type ColumnConfig struct {
	Type     string    `json:"type"`
	SetNull  bool      `json:"set_null"`
	Suffixes *[]string `json:"suffixes"`
	Keys     *[]string `json:"keys"`
}

type TableConfig struct {
	Columns    map[string]ColumnConfig `json:"columns"`
	IgnoreRows map[string][]string     `json:"ignore_rows"`
}

var version string
var pepper = []byte(strconv.FormatFloat(rand.Float64(), 'f', 7, 64))
var hasher = sha3.NewCShake128([]byte{}, pepper)

func GetTableNameFromStatement(statement string) (string, error) {
	if !strings.HasPrefix(statement, "COPY ") {
		return "", errors.New("statement not a copy statement")
	}

	substr := strings.Split(statement, " ")

	return substr[1], nil
}

func GetColumnNames(copyStatement string) ([]string, error) {
	result := make([]string, 0)
	s := strings.Index(copyStatement, "(")
	e := strings.Index(copyStatement, ")")

	if s == -1 || e == -1 {
		return []string{}, errors.New(fmt.Sprintf("unexpected copy statement: %s", copyStatement))
	}

	c := copyStatement[s+1 : e]
	cs := strings.Split(c, ", ")

	if len(cs) < 1 {
		return []string{}, errors.New("could not split copy statement columns")
	}

	for _, val := range cs {
		result = append(result, strings.Trim(val, "\""))
	}

	return result, nil
}

func hashString(val string, hashLength int) (string, error) {
	out := make([]byte, hashLength)
	_, err := hasher.Write([]byte(val))

	if err != nil {
		return "", err
	}

	_, err = hasher.Read(out)

	if err != nil {
		return "", err
	}

	hasher.Reset()

	return hex.EncodeToString(out), nil
}

func GetAnonymizedValue(val string, isEmail bool) (string, error) {
	if isEmail {
		parts := strings.Split(val, "@")

		if len(parts) != 2 {
			return "", errors.New(fmt.Sprintf("invalid email address: %s", val))
		}

		l := len([]rune(parts[0]))

		if l < 6 {
			l = 6
		}

		uname, e1 := hashString(parts[0], l)

		d := []rune(parts[1])

		if len(d) > 4 {
			d = d[0 : len(d)-4]
			d = append(d, '.', 'c', 'o', 'm')
		}

		domain, e2 := hashString(string(d), len(d))

		if e1 != nil {
			return "", e1
		}

		if e2 != nil {
			return "", e2
		}

		return fmt.Sprintf("%s@%s.com", uname, domain), nil
	} else {
		h, err := hashString(val, len([]rune(val)))

		if err != nil {
			return "", err
		}

		return h, nil
	}
}

func GetNewJsonValue(value string, keys *[]string) (string, error) {
	if keys == nil || len(*keys) == 0 {
		return "\\N", nil
	}

	var anyJson map[string]interface{}
	err := json.Unmarshal([]byte(value), &anyJson)

	if err != nil {
		return "", nil
	}

	for _, key := range *keys {
		_, ok := anyJson[key]

		if ok {
			anyJson[key], err = GetAnonymizedValue(value, false)

			if err != nil {
				return "", err
			}
		}
	}

	bytes, err := json.Marshal(anyJson)

	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func GetNewTextArrayValue(value string) (string, error) {
	val := strings.Trim(value, "{")
	val = strings.Trim(val, "}")
	vals := strings.Split(val, ",")
	result := "{"

	for _, v := range vals {
		res, err := GetAnonymizedValue(v, false)

		if err != nil {
			return "", err
		}

		result += res
		result += ","
	}

	result = strings.Trim(result, ",")
	result += "}"

	return result, nil
}

// SanitizeStatement Sanitize values in a copy statement from a pg-dump.
// Pass the statement, info about how and which column values should be replaced, names of the columns taken from the
// first line of the copy statement and a map of previously persisted values.
func SanitizeStatement(statement string, tableConfig *TableConfig, columnNames []string) (string, error) {
	columnValue := strings.Split(statement, "\t")
	var result = make([]string, len(columnValue))

	for i, val := range columnValue {
		newVal := ""
		colName := columnNames[i]

		if tableConfig.IgnoreRows != nil {
			ignoreValues, ok := tableConfig.IgnoreRows[colName]

			if ok {
				for _, ignore := range ignoreValues {
					if val == ignore {
						// if we find a value that says the row should be ignored, just return the unprocessed statement
						return statement, nil
					}
				}
			}
		}

		// null columnValue can be ignored
		if val == "\\N" {
			result[i] = val
			continue
		}

		colConfig, ok := tableConfig.Columns[colName]

		if ok {
			if colConfig.SetNull {
				newVal = "\\N"
			} else {
				var err error

				if colConfig.Type == JsonColType {
					newVal, err = GetNewJsonValue(val, colConfig.Keys)
				} else if colConfig.Type == TextArrayColType {
					newVal, err = GetNewTextArrayValue(val)
				} else {
					newVal, err = GetAnonymizedValue(val, colConfig.Type == EmailColType)
				}

				if err != nil {
					return "", err
				}

				// check if org value have a suffix that we want to keep
				if colConfig.Suffixes != nil && len(*colConfig.Suffixes) > 0 {
					for _, suffix := range *colConfig.Suffixes {
						if strings.HasSuffix(val, suffix) {
							newVal = newVal[0:len(newVal)-len(suffix)] + suffix
							break
						}
					}
				}
			}
		}

		if len(newVal) > 0 {
			result[i] = newVal
		} else {
			result[i] = val
		}
	}

	return strings.Join(result, "\t"), nil
}

func readJsonConfig(filename string) (map[string]TableConfig, error) {

	file, err := os.Open(filename)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	bytes, err := io.ReadAll(file)

	if err != nil {
		return nil, err
	}

	var config map[string]TableConfig

	err = json.Unmarshal(bytes, &config)

	return config, err
}

func main() {
	filename := flag.String("config", "config.json", "Path to config json file")
	flag.Parse()
	filePath := flag.Arg(0)

	if filePath == "version" {
		fmt.Println("version: ", version)
		os.Exit(0)
	}

	config, err := readJsonConfig(*filename)

	if err != nil {
		panic(err)
	}

	if len(filePath) == 0 {
		panic("First argument must be path to sql file")
	}

	file, err := os.Open(filePath)

	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(os.Stdout)
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	scanner.Split(bufio.ScanLines)

	var statement = ""
	var currentTable = ""
	var currentColumns []string
	var line = ""

	for scanner.Scan() {
		line = scanner.Text()

		if line == "\\." {
			_, err = writer.Write([]byte(line))
			if err != nil {
				panic(err)
			}

			err = writer.WriteByte('\n')
			if err != nil {
				panic(err)
			}

			err = writer.Flush()
			if err != nil {
				panic(err)
			}

			currentTable = ""
			currentColumns = make([]string, 0)
			continue
		}

		if len(line) == 0 || strings.HasPrefix(line, "--") {
			err = writer.Flush()
			if err != nil {
				panic(err)
			}

			currentTable = ""
			currentColumns = make([]string, 0)
			continue
		}

		statement += line

		// statement has ended, print it
		if strings.HasSuffix(line, ";") {
			if len(currentTable) == 0 {
				if strings.HasPrefix(statement, "COPY ") {
					currentTable = statement
					currentColumns, err = GetColumnNames(statement)

					if err != nil {
						panic(err)
					}
				}
			}
		} else if len(currentTable) > 0 {
			// this is an insert
			tableName, terr := GetTableNameFromStatement(currentTable)

			if terr != nil {
				panic(terr)
			}

			tableConfig, ok := config[tableName]

			if ok {
				statement, err = SanitizeStatement(statement, &tableConfig, currentColumns)

				if err != nil {
					panic(err)
				}
			}
		}

		_, err = writer.Write([]byte(statement))

		if err != nil {
			panic(err)
		}

		err = writer.WriteByte('\n')

		if err != nil {
			panic(err)
		}

		statement = ""
	}
}
