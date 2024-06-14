package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
)

func GetTableNameFromStatement(statement string) string {
	if !strings.HasPrefix(statement, "COPY ") {
		log.Fatalf("statement not a copy statement")
	}

	substr := strings.Split(statement, " ")

	return substr[1]
}

func GetColumnNames(copyStatement string) []string {
	result := make([]string, 0)
	s := strings.Index(copyStatement, "(")
	e := strings.Index(copyStatement, ")")

	if s == -1 || e == -1 {
		log.Fatalf("Unexpected copy statement: %s", copyStatement)
	}

	c := copyStatement[s+1 : e]
	cs := strings.Split(c, ", ")

	if len(cs) < 1 {
		log.Fatalf("Could not split copy statement columns")
	}

	for _, val := range cs {
		result = append(result, strings.Trim(val, "\""))
	}

	return result
}

func GenerateRandomString(length int) string {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	ret := make([]byte, length)
	for i := 0; i < length; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			log.Fatalf("Error generating random number: %v", err)
		}
		ret[i] = letters[num.Int64()]
	}

	return string(ret)
}

func GetNewValue(isEmail bool) string {
	if isEmail {
		return fmt.Sprintf("%s@%s.com", GenerateRandomString(7), GenerateRandomString(7))
	} else {
		return GenerateRandomString(10)
	}
}

func SanitizeStatement(statement string, columnInfos *[]ColumnInfo, columnNames []string, persistedValues map[string]string) string {
	values := strings.Split(statement, "\t")
	var result = make([]string, len(values))

	for i, val := range values {
		newVal := ""
		colName := columnNames[i]

		// null values can be ignored
		if val == "\\N" {
			result[i] = val
			continue
		}

		for _, info := range *columnInfos {
			if info.Name == colName {

				if info.Persist {
					persistedValue, persisted := persistedValues[val]

					if persisted {
						newVal = persistedValue
					} else {
						newVal = GetNewValue(info.Type == EmailColType)
						persistedValues[val] = newVal
					}
				} else {
					newVal = GetNewValue(info.Type == EmailColType)
				}

				break
			}
		}

		if len(newVal) > 0 {
			result[i] = newVal
		} else {
			result[i] = val
		}
	}

	return strings.Join(result, "\t")
}

const (
	EmailColType string = "email"
	TextColType  string = "text"
)

type ColumnInfo struct {
	Name    string
	Type    string
	Persist bool
}

func main() {
	var colums = map[string][]ColumnInfo{
		"public.\"EmailHistories\"": {
			ColumnInfo{
				Name:    "Email",
				Persist: true,
				Type:    EmailColType,
			},
		},
		"public.\"Users\"": {
			ColumnInfo{
				Name:    "Email",
				Persist: true,
				Type:    EmailColType,
			},
			ColumnInfo{
				Name:    "NewEmail",
				Persist: true,
				Type:    EmailColType,
			},
			ColumnInfo{
				Name:    "ScreenName",
				Persist: false,
				Type:    TextColType,
			},
		},
	}

	filePath := os.Args[1]

	if len(filePath) == 0 {
		log.Fatal("First argument must be path to sql file")
	}

	file, err := os.Open(filePath)

	if err != nil {
		log.Fatalf("Faile to open file: %s", err.Error())
	}

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	scanner.Split(bufio.ScanLines)

	var statement = ""
	var currentTable = ""
	var currentColumns []string
	var persistedValues = make(map[string]string)

	for scanner.Scan() {
		line := scanner.Text()

		if len(line) == 0 || strings.HasPrefix(line, "--") {
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
					currentColumns = GetColumnNames(statement)
				}
			}
		} else if len(currentTable) > 0 {
			// this is an insert
			tableName := GetTableNameFromStatement(currentTable)

			columnInfos, ok := colums[tableName]

			if ok {
				statement = SanitizeStatement(statement, &columnInfos, currentColumns, persistedValues)
			}
		}

		fmt.Printf("%s\n", statement)
		statement = ""
	}
}
