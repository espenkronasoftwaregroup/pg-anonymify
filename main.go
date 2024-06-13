package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func GetTableNameFromStatement(statement string) string {
	if !strings.HasPrefix(statement, "COPY ") {
		log.Fatal("statement not a copy statement")
	}

	substr := strings.Split(statement, " ")

	return substr[1]
}

func GetColumnIndices(cols []string, copyStatement string) []int {
	result := make([]int, 0)
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

	for i, val := range cs {
		for _, col := range cols {
			if strings.Trim(strings.TrimSpace(val), "\"") == col {
				result = append(result, i)
				break
			}
		}
	}

	return result
}

func SanitizeStatement(statement string, cols []int) string {

	return statement
}

func main() {
	var colums = map[string][]string{
		"public.\"EmailHistories\"": {
			"Email",
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
	scanner.Split(bufio.ScanLines)

	var statement = ""
	var currentTable = ""

	for scanner.Scan() {
		line := scanner.Text()

		if len(line) == 0 || strings.HasPrefix(line, "--") {
			currentTable = ""
			continue
		}

		statement += line

		if strings.HasPrefix(line, "41389\t") {
			fmt.Print("-- hej")
		}

		// statement has ended, print it
		if strings.HasSuffix(line, ";") {
			if len(currentTable) == 0 {
				if strings.HasPrefix(statement, "COPY ") {
					currentTable = statement
				}
			}
		} else if len(currentTable) > 0 {
			// this is an insert
			tableName := GetTableNameFromStatement(currentTable)

			columnNames, ok := colums[tableName]

			if ok {
				columnIndices := GetColumnIndices(columnNames, currentTable)
				statement = SanitizeStatement(statement, columnIndices)
			}
		}

		fmt.Printf("%s\n", statement)
		statement = ""
	}
}
