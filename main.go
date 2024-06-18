package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
	"unsafe"
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

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
var src = rand.NewSource(time.Now().UnixNano())

func GenerateRandomString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return *(*string)(unsafe.Pointer(&b))
}

func GetNewValue(isEmail bool, length int) string {
	if isEmail {
		return fmt.Sprintf("%s@%s.com", GenerateRandomString(7), GenerateRandomString(7))
	} else {
		return GenerateRandomString(length)
	}
}

func GetNewJsonValue(value string, keys *[]string) string {
	if keys == nil || len(*keys) == 0 {
		return "\\N"
	}

	var anyJson map[string]interface{}
	err := json.Unmarshal([]byte(value), &anyJson)

	if err != nil {
		log.Fatalf("Error parsing json: %v", err)
	}

	for _, key := range *keys {
		_, ok := anyJson[key]

		if ok {
			anyJson[key] = GetNewValue(false, 10)
		}
	}

	bytes, err := json.Marshal(anyJson)

	if err != nil {
		log.Fatalf("Error serializing to json: %v", err)
	}

	return string(bytes)
}

func GetNewTextArrayValue(value string, persistValues bool, persistedValues map[string]string) string {
	val := strings.Trim(value, "{")
	val = strings.Trim(val, "}")
	vals := strings.Split(val, ",")
	result := "{"

	for _, v := range vals {
		if persistValues {
			x, ok := persistedValues[v]

			if !ok {
				x = GetNewValue(false, len(v))
				persistedValues[v] = x
			}

			result += x + ","
		} else {
			result += GetNewValue(false, len(v))
			result += ","
		}
	}

	result = strings.Trim(result, ",")
	result += "}"

	return result
}

// SanitizeStatement Sanitize values in a copy statement from a pg-dump.
// Pass the statement, info about how and which column values should be replaced, names of the columns taken from the
// first line of the copy statement and a map of previously persisted values.
func SanitizeStatement(statement string, columnInfos *[]ColumnInfo, columnNames []string, persistedValues map[string]string) string {
	columnValue := strings.Split(statement, "\t")
	var result = make([]string, len(columnValue))

	for i, val := range columnValue {
		newVal := ""
		colName := columnNames[i]

		// null columnValue can be ignored
		if val == "\\N" {
			result[i] = val
			continue
		}

		for _, info := range *columnInfos {
			if info.Name == colName {

				valLen := len(val)
				if info.MaxLength > 0 && valLen > info.MaxLength {
					valLen = info.MaxLength
				}

				if info.Type == JsonColType {
					newVal = GetNewJsonValue(val, info.Keys)
				} else if info.Type == TextArrayColType {
					newVal = GetNewTextArrayValue(val, info.Persist, persistedValues)
				} else {
					if info.Persist {
						persistedValue, persisted := persistedValues[val]
						if persisted {
							newVal = persistedValue
						} else {
							newVal = GetNewValue(info.Type == EmailColType, valLen)
							persistedValues[val] = newVal
						}
					} else {
						newVal = GetNewValue(info.Type == EmailColType, valLen)
					}
				}

				// check if org value have a suffix that we want to keep
				if info.Suffixes != nil && len(*info.Suffixes) > 0 {
					for _, suffix := range *info.Suffixes {
						if strings.HasSuffix(val, suffix) {
							newVal = newVal[0:len(newVal)-len(suffix)] + suffix
							if info.Persist {
								persistedValues[val] = newVal
							}

							break
						}
					}
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
	EmailColType     string = "email"
	TextColType      string = "text"
	JsonColType      string = "json"
	TextArrayColType string = "text_array"
)

type ColumnInfo struct {
	Name      string
	Type      string
	Persist   bool
	Keys      *[]string
	Suffixes  *[]string
	MaxLength int
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
				Name:      "ScreenName",
				Persist:   false,
				Type:      TextColType,
				MaxLength: 64,
			},
			ColumnInfo{
				Name:    "CompanyInfo",
				Persist: false,
				Type:    JsonColType,
				Keys: &[]string{
					"TaxId",
					"PostalCode",
					"CompanyName",
					"AddressLine1",
				},
			},
		},
		"public.\"LicenseKeys\"": {
			ColumnInfo{
				Name:    "Key",
				Persist: true,
				Type:    TextColType,
				Suffixes: &[]string{
					"-TRIAL",
					"-NFR",
					"-BETA",
					"-SUB",
				},
			},
		},
		"public.\"Orders\"": {
			ColumnInfo{
				Name:    "Email",
				Persist: true,
				Type:    EmailColType,
			},
			ColumnInfo{
				Name:    "TaxId",
				Persist: true,
				Type:    TextColType,
			},
			ColumnInfo{
				Name:    "RecipientEmail",
				Persist: true,
				Type:    EmailColType,
			},
		},
		"public.\"Subscriptions\"": {
			ColumnInfo{
				Name:    "Email",
				Persist: true,
				Type:    EmailColType,
			},
			ColumnInfo{
				Name:    "TaxId",
				Persist: true,
				Type:    TextColType,
			},
		},
		"public.\"TransferRequests\"": {
			ColumnInfo{
				Name:    "Keys",
				Persist: true,
				Type:    TextArrayColType,
			},
		},
		"public.\"Redemptions\"": {
			ColumnInfo{
				Name:    "LicenseKey",
				Persist: true,
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

	writer := bufio.NewWriter(os.Stdout)
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	scanner.Split(bufio.ScanLines)

	var statement = ""
	var currentTable = ""
	var currentColumns []string
	var persistedValues = make(map[string]string)
	var line = ""

	for scanner.Scan() {
		line = scanner.Text()

		if line == "\\." {
			writer.Write([]byte(line))
			writer.WriteByte('\n')
			writer.Flush()
			currentTable = ""
			currentColumns = make([]string, 0)
			continue
		}

		if len(line) == 0 || strings.HasPrefix(line, "--") {
			writer.Flush()
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

		writer.Write([]byte(statement))
		writer.WriteByte('\n')
		statement = ""
	}
}
