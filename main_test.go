package main

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestGetColumnNames(t *testing.T) {
	var copyStatement = "COPY public.\"EmailHistories\" (\"Id\", \"UserId\", \"Email\", \"Created\", \"LastUpdated\") FROM stdin;"

	result, err := GetColumnNames(copyStatement)

	assert.Nil(t, err)
	assert.Equal(t, 5, len(result))
	assert.Equal(t, "Id", result[0])
	assert.Equal(t, "UserId", result[1])
	assert.Equal(t, "Email", result[2])
	assert.Equal(t, "Created", result[3])
	assert.Equal(t, "LastUpdated", result[4])
}

func TestGetTableNameFromStatement(t *testing.T) {
	var copyStatement = "COPY public.\"EmailHistories\" (\"Id\", \"UserId\", \"Email\", \"Created\", \"LastUpdated\") FROM stdin;"
	result, err := GetTableNameFromStatement(copyStatement)

	assert.Nil(t, err)
	assert.Equal(t, "public.\"EmailHistories\"", result)
}

func TestSanitizeStatement(t *testing.T) {
	var config = TableConfig{
		Columns: map[string]ColumnConfig{
			"Email": {
				Type: EmailColType,
			},
		},
	}

	var statement = "ac392482-5b4f-4d7d-8578-ed5df736d089	499833	abc123@somedomain.com	2024-04-29 11:37:25.34551+00	2024-04-29 11:37:25.345508+00"
	sanitizedStatement, err := SanitizeStatement(statement, &config, []string{"Id", "UserId", "Email", "Created", "LastUpdated"})

	assert.Nil(t, err)
	assert.NotEqual(t, statement, sanitizedStatement)
	assert.Equal(t, -1, strings.Index(sanitizedStatement, "abc123@somedomain.com"))
}

func TestSanitizeStatement_PersistValues(t *testing.T) {
	var config = TableConfig{
		Columns: map[string]ColumnConfig{
			"Email": {
				Type: EmailColType,
			},
		},
	}

	var statement = "ac392482-5b4f-4d7d-8578-ed5df736d089	499833	abc123@somedomain.com	2024-04-29 11:37:25.34551+00	2024-04-29 11:37:25.345508+00"
	sanitizedStatement, err := SanitizeStatement(statement, &config, []string{"Id", "UserId", "Email", "Created", "LastUpdated"})

	assert.Nil(t, err)
	assert.NotEqual(t, statement, sanitizedStatement)
	assert.Equal(t, -1, strings.Index(sanitizedStatement, "abc123@somedomain.com"))

	var statement2 = "54234	abc123@somedomain.com	Mixwithmagic	f	2024-03-26 14:32:35.247068+00	f	f	f	0	\\N	someothermail@domain.com			38KSmDrbJ8ZH20jG3omGrgFwYhIPMveMHBByKzrGyFA=	\\x9c5e95b3672167c8acbca144dfafe24f	\\N	\\N	2019-04-11 18:37:46+00	2024-03-26 14:33:23.487247+00	{fca7d99e-d9ae-4cde-94f4-8b4017e98c10}"
	sanitizedStatement2, err := SanitizeStatement(statement2, &config, []string{"Id", "Email", "ScreenName", "Verified", "LastLogin", "OptIn", "Deleted", "IsAdmin", "CreatedFrom", "CompanyInfo", "NewEmail", "FirstName", "LastName", "PasswordHash", "Salt", "OldPasswordHash", "ResetPasswordSecret", "Created", "LastUpdated", "VerificationSecrets"})

	assert.Nil(t, err)
	assert.NotEqual(t, statement2, sanitizedStatement2)
	assert.Equal(t, -1, strings.Index(sanitizedStatement2, "abc123@somedomain.com"))
	//assert.NotEqual(t, -1, strings.Index(sanitizedStatement2, persistedValues["abc123@somedomain.com"]))
}

func TestGetAnonymizedValue_LengthMustBeSame(t *testing.T) {
	str := "testing@example.com'||dbms_pipe.receive_message(chr(98)||chr(98)||chr(98),15)||'"
	val, err := GetAnonymizedValue(str, true)

	assert.Nil(t, err)
	assert.Equal(t, len([]rune(str)), len([]rune(val)))
}

func TestGetAnonymizedValue(t *testing.T) {
	str := "ABCあいう"
	assert.Equal(t, 12, len(str))
	assert.Equal(t, 6, len([]rune(str)))

	val, err := GetAnonymizedValue(str, false)

	assert.Nil(t, err)
	assert.NotEqual(t, str, val)
	assert.Equal(t, 12, len(val)) // hashed value should be in ascii and not contain any runes bigger than 1 byte
}

func TestGetNewJsonValue(t *testing.T) {
	var val = "{\"City\": \"Neuchatel\", \"TaxId\": \"SE123123-ABC\", \"Region\": \"\", \"PostalCode\": \"2000\", \"CompanyName\": \"MF Company Ltd\", \"AddressLine1\": \"Street1\"}"
	newVal, err := GetNewJsonValue(val, &[]string{"City", "TaxId"})

	assert.Nil(t, err)
	assert.NotEqual(t, val, newVal)

	var anyJson map[string]interface{}
	err = json.Unmarshal([]byte(newVal), &anyJson)

	assert.Nil(t, err)

	tid, ok := anyJson["TaxId"]
	assert.True(t, ok)
	assert.NotEqual(t, "SE123123-ABC", tid)

	city, ok := anyJson["City"]
	assert.True(t, ok)
	assert.NotEqual(t, "NEUCHATEL", city)
}

func TestSanitizeStatement_JsonColumn(t *testing.T) {
	var statement = "39157	VQ27nQc@lJSjn16.com	8gqVYW-y4b	t	2024-03-26 19:50:22.14547+00	t	f	f	0	{\"City\": \"Neuchatel\", \"TaxId\": \"SE123123-ABC\", \"Region\": \"\", \"PostalCode\": \"2000\", \"CompanyName\": \"MF Company Ltd\", \"AddressLine1\": \"Street1\"}	email@domain.com	Person Personsson	nz0RVJjlbBJpQ7BjUrOUxHpk3TbnygoXdPhgFcIWUdc=	\\x56722f8983026de83d0d3f13a32c7053	\\N	\\N	2018-08-23 08:58:18+00	2024-03-26 19:54:59.498137+00	{efb2f51b-6dd3-4248-815e-71b5ee9626aa}"
	var config = TableConfig{
		Columns: map[string]ColumnConfig{
			"CompanyInfo": {
				Type: JsonColType,
				Keys: &[]string{
					"CompanyName",
					"TaxId",
				},
			},
		},
	}

	sanitizedStatement, err := SanitizeStatement(statement, &config, []string{"Id", "Email", "ScreenName", "Verified", "LastLogin", "OptIn", "Deleted", "IsAdmin", "CreatedFrom", "CompanyInfo", "NewEmail", "FirstName", "LastName", "PasswordHash", "Salt", "OldPasswordHash", "ResetPasswordSecret", "Created", "LastUpdated", "VerificationSecrets"})

	assert.Nil(t, err)
	assert.NotEqual(t, sanitizedStatement, statement)
	assert.Equal(t, -1, strings.Index(sanitizedStatement, "SE123123-ABC"), "Make sure tax id is removed")
	assert.Equal(t, -1, strings.Index(sanitizedStatement, "MF Company Ltd"), "Make sure tax id is removed")

	values := strings.Split(sanitizedStatement, "\t")
	j := values[9]

	var anyJson map[string]interface{}
	err = json.Unmarshal([]byte(j), &anyJson)

	assert.Nil(t, err)
	assert.NotEqual(t, "SE123123-ABC", anyJson["TaxId"])
	assert.NotEqual(t, "MF Company Ltd", anyJson["CompanyName"])
}

func TestSanitizeStatement_TextArrayColumn(t *testing.T) {
	result, err := GetNewTextArrayValue("{abc123,untzxxx123}")
	assert.Nil(t, err)
	assert.Equal(t, -1, strings.Index(result, "abc123"))
	assert.Equal(t, -1, strings.Index(result, "untzxxx123"))
}

func TestSanitizeStatement_WithSuffix(t *testing.T) {
	var statement = "BG785VXY-TEEXCWMF-92V0PHMS-D8RSTGGY-6CJESGTE-96VXBG2D-NH3QS158-55WZA1YP-20240612-TRIAL\t507870\tkpeq\t\\N\t\\N\t\\N\t2024-06-12 01:54:58.820788+00\tf\tf\tf\tt\tf\tf\t\\N\t\\N\t\\N\t2024-06-02 01:54:58.824981+00\t2024-06-02 01:54:58.824981+00"
	var config = TableConfig{
		Columns: map[string]ColumnConfig{
			"Key": {
				Type: TextColType,
				Suffixes: &[]string{
					"-TRIAL",
					"-NFR",
				},
			},
		},
	}

	var colNames = []string{"Key", "UserId", "ProductId", "OrderId", "LegacyOrderId", "SubscriptionId", "Expires", "Beta", "Nfr", "Revoked", "Trial", "LegacySubscription", "GiveAway", "FulfillmentReference", "TransferredTo", "TransferredFrom", "Created", "LastUpdated"}

	sanitizedStatement, err := SanitizeStatement(statement, &config, colNames)

	assert.Nil(t, err)
	assert.Equal(t, -1, strings.Index(sanitizedStatement, "BG785VXY-TEEXCWMF-92V0PHMS-D8RSTGGY-6CJESGTE-96VXBG2D-NH3QS158-55WZA1YP-20240612-TRIAL"))

	newVals := strings.Split(sanitizedStatement, "\t")

	assert.True(t, strings.HasSuffix(newVals[0], "-TRIAL"))
}

func TestSanitizeStatement_PersistWithSuffix(t *testing.T) {
	var statement = "BG785VXY-TEEXCWMF-92V0PHMS-D8RSTGGY-6CJESGTE-96VXBG2D-NH3QS158-55WZA1YP-20240612-TRIAL"
	var config = TableConfig{
		Columns: map[string]ColumnConfig{
			"Key": {
				Type: TextColType,
				Suffixes: &[]string{
					"-TRIAL",
				},
			},
		},
	}

	var colNames = []string{"Key"}

	sanitizedStatement, err := SanitizeStatement(statement, &config, colNames)

	assert.Nil(t, err)
	assert.NotEqual(t, sanitizedStatement, statement)
	assert.True(t, strings.HasSuffix(sanitizedStatement, "-TRIAL"))
}

func TestSanitizeStatement_SetNull(t *testing.T) {
	var statement = "BG785VXY\t507870\tkpeq\t\\N\t\\N\t\\N\t2024-06-12 01:54:58.820788+00\tf\tf\tf\tt\tf\tf\t\\N\t\\N\t\\N\t2024-06-02 01:54:58.824981+00\t2024-06-02 01:54:58.824981+00"
	var config = TableConfig{
		Columns: map[string]ColumnConfig{
			"Key": {
				Type:    TextColType,
				SetNull: true,
			},
		},
	}
	var colNames = []string{"Key", "UserId", "ProductId", "OrderId", "LegacyOrderId", "SubscriptionId", "Expires", "Beta", "Nfr", "Revoked", "Trial", "LegacySubscription", "GiveAway", "FulfillmentReference", "TransferredTo", "TransferredFrom", "Created", "LastUpdated"}

	sanitizedStatement, err := SanitizeStatement(statement, &config, colNames)

	assert.Nil(t, err)
	assert.NotEqual(t, sanitizedStatement, statement)

	values := strings.Split(sanitizedStatement, "\t")
	assert.Equal(t, "\\N", values[0])
}
