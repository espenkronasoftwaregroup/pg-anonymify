package main

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestGetColumnNames(t *testing.T) {
	var copyStatement = "COPY public.\"EmailHistories\" (\"Id\", \"UserId\", \"Email\", \"Created\", \"LastUpdated\") FROM stdin;"

	result := GetColumnNames(copyStatement)

	assert.Equal(t, 5, len(result))
	assert.Equal(t, "Id", result[0])
	assert.Equal(t, "UserId", result[1])
	assert.Equal(t, "Email", result[2])
	assert.Equal(t, "Created", result[3])
	assert.Equal(t, "LastUpdated", result[4])
}

func TestGetTableNameFromStatement(t *testing.T) {
	var copyStatement = "COPY public.\"EmailHistories\" (\"Id\", \"UserId\", \"Email\", \"Created\", \"LastUpdated\") FROM stdin;"
	result := GetTableNameFromStatement(copyStatement)

	assert.Equal(t, "public.\"EmailHistories\"", result)
}

func TestSanitizeStatement(t *testing.T) {
	var columns = []ColumnInfo{
		{
			Name:    "Email",
			Persist: true,
			Type:    EmailColType,
		},
	}
	var statement = "ac392482-5b4f-4d7d-8578-ed5df736d089	499833	abc123@somedomain.com	2024-04-29 11:37:25.34551+00	2024-04-29 11:37:25.345508+00"
	sanitizedStatement := SanitizeStatement(statement, &columns, []string{"Id", "UserId", "Email", "Created", "LastUpdated"}, map[string]string{})

	assert.NotEqual(t, statement, sanitizedStatement)
	assert.Equal(t, -1, strings.Index(sanitizedStatement, "abc123@somedomain.com"))
}

func TestSanitizeStatement_PersistValues(t *testing.T) {
	var columns = []ColumnInfo{
		{
			Name:    "Email",
			Persist: true,
			Type:    EmailColType,
		},
	}
	var persistedValues = make(map[string]string)

	var statement = "ac392482-5b4f-4d7d-8578-ed5df736d089	499833	abc123@somedomain.com	2024-04-29 11:37:25.34551+00	2024-04-29 11:37:25.345508+00"
	sanitizedStatement := SanitizeStatement(statement, &columns, []string{"Id", "UserId", "Email", "Created", "LastUpdated"}, persistedValues)

	assert.NotEqual(t, statement, sanitizedStatement)
	assert.Equal(t, -1, strings.Index(sanitizedStatement, "abc123@somedomain.com"))

	pv, p := persistedValues["abc123@somedomain.com"]

	assert.True(t, p)
	assert.NotEqual(t, pv, "abc123@somedomain.com")

	var statement2 = "54234	abc123@somedomain.com	Mixwithmagic	f	2024-03-26 14:32:35.247068+00	f	f	f	0	\\N	someothermail@domain.com			38KSmDrbJ8ZH20jG3omGrgFwYhIPMveMHBByKzrGyFA=	\\x9c5e95b3672167c8acbca144dfafe24f	\\N	\\N	2019-04-11 18:37:46+00	2024-03-26 14:33:23.487247+00	{fca7d99e-d9ae-4cde-94f4-8b4017e98c10}"
	sanitizedStatement2 := SanitizeStatement(statement2, &columns, []string{"Id", "Email", "ScreenName", "Verified", "LastLogin", "OptIn", "Deleted", "IsAdmin", "CreatedFrom", "CompanyInfo", "NewEmail", "FirstName", "LastName", "PasswordHash", "Salt", "OldPasswordHash", "ResetPasswordSecret", "Created", "LastUpdated", "VerificationSecrets"}, persistedValues)

	assert.NotEqual(t, statement2, sanitizedStatement2)
	assert.Equal(t, -1, strings.Index(sanitizedStatement2, "abc123@somedomain.com"))
	assert.NotEqual(t, -1, strings.Index(sanitizedStatement2, persistedValues["abc123@somedomain.com"]))
}
