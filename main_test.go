package main

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestGetColumnIndices(t *testing.T) {
	var copyStatement = "COPY public.\"EmailHistories\" (\"Id\", \"UserId\", \"Email\", \"Created\", \"LastUpdated\") FROM stdin;"

	result := GetColumnIndices([]string{"Email"}, copyStatement)

	assert.Equal(t, 1, len(result))
	assert.Equal(t, 2, result[0])
}

func TestGetTableNameFromStatement(t *testing.T) {
	var copyStatement = "COPY public.\"EmailHistories\" (\"Id\", \"UserId\", \"Email\", \"Created\", \"LastUpdated\") FROM stdin;"
	result := GetTableNameFromStatement(copyStatement)

	assert.Equal(t, "public.\"EmailHistories\"", result)
}

func TestSanitizeStatement(t *testing.T) {
	var statement = "ac392482-5b4f-4d7d-8578-ed5df736d089	499833	abc123@somedomain.com	2024-04-29 11:37:25.34551+00	2024-04-29 11:37:25.345508+00"
	sanitizedStatement := SanitizeStatement(statement, []int{2})

	assert.NotEqual(t, statement, sanitizedStatement)
	assert.Equal(t, -1, strings.Index(sanitizedStatement, "abc123@somedomain.com"))
}
