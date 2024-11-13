package main

import (
	"math/rand/v2"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
)

const (
	_MAX_AGE = 100
)

type User struct {
	ID    uuid.UUID
	Name  string
	Email string
	Age   int
}

func CreateFakeUser() User {
	return User{
		ID:    uuid.New(),
		Name:  gofakeit.Name(),
		Email: gofakeit.Email(),
		Age:   rand.IntN(_MAX_AGE),
	}
}
