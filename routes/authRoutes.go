package routes

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"strconv"
	"time"

	"github.com/avii09/hookit/models"

	"github.com/golang-jwt/jwt/v5"
	"github.com/pkg/errors"

	// "gofr.dev/pkg/errors"
	"gofr.dev/pkg/gofr"
	"golang.org/x/crypto/bcrypt"
)

var jwtKey = []byte("my_secret_key")

type Claims struct {
	Username string `json:"username"`
	jwt.RegisteredClaims
}

type AuthMiddlewareBody struct {
	Username string `json:"userEmail"`
}

// HashPassword returns hashed password generated from the password passed as argument to it
func HashPassword(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 14)
	return string(bytes), err
}

// RegisterUser creates a new user if user does not already exist in the database/**

func GenerateRandomAPIKey() (string, error) {
	// Generate a random 32-byte slice
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	// Encode the random bytes to a base64 string
	return base64.StdEncoding.EncodeToString(b), nil
}

func RegisterUser(ctx *gofr.Context) (interface{}, error) {
	var user models.UserRequestData
	err := ctx.Bind(&user)
	if err != nil {
		return nil, err
	}
	// Check if any user already exists with the same email
	rows, queryErr := ctx.SQL.QueryContext(ctx, "SELECT * FROM users WHERE email=$1", user.Email)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		break
	}

	if count > 0 {
		return nil, errors.New("User already exists")
	}

	// Hash the password
	hashPassword, hashErr := HashPassword(user.Pass)
	if hashErr != nil {
		return nil, hashErr
	}

	apiKey, apiKeyErr := GenerateRandomAPIKey()
	if apiKeyErr != nil {
		return nil, apiKeyErr
	}

	// Insert new user
	_, insertErr := ctx.SQL.ExecContext(ctx, "INSERT INTO users (name,email,hash_pass,api_key) VALUES ($1,$2,$3,$4)", user.Name, user.Email, hashPassword, apiKey)
	if insertErr != nil {
		return nil, insertErr
	}

	// Generate JWT token
	expirationTime := time.Now().Add(5 * 30 * 24 * time.Hour) // 5 months
	claims := &Claims{
		Username: user.Email,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, tokenErr := token.SignedString(jwtKey)
	if tokenErr != nil {
		return nil, tokenErr
	}

	// Success response
	success := map[string]string{
		"description": "User successfully created and logged in!",
		"token":       tokenString,
		"statusCode":  strconv.Itoa(http.StatusOK),
		// "data":
	}
	return success, nil
}

func LoginUser(ctx *gofr.Context) (interface{}, error) {
	var loginBody models.LoginBody
	err := ctx.Bind(&loginBody)
	if err != nil {
		return nil, err
	}

	//check if any user already exists with the same email
	rows, queryErr := ctx.SQL.QueryContext(ctx, "SELECT * FROM users WHERE email=?", loginBody.Email)

	if queryErr != nil {
		return nil, queryErr
	}

	var users []models.User
	for rows.Next() {
		var user models.User
		if err := rows.Scan(&user.Id, &user.Name, &user.Email, &user.HashPass); err != nil {
			return nil, err
		}

		users = append(users, user)
	}

	if len(users) == 0 {
		return nil, ctx.Err()
	} else {
		//user exists check for password

		var user = users[0]

		compareErr := bcrypt.CompareHashAndPassword([]byte(user.HashPass), []byte(loginBody.Pass))
		if compareErr != nil {
			//password does not match
			return nil, err
		} else {
			//password matches then generate jwt and send
			//expirationTime is of 5 months
			expirationTime := time.Now().Add(24 * 30 * 5 * time.Hour)
			// Create the JWT claims, which includes the username and expiry time
			claims := &Claims{
				Username: loginBody.Email,
				RegisteredClaims: jwt.RegisteredClaims{
					// In JWT, the expiry time is expressed as unix milliseconds
					ExpiresAt: jwt.NewNumericDate(expirationTime),
				},
			}

			token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
			tokenString, err := token.SignedString(jwtKey)
			if err != nil {
				// If there is an error in creating the JWT return an internal server error
				return nil, err
			}
			response := map[string]interface{}{
				"token":       tokenString,
				"description": "User created and logged in successfully",
				"data":        user,
			}
			return response, nil
		}
	}

}
