package main

import (
    "database/sql"
    "fmt"
    "net/http"
    "time"

    "github.com/dgrijalva/jwt-go"
    "github.com/gin-contrib/cors"
    "github.com/gin-gonic/gin"
    _ "github.com/lib/pq"
)

// Global variable for the database connection
var db *sql.DB

// TODO: need env to store secret key
var secretKey = []byte("secret")

const (
    host = "database"
    port     = 5432
    user     = "nt_user"
    password = "db123"
    dbname   = "nt_db"
)

type ErrorResponse struct {
    Success bool              `json:"success"`
    Data    map[string]string `json:"data"`
}

type Register struct {
    UserName string `json:"user_name"`
    Name     string `json:"name"`
    Password string `json:"password"`
}

type Login struct {
    UserName string `json:"user_name"`
    Password string `json:"password"`
}

type Response struct {
    Success bool                   `json:"success"`
    Data    map[string]interface{} `json:"data"`
}

type Claims struct {
    Name     string `json:"name"`
    UserName string `json:"user_name"`
    jwt.StandardClaims
}

func handleError(c *gin.Context, statusCode int, message string, err error) {
    errorResponse := ErrorResponse{
        Success: false,
        Data:    map[string]string{"error": message},
    }
    c.IndentedJSON(statusCode, errorResponse)
}

func createToken(name string, username string, expirationTime time.Time) (string, error) {
    claims := &Claims{
        Name:     name,
        UserName: username,
        StandardClaims: jwt.StandardClaims{
            ExpiresAt: expirationTime.Unix(),
        },
    }
    token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
    return token.SignedString(secretKey)
}

func postLogin(c *gin.Context) {
    var login Login

    if err := c.BindJSON(&login); err != nil {
        handleError(c, http.StatusBadRequest, "Invalid request body", err)
        return
    }

    fmt.Println("Using global database connection")

    var count int
    err := db.QueryRow("SELECT COUNT(*) FROM users WHERE user_name = $1", login.UserName).Scan(&count)
    if err != nil {
        handleError(c, http.StatusInternalServerError, "Failed to query the database", err)
        return
    }
    if count == 0 {
        handleError(c, http.StatusBadRequest, "Username does not exist", nil)
        return
    }

    var correctPassword bool
    err = db.QueryRow("SELECT (user_pass = crypt($1, user_pass)) AS is_valid FROM users WHERE user_name = $2", login.Password, login.UserName).Scan(&correctPassword)
    if err != nil {
        handleError(c, http.StatusInternalServerError, "Failed to query the database", err)
        return
    }
    if !correctPassword {
        handleError(c, http.StatusOK, "Incorrect password", nil)
        return
    }

    var name string
    err = db.QueryRow("SELECT name FROM users WHERE user_name = $1", login.UserName).Scan(&name)
    if err != nil {
        handleError(c, http.StatusInternalServerError, "Failed to query the database", err)
        return
    }

    minutes := 30 * time.Minute
    expirationTime := time.Now().Add(minutes)
    token, err := createToken(name, login.UserName, expirationTime)
    if err != nil {
        handleError(c, http.StatusInternalServerError, "Failed to create token", err)
        return
    }

    loginResponse := Response{
        Success: true,
        Data:    map[string]interface{}{"token": token},
    }

    c.IndentedJSON(http.StatusOK, loginResponse)
}

func postRegister(c *gin.Context) {
    var newRegister Register

    if err := c.BindJSON(&newRegister); err != nil {
        handleError(c, http.StatusBadRequest, "Invalid request body", err)
        return
    }

    fmt.Println("Using global database connection")

    var count int
    err := db.QueryRow("SELECT COUNT(*) FROM users WHERE user_name = $1", newRegister.UserName).Scan(&count)
    if err != nil {
        handleError(c, http.StatusInternalServerError, "Failed to query the database", err)
        return
    }
    if count > 0 {
        handleError(c, http.StatusOK, "Username already exists", nil)
        return
    }

    _, err = db.Exec("INSERT INTO users (user_name, name, user_pass) VALUES ($1, $2, $3)", newRegister.UserName, newRegister.Name, newRegister.Password)
    if err != nil {
        handleError(c, http.StatusInternalServerError, "Failed to insert new user to the database", err)
        return
    }

    successResponse := Response{
        Success: true,
        Data:    nil,
    }

    c.IndentedJSON(http.StatusCreated, successResponse)
}

func main() {
    postgresqlDbInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
    var err error
    db, err = sql.Open("postgres", postgresqlDbInfo)
    if err != nil {
        fmt.Printf("Failed to connect to the database: %v\n", err)
        return
    }
    defer db.Close()

    db.SetMaxOpenConns(100) // Set maximum number of open connections
    db.SetMaxIdleConns(5) // Set maximum number of idle connections

    router := gin.Default()

    config := cors.DefaultConfig()
    config.AllowOrigins = []string{"http://localhost:3000"}
    config.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
    config.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "token"}
    config.AllowCredentials = true
    router.Use(cors.New(config))

    router.POST("/login", postLogin)
    router.POST("/register", postRegister)
    router.Run(":8888")
}
