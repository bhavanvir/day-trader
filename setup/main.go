package main

import (
	"database/sql"
	"fmt"
	"net/http"

	"github.com/Poomon001/day-trading-package/identification"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
)

type Stock struct {
	StockName string `json:"stock_name"`
}

const (
    host     = "database"
    // host     = "localhost" // for local testing
    port     = 5432
    user     = "nt_user"
    password = "db123"
    dbname   = "nt_db"
)

type AddStockRequest struct {
	StockID  string `json:"stock_id"`
	Quantity int    `json:"quantity"`
}

type ErrorResponse struct {
	Success bool   `json:"success"`
	Data    string `json:"data"`
	Message string `json:"message"`
}

type PostResponse struct {
	Success bool    `json:"success"`
	Data    *string `json:"data"`
}

func handleError(c *gin.Context, statusCode int, message string, err error) {
	errorResponse := ErrorResponse{
		Success: false,
		Data:    "",
		Message: fmt.Sprintf("%s: %v", message, err),
	}
	c.IndentedJSON(statusCode, errorResponse)
}

func createStock(c *gin.Context) {
	user_name, exists := c.Get("user_name")
	if !exists || user_name == nil {
		handleError(c, http.StatusUnauthorized, "User not authenticated", nil)
		return
	}

	var json Stock

	if err := c.BindJSON(&json); err != nil {
		handleError(c, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Save stock to database
	stockID, err := saveStockToDatabase(json)
	if err != nil {
		handleError(c, http.StatusInternalServerError, "Failed to save stock to database", err)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"stock_id": stockID,
		},
	})
}

func saveStockToDatabase(stock Stock) (int, error) {
	// Define formatted string for database connection
	postgresqlDbInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// Attempt to connect to database
	db, err := sql.Open("postgres", postgresqlDbInfo)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var stockID int

	// Insert stock into the stocks table and retrieve the generated stock_id
	err = db.QueryRow("INSERT INTO stocks (stock_name) VALUES ($1) RETURNING stock_id", stock.StockName).Scan(&stockID)
	if err != nil {
		return 0, err
	}

	return stockID, nil
}

func addStockToUser(c *gin.Context) {
	// Get user name from identification middleware
	userName, _ := c.Get("user_name")
	if userName == nil {
		handleError(c, http.StatusBadRequest, "Failed to obtain the user name", nil)
		return
	}

	var req AddStockRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		handleError(c, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	// Connect to the PostgreSQL database
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname))
	if err != nil {
		handleError(c, http.StatusInternalServerError, "Failed to connect to the database", err)
		return
	}
	defer db.Close()

	fmt.Println("userName:", userName)
	fmt.Println("ID:", req.StockID)
	fmt.Println("quantity:", req.Quantity)
	// Insert stock into user_stocks table
	_, err = db.Exec(`
		INSERT INTO user_stocks (user_name, stock_id, quantity)
		VALUES ($1, $2, $3)
		ON CONFLICT (user_name, stock_id)
		DO UPDATE SET quantity = user_stocks.quantity + EXCLUDED.quantity;
	`, userName, req.StockID, req.Quantity)
	if err != nil {
		handleError(c, http.StatusInternalServerError, "Failed to add stock to user", err)
		return
	}

	// If everything succeeded, return success response
	response := PostResponse{
		Success: true,
		Data:    nil,
	}
	c.IndentedJSON(http.StatusOK, response)
}

func wipeDatabaseTables(c *gin.Context) {
	/* 
	This function is needed when running the postman collection tests, as not doing so 
	will cause certain tests to fail 
	*/

    db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname))
    if err != nil {
        handleError(c, http.StatusInternalServerError, "Failed to connect to the database", err)
        return
    }
    defer db.Close()

    // Define a list of tables to truncate
    tables := []string{"stock_transactions", "stocks", "user_stocks", "users", "wallet_transactions"}

    // Truncate each table. This will delete all rows in the table
    for _, table := range tables {
        _, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table))
        if err != nil {
            handleError(c, http.StatusInternalServerError, "Failed to truncate table", err)
            return
        }
    }

	// Reset the stock_id sequence to start at  1 after truncating the stocks table. This is necessary because
	// when we test the endpoint in the postman collection, the stock_id will be auto incremented by 1, causing
	// the test to fail
    _, err = db.Exec("ALTER SEQUENCE stocks_stock_id_seq RESTART WITH  1")
    if err != nil {
        handleError(c, http.StatusInternalServerError, "Failed to reset stock_id sequence", err)
        return
    }

    response := PostResponse{
        Success: true,
        Data:    nil,
    }
    c.IndentedJSON(http.StatusOK, response)
}

func main() {
	router := gin.Default()
	router.Use(cors.Default())
	identification.Test()
	router.POST("/createStock", identification.Identification, createStock)
	router.POST("/addStockToUser", identification.Identification, addStockToUser)

	// For testring purposes: all database tables are wiped before running postman-collection tests
	router.DELETE("/wipeDatabaseTables", wipeDatabaseTables)

	router.Run(":8080")
}
