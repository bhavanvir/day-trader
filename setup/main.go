package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"

	_ "github.com/lib/pq"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type Stock struct {
	ID        string `json:"stock_id"`
	StockName string `json:"stock_name"`
}

const (
	host     = "database"
	port     = 5432
	user     = "nt_user"
	password = "db123"
	dbname   = "nt_db"
)

var stocks = []Stock{}

func generateUUID() (string, error) {
	uuidBytes := make([]byte, 16)
	_, err := rand.Read(uuidBytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(uuidBytes), nil
}

func createStock(c *gin.Context) {
	var json struct {
		StockName string `json:"stock_name"`
	}

	if err := c.BindJSON(&json); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	stockID, err := generateUUID()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate UUID"})
		return
	}

	newStock := Stock{
		ID:        stockID,
		StockName: json.StockName,
	}

	stocks = append(stocks, newStock)

	// Save stocks to database
	if err := saveStockToDatabase(newStock); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save stock to database"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data": gin.H{
			"stock_id": stockID,
		},
	})
}

func saveStockToDatabase(stock Stock) error {
	// Define formatted string for database connection
	postgresqlDbInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// Attempt to connect to database
	db, err := sql.Open("postgres", postgresqlDbInfo)
	if err != nil {
		return err
	}
	defer db.Close()

	// Insert stock into the stocks table
	_, err = db.Exec("INSERT INTO stocks (stock_name) VALUES ($1)", stock.StockName)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	router := gin.Default()
	router.Use(cors.Default())

	router.POST("/createStock", createStock)

	router.Run(":8080")
}
