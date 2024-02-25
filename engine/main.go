package main

// TODO: seperate into module: queue, buy, sell, matching
// Clarification: getWalletTransactions and getStockTransactions - is_debit, wallet_tx_id, (duplicate) stock_tx_id

// TODO: seperate into module: queue, buy, sell, matching

import (
	"container/heap"
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-contrib/cors"

	"github.com/Poomon001/day-trading-package/identification"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const (
	host = "database"
	// host     = "localhost" // for local testing
	port     = 5432
	user     = "nt_user"
	password = "db123"
	dbname   = "nt_db"

	namespaceUUID = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
)

// Define the structure of the request body for placing a stock order
type PlaceStockOrderRequest struct {
	StockID   int      `json:"stock_id" binding:"required"`
	IsBuy     *bool    `json:"is_buy" binding:"required"`
	OrderType string   `json:"order_type" binding:"required"`
	Quantity  int      `json:"quantity" binding:"required"`
	Price     *float64 `json:"price"`
}

// Define the structure of the response body for placing a stock order
type PlaceStockOrderResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data"`
}

// Define the structure of the request body for cancelling a stock transaction
type CancelStockTransactionRequest struct {
	StockTxID string `json:"stock_tx_id" binding:"required"`
}

// Define the structure of the response body for cancelling a stock transaction
type CancelStockTransactionResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data"`
}

type Order struct {
	StockTxID  string  `json:"stock_tx_id"`
	StockID    int     `json:"stock_id"`
	WalletTxID string  `json:"wallet_tx_id"`
	ParentTxID *string `json:"parent_tx_id"`
	IsBuy      bool    `json:"is_buy"`
	OrderType  string  `json:"order_type"`
	Quantity   int     `json:"quantity"`
	Price      float64 `json:"price"`
	TimeStamp  string  `json:"time_stamp"`
	Status     string  `json:"status"`
}

// Define the order book
type OrderBook struct {
	BuyOrders  PriorityQueue
	SellOrders PriorityQueue
	mu         sync.Mutex
}

// PriorityQueue
type PriorityQueue struct {
	Order    []*Order
	LessFunc func(i, j float64) bool
}

// handleError is a helper function to send error responses
func handleError(c *gin.Context, statusCode int, message string, err error) {
	errorResponse := map[string]interface{}{
		"success": false,
		"data":    nil,
		"message": message,
	}
	if err != nil {
		errorResponse["message"] = fmt.Sprintf("%s: %v", message, err)
	}
	c.JSON(statusCode, errorResponse)
}

func openConnection() (*sql.DB, error) {
	postgresqlDbInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	return sql.Open("postgres", postgresqlDbInfo)
}

/** standard heap interface **/
func (pq PriorityQueue) Len() int      { return len(pq.Order) }
func (pq PriorityQueue) Swap(i, j int) { pq.Order[i], pq.Order[j] = pq.Order[j], pq.Order[i] }
func (pq PriorityQueue) Less(i, j int) bool {
	if pq.Order[i].Price == pq.Order[j].Price {
		return pq.Order[i].TimeStamp < pq.Order[j].TimeStamp
	}
	return pq.LessFunc(pq.Order[i].Price, pq.Order[j].Price)
}
func highPriorityLess(i, j float64) bool { return i > j }
func lowPriorityLess(i, j float64) bool  { return i < j }

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Order)
	pq.Order = append(pq.Order, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.Order
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	pq.Order = old[0 : n-1]
	return item
}

/** standard heap interface END **/

// print the queue
func printq(book *OrderBook) {
	// Print the order book after adding the order
	orderBookMap.mu.Lock()
	defer orderBookMap.mu.Unlock()
	fmt.Println("\n === Current Sell Queue === \n")
	book.SellOrders.Printn()
	fmt.Println("\n ====== \n")
	fmt.Println("\n === Current Buy Queue === \n")
	book.BuyOrders.Printn()
	fmt.Println("\n ====== \n")
}

func (pq *PriorityQueue) Printn() {
	temp := PriorityQueue{Order: make([]*Order, len(pq.Order)), LessFunc: pq.LessFunc}
	copy(temp.Order, pq.Order)
	for temp.Len() > 0 {
		item := heap.Pop(&temp).(*Order)
		fmt.Printf("Stock Tx ID: %s, StockID: %d, WalletTxID: %s, Price: %.2f, Quantity: %d, Status: %s, TimeStamp: %s\n", item.StockTxID, item.StockID, item.WalletTxID, item.Price, item.Quantity, item.Status, item.TimeStamp)
	}
}

// update modifies the priority and value in the queue
// func (pq *PriorityQueue) update(order *Order, quantity int, timestamp string, status string) {
// 	order.Quantity = quantity
// 	order.TimeStamp = timestamp
// 	order.Status = status
// 	heap.Fix(pq, order.StockTxID)
// }

// Used by clean up routine to remove timed out orders
// Pass in the order book, search both queues for stale/old orders and remove them

func remove(book *OrderBook) {
	//orderBookMap.mu.Lock()
	//defer orderBookMap.mu.Unlock()

	// Take timestamp of when remove was called
	currTime := time.Now().Format(time.RFC3339Nano)
	currTimestamp, _ := time.Parse(time.Layout, currTime)

	tempBuyQueue := book.BuyOrders.Order
	tempSellQueue := book.SellOrders.Order

	nBuy := len(tempBuyQueue)
	nSell := len(tempSellQueue)
	if (nBuy == 0) && (nSell == 0) {
		return
	}

	//Check for timed out orders in buyQueue
	for i := 0; i < nBuy; i++ {
		// Set timeout to 15min after order timestamp
		buyTimeStamp, _ := time.Parse(time.Layout, tempBuyQueue[i].TimeStamp)
		timeout := buyTimeStamp.Add(time.Minute * 15)

		if buyTimeStamp.Sub(currTimestamp) > timeout {
			// remove order from queue
			tempBuyQueue[i], tempBuyQueue[nBuy-1] = tempBuyQueue[nBuy-1], tempBuyQueue[i]
			tempBuyQueue = tempBuyQueue[:nBuy-1]
		}
	}

	//Check for timed out orders in sellQueue
	for i := 0; i < nSell; i++ {
		// Set timeout to 15min after order timestamp
		sellTimeStamp, _ := time.Parse(time.Layout, tempSellQueue[i].TimeStamp)
		timeout := sellTimeStamp.Add(time.Minute * 15)

		if sellTimeStamp.Sub(currTimestamp) > timeout {
			// remove order from queue
			tempSellQueue[i], tempSellQueue[nSell-1] = tempSellQueue[nSell-1], tempSellQueue[i]
			tempSellQueue = tempSellQueue[:nSell-1]
		}
	}

	return
}

// generateOrderID generates a unique ID for each order
func generateOrderID() string {
	return uuid.New().String()
}

// Generate a unique wallet ID for the user
func generateWalletID(userName string) string {
	// return uuid.NewSHA1(uuid.Must(uuid.NewRandom()), []byte(userName)).String()
	return uuid.New().String()
}

func validateOrderType(request *PlaceStockOrderRequest) error {
	if request.OrderType == "MARKET" && request.Price != nil {
		return fmt.Errorf("Price must be null for market orders")
	} else if request.OrderType == "LIMIT" && request.Price == nil {
		return fmt.Errorf("Price must not be null for limit orders")
	}
	return nil
} // validateOrderType

func createOrder(request *PlaceStockOrderRequest, userName string) (Order, error) {
	order := Order{
		StockTxID:  generateOrderID(),
		StockID:    request.StockID,
		WalletTxID: generateWalletID(userName),
		ParentTxID: nil,
		IsBuy:      request.IsBuy != nil && *request.IsBuy,
		OrderType:  request.OrderType,
		Quantity:   request.Quantity,
		Price:      *request.Price,
		TimeStamp:  time.Now().Format(time.RFC3339Nano),
		Status:     "IN_PROGRESS",
	}
	return order, nil
} // createOrder

func HandlePlaceStockOrder(c *gin.Context) {
	user_name, exists := c.Get("user_name")
	if !exists || user_name == nil {
		handleError(c, http.StatusUnauthorized, "User not authenticated", nil)
		return
	}

	userName, ok := user_name.(string)
	if !ok {
		handleError(c, http.StatusBadRequest, "Invalid user name type", nil)
		return
	}

	var request PlaceStockOrderRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		handleError(c, http.StatusBadRequest, err.Error(), err)
		return
	}

	if err := validateOrderType(&request); err != nil {
		handleError(c, http.StatusBadRequest, err.Error(), nil)
		return
	}

	order, e := createOrder(&request, userName)
	if e != nil {
		handleError(c, http.StatusInternalServerError, "Failed to create order", e)
		return
	}

	// Feb 24/24 - Removed unnecessary branching; now single path
	// ----------------------------------------------------------

	if err := updateWallet(userName, order); err != nil {
		handleError(c, http.StatusInternalServerError, "Failed to deduct money from user's wallet", err)
		return
	}

	if err := updateStockPortfolio(userName, order); err != nil {
		handleError(c, http.StatusInternalServerError, "Failed to deduct stock from user's portfolio", err)
		return
	}

	// TODO: Fix db bug FIXED Feb 22/24
	if err := setWalletTransaction(userName, order); err != nil {
		handleError(c, http.StatusInternalServerError, "Buy Order setWalletTx Error: "+err.Error(), err)
		return
	}

	// TODO: Fix db bug FIXED Feb 22/24
	if err := setStockTransaction(userName, order); err != nil {
		handleError(c, http.StatusInternalServerError, "Buy Order setStockTx Error: "+err.Error(), err)
		return
	}

	//fmt.Println("\n === Test === \n")

	book, bookerr := initializePriorityQueue(order)
	if bookerr != nil {
		handleError(c, http.StatusInternalServerError, "Failed to push order to priority queue", bookerr)
		return
	}

	processOrder(book, order)

	printq(book)

	// Feb 24/24 - Condensed this part into a single branch that handles both buy and sell orders
	// -------------------------------------------------------------------------------------------

	// if order.IsBuy {
	// 	if err := updateWallet(userName, order); err != nil {
	// 		handleError(c, http.StatusInternalServerError, "Failed to deduct money from user's wallet", err)
	// 		return
	// 	}

	// 	// TODO: Fix db bug FIXED Feb 22/24
	// 	if err := setWalletTransaction(userName, order); err != nil {
	// 		handleError(c, http.StatusInternalServerError,  "Buy Order setWalletTx Error: " + err.Error(), err)
	// 		return
	// 	}

	// 	// TODO: Fix db bug FIXED Feb 22/24
	// 	if err := setStockTransaction(userName, order); err != nil {
	// 		handleError(c, http.StatusInternalServerError, "Buy Order setStockTx Error: " + err.Error(), err)
	// 		return
	// 	}

	// 	fmt.Println("\n === Test === \n")

	// 	book, bookerr := initializePriorityQueue(order)
	// 	if bookerr != nil {
	// 		handleError(c, http.StatusInternalServerError, "Failed to push order to priority queue", bookerr)
	// 		return
	// 	}

	// 	processOrder(book, order)

	// 	printq(book)
	// } else {
	// 	if err := updateStockPortfolio(userName, order); err != nil {
	// 		handleError(c, http.StatusInternalServerError, "Failed to deduct stock from user's portfolio", err)
	// 		return
	// 	}

	// 	if err := setWalletTransaction(userName, order); err != nil {
	// 		handleError(c, http.StatusInternalServerError,  "Sell Order setWalletTx Error: " + err.Error(), err)
	// 		return
	// 	}

	// 	// TODO: Fix db bug - FIXED Feb 22/24
	// 	if err := setStockTransaction(userName, order); err != nil {
	// 		handleError(c, http.StatusInternalServerError, "Sell Order setStockTx Error: " + err.Error(), err)
	// 		return
	// 	}

	// 	book, bookerr := initializePriorityQueue(order)
	// 	if bookerr != nil {
	// 		handleError(c, http.StatusInternalServerError, "Failed to push order to priority queue", bookerr)
	// 		return
	// 	}

	// 	processOrder(book, order)

	// 	printq(book)
	// }

	// Feb 24/24 - Replaced with func updateStockPortfolio()
	// ------------------------------------------------------
	// Update the user's stock quantity in the database
	// err := updateUserStockQuantity(userName, order.StockID, order.Quantity, order.IsBuy)
	// if err != nil {
	// 	handleError(c, http.StatusInternalServerError, "Failed to update user stock quantity", err)
	// 	return
	// }

	response := PlaceStockOrderResponse{
		Success: true,
		Data:    nil,
	}

	c.IndentedJSON(http.StatusOK, response)
} // HandlePlaceStockOrder

// FEB 24/24 - DEPRICATED: REPLACED WITH updateStockPortfolio()
// ------------------------------------------------------------
// updateUserStockQuantity updates the user's stock quantity in the database
func updateUserStockQuantity(userName string, stockID int, quantity int, isBuy bool) error {
	var query string

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname))
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if isBuy {
		query = "UPDATE user_stocks SET quantity = quantity + $1 WHERE user_name = $2 AND stock_id = $3"
	} else {
		query = "UPDATE user_stocks SET quantity = quantity - $1 WHERE user_name = $2 AND stock_id = $3"
	}

	_, err = tx.Exec(query, quantity, userName, stockID)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func TraverseOrderBook(StockTxID string, book *OrderBook, bookType string) (response CancelStockTransactionResponse) {
	response = CancelStockTransactionResponse{
		Success: false,
		Data:    nil,
	}

	var bookOrders *PriorityQueue
	if bookType == "buy" {
		bookOrders = &book.BuyOrders
	} else {
		bookOrders = &book.SellOrders
	}

	// Find the index of the order to remove
	indexToRemove := -1
	for i, order := range bookOrders.Order {
		if order.StockTxID == StockTxID && order.Status == "IN_PROGRESS" && order.OrderType == "LIMIT" {
			indexToRemove = i
			break
		}
	}

	// If the order was found, remove it from the heap
	if indexToRemove != -1 {
		heap.Remove(bookOrders, indexToRemove)
		response.Success = true
	}

	return response
}

func HandleCancelStockTransaction(c *gin.Context) {
	userName, exists := c.Get("user_name")
	if !exists || userName == nil {
		handleError(c, http.StatusUnauthorized, "User not authenticated", nil)
		return
	}

	var request CancelStockTransactionRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		handleError(c, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	StockTxID := request.StockTxID

	orderBookMap.mu.Lock()
	defer orderBookMap.mu.Unlock()
	// Find which order book the order is in
	for _, book := range orderBookMap.OrderBooks {
		book.mu.Lock()
		defer book.mu.Unlock()

		foundBuy := TraverseOrderBook(StockTxID, book, "buy")
		foundSell := TraverseOrderBook(StockTxID, book, "sell")

		// Inside TraverseOrderBook, after removing the item
		fmt.Println("\n --- Current Sell Queue --- \n")
		book.SellOrders.Printn()
		fmt.Println("\n ------ \n")
		fmt.Println("\n --- Current Buy Queue --- \n")
		book.BuyOrders.Printn()
		fmt.Println("\n ------ \n")

		if foundBuy.Success || foundSell.Success {
			response := CancelStockTransactionResponse{
				Success: true,
				Data:    nil,
			}
			c.IndentedJSON(http.StatusOK, response)
			return
		}
	}

	handleError(c, http.StatusBadRequest, "Order not found", nil)
}

// Define the structure of the order book map
type OrderBookMap struct {
	OrderBooks map[int]*OrderBook // Map of stock ID to order book
	mu         sync.Mutex         // Mutex to synchronize access to the map
}

// Initialize the order book map
var orderBookMap = OrderBookMap{
	OrderBooks: make(map[int]*OrderBook),
}

/** === BUY Order === **/
func matchLimitBuyOrder(book *OrderBook, order Order) {
	// Add the buy order to the buy queue
	heap.Push(&book.BuyOrders, &order)
	highestBuyOrder := heap.Pop(&book.BuyOrders).(*Order)

	// If the buy order is a limit order, match it with the lowest sell order that is less than or equal to the buy order price
	for highestBuyOrder.Quantity > 0 && book.SellOrders.Len() > 0 {
		lowestSellOrder := heap.Pop(&book.SellOrders).(*Order)

		// If the lowest sell order price is less than or equal to the buy order price, execute the trade
		if lowestSellOrder.Price <= highestBuyOrder.Price {
			executeBuyLimitTrade(book, highestBuyOrder, lowestSellOrder)
			fmt.Println("Trade executed")
			fmt.Println("Buy Order: ", highestBuyOrder)
			fmt.Println("Sell Order: ", lowestSellOrder)
		} else {
			// If the lowest sell order price is greater than the buy order price, put it back in the sell queue
			fmt.Println("No match found, putting back in the buy queue")
			heap.Push(&book.SellOrders, lowestSellOrder)
			break
		}
	}

	// If the buy order was not fully executed, put it back in the buy queue
	if highestBuyOrder.Quantity > 0 {
		heap.Push(&book.BuyOrders, highestBuyOrder)
	}
}

func executeBuyLimitTrade(book *OrderBook, order *Order, sellOrder *Order) {
	tradeQuantity := min(order.Quantity, sellOrder.Quantity)
	if order.Quantity > sellOrder.Quantity {
		// execute partial trade for buy order and complete trade for sell order
		order.Quantity -= tradeQuantity
		sellOrder.Quantity = 0
	} else if order.Quantity < sellOrder.Quantity {
		// execute partial trade for sell order and complete trade for buy order
		sellOrder.Quantity -= tradeQuantity
		order.Quantity = 0
		heap.Push(&book.SellOrders, sellOrder)
	} else {
		// execute complete trade for both buy and sell orders
		order.Quantity = 0
		sellOrder.Quantity = 0
	}
}

/** === END BUY Order === **/

/** === SELL Order === **/
func matchLimitSellOrder(book *OrderBook, order Order) {
	// Add the sell order to the sell queue
	heap.Push(&book.SellOrders, &order)
	lowestSellOrder := heap.Pop(&book.SellOrders).(*Order)

	for lowestSellOrder.Quantity > 0 && book.BuyOrders.Len() > 0 {
		fmt.Println("Try matching limit sell order:")
		highestBuyOrder := heap.Pop(&book.BuyOrders).(*Order)

		if highestBuyOrder.Price >= lowestSellOrder.Price {
			executeSellLimitTrade(book, highestBuyOrder, lowestSellOrder)
			fmt.Println("Trade executed")
			fmt.Println("Buy Order: ", highestBuyOrder)
			fmt.Println("Sell Order: ", lowestSellOrder)
		} else {
			fmt.Println("No match found, putting back in the buy queue")
			heap.Push(&book.BuyOrders, highestBuyOrder)
			break
		}
	}

	if lowestSellOrder.Quantity > 0 {
		heap.Push(&book.SellOrders, lowestSellOrder)
	}
}

func executeSellLimitTrade(book *OrderBook, buyOrder *Order, order *Order) {
	tradeQuantity := min(buyOrder.Quantity, order.Quantity)
	if buyOrder.Quantity > order.Quantity {
		// execute partial trade for buy order and complete trade for sell order
		buyOrder.Quantity -= tradeQuantity
		order.Quantity = 0
		heap.Push(&book.BuyOrders, buyOrder)
	} else if buyOrder.Quantity < order.Quantity {
		// execute partial trade for sell order and complete trade for buy order
		order.Quantity -= tradeQuantity
		buyOrder.Quantity = 0
	} else {
		// execute complete trade for both buy and sell orders
		buyOrder.Quantity = 0
		order.Quantity = 0
	}
}

/** === END SELL Order === **/

/** === BUY/SELL Order === **/
func updateWallet(userName string, order Order) error {
	fmt.Println("Deducting money from wallet")

	// Connect to database
	db, err := openConnection()
	if err != nil {
		return fmt.Errorf("Failed to connect to database: %w", err)
	}
	defer db.Close()

	// Calculate total to be added or deducted
	total := order.Price * float64(order.Quantity)
	if order.IsBuy {
		total = total * (-1) // Reduce funds if buying
	}

	// Update the user's wallet
	_, err = db.Exec(`
		UPDATE users SET wallet = wallet + $1 WHERE user_name = $2`, total, userName)
	if err != nil {
		return fmt.Errorf("Failed to update wallet: %w", err)
	}
	return nil
}

func updateStockPortfolio(userName string, order Order) error {
	fmt.Println("Deducting stock from portfolio")

	// Connect to database
	db, err := openConnection()
	if err != nil {
		return fmt.Errorf("Failed to connect to database: %w", err)
	}
	defer db.Close()

	// Calculate total to be added or deducted
	total := order.Quantity
	if !order.IsBuy {
		total = total * (-1) // Reduce stocks if selling
	}

	rows, err := db.Query(`
		SELECT stock_id FROM user_stocks WHERE user_name = $1 AND stock_id = $2`, userName, order.StockID)
	if err != nil {
		return fmt.Errorf("Failed to query user stocks: %w", err)
	}
	defer rows.Close()

	// User already owns this stock
	if rows.Next() {
		// Update the user's stocks
		_, err = db.Exec(`
			UPDATE user_stocks SET quantity = quantity + $1 WHERE user_name = $2 AND stock_id = $3`, total, userName, order.StockID)
		if err != nil {
			return fmt.Errorf("Failed to update user stocks: %w", err)
		}
	} else { // Create new user_stock
		_, err = db.Exec(`
			INSERT INTO user_stocks VALUES ($1, $2, $3)`, userName, order.StockID, total)
		if err != nil {
			return fmt.Errorf("Failed to update user stocks: %w", err)
		}
	}
	return nil
}

// Store completed wallet transactions in the database
func setWalletTransaction(userName string, tx Order) error {
	// Connect to database
	db, err := openConnection()
	if err != nil {
		return fmt.Errorf("Failed to insert stock transaction: %w", err)
	}
	defer db.Close()

	// Insert transaction to wallet transactions
	_, err = db.Exec(`
		INSERT INTO wallet_transactions (wallet_tx_id, user_name, is_debit, amount, time_stamp)
		VALUES ($1, $2, $3, $4, $5)`, tx.WalletTxID, userName, true, tx.Price, tx.TimeStamp)
	if err != nil {
		return fmt.Errorf("Failed to commit transaction: %w", err)
	}
	return nil
}

// Store completed stock transactions in the database
func setStockTransaction(userName string, tx Order) error {
	fmt.Println("Setting stock transaction")
	// Connect to database
	db, err := openConnection()
	if err != nil {
		return fmt.Errorf("Failed to insert stock transaction: %w", err)
	}
	defer db.Close()

	// Insert transaction to stock transactions
	_, err = db.Exec(`
		INSERT INTO stock_transactions (stock_tx_id, user_name, stock_id, wallet_tx_id, order_status,  parent_tx_id, is_buy, order_type, stock_price, quantity,  time_stamp)
	    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`, tx.StockTxID, userName, tx.StockID, tx.WalletTxID, tx.Status, tx.ParentTxID, tx.IsBuy, tx.OrderType, tx.Price, tx.Quantity, tx.TimeStamp)
	if err != nil {
		return fmt.Errorf("Failed to commit transaction: %w", err)
	}
	return nil
}

func initializePriorityQueue(order Order) (*OrderBook, error) {
	// Add the order to the order book corresponding to the stock ID
	orderBookMap.mu.Lock()
	defer orderBookMap.mu.Unlock()
	book, ok := orderBookMap.OrderBooks[order.StockID]
	if !ok {
		// If the order book for this stock does not exist, create a new one
		book = &OrderBook{
			BuyOrders:  PriorityQueue{Order: make([]*Order, 0), LessFunc: highPriorityLess},
			SellOrders: PriorityQueue{Order: make([]*Order, 0), LessFunc: lowPriorityLess},
		}
		orderBookMap.OrderBooks[order.StockID] = book
	}
	return book, nil
}

// ProcessOrder processes a buy or sell order based on the order type
func processOrder(book *OrderBook, order Order) {
	book.mu.Lock()
	defer book.mu.Unlock()

	if order.IsBuy {
		if order.OrderType == "MARKET" {
			// matchMarketBuyOrder(book, order)
		} else {
			matchLimitBuyOrder(book, order)
		}
	} else {
		if order.OrderType == "MARKET" {
			// matchMarketSellOrder(book, order)
		} else {
			matchLimitSellOrder(book, order)
		}
	}

}

/** === END BUY/SELL Order === **/

func main() {
	router := gin.Default()

	config := cors.DefaultConfig()
	config.AllowOrigins = []string{"http://localhost:3000"}
	config.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
	config.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"}
	config.AllowCredentials = true
	router.Use(cors.New(config))

	identification.Test()
	router.POST("/placeStockOrder", identification.Identification, HandlePlaceStockOrder)
	router.POST("/cancelStockTransaction", identification.Identification, HandleCancelStockTransaction)
	router.Run(":8585")

	// test for timeout checks
	//time.Sleep(time.Minute * 1)
	//remove(OrderBook)

}
