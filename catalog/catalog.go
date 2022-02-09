package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
	"math/rand"
)

type Product struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to the HomePage!")
	fmt.Println("Endpoint Hit: homePage")
}

func returnProduct(w http.ResponseWriter, r *http.Request) {
	Products := []Product{
		{ID: "1", Name: "Product 1"},
		{ID: "2", Name: "Product 2"},
	}
	min := 1000
	max := 10000
	timeout := rand.Intn(max - min) + min
	time.Sleep(time.Duration(timeout)*time.Millisecond)

	if rand.Intn(100)%2 == 0 {
		fmt.Printf("forced failed response\n")
		//return ChainProductsResponse{Err: fmt.Errorf("failed to do anything")}
		http.Error(w, "failed yo yo", http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(Products)
}

func handleRequests() {
	http.HandleFunc("/", homePage)
	http.HandleFunc("/product", returnProduct)
	log.Fatal(http.ListenAndServe(":51000", nil))
}

func main() {
	fmt.Println("Rest API v2.0 - Mux Routers")

	handleRequests()
}
