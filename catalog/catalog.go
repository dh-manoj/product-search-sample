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
	min := 10
	max := 100
	timeout := rand.Intn(max - min) + min
	time.Sleep(time.Duration(timeout)*time.Millisecond)

	
	ID1 := fmt.Sprintf("%v", timeout)
	ID2 := fmt.Sprintf("%v", timeout + 1)
	Products := []Product{
		{ID: ID1, Name: fmt.Sprintf("Product %v", ID1)},
		{ID: ID2, Name: fmt.Sprintf("Product %v", ID2)},
	}

	if timeout % 11 == 0 {
		fmt.Printf("forced failed response ID1:%v ID2:%v\n", ID1, ID2)
		//return ChainProductsResponse{Err: fmt.Errorf("failed to do anything")}
		http.Error(w, "failed!!", http.StatusInternalServerError)
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
