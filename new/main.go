package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type Product struct {
	Name string
}

// ChainProductsResponse - model of the catalog response from chain products endpoint.
type ChainProductsResponse struct {
	Products []Product `json:"products"`
	Err      error
}

type Client struct {
	SkusLimit int
}

// SearchProducts allows to search products by term.
func (c *Client) SearchProducts(ctx context.Context, term string, skus []string) (chpr ChainProductsResponse, err error) {

	if len(skus) == 0 {
		resp := c.searchProductsSerial(ctx, term, skus, 0)
		return resp, resp.Err
	}

	skuChunks := c.chunkSlice(skus, int(c.SkusLimit))
	fmt.Println(skuChunks, len(skuChunks))

	searchProductsResponseChannel := make(chan ChainProductsResponse, len(skuChunks))
	var wg sync.WaitGroup
	wg.Add(len(skuChunks))

	for workerCount := 0; workerCount < len(skuChunks); workerCount++ {
		go func(wc int) {
			searchProductsResponseChannel <- c.searchProductsSerial(ctx, term, skuChunks[wc], wc)
			wg.Done()
		}(workerCount)
	}

	wg.Wait()
	close(searchProductsResponseChannel)

	var allProducts []Product
	for response := range searchProductsResponseChannel {
		if response.Err != nil {
			fmt.Println("product error !!")
			return ChainProductsResponse{Products: allProducts}, err
		} else {
			fmt.Println("product !!")
			allProducts = append(allProducts, response.Products...)
		}
	}
	return ChainProductsResponse{Products: allProducts}, nil
}

func (c *Client) chunkSlice(skus []string, sizeOfChunk int) (chunks [][]string) {
	for len(skus) > 0 {
		pendingCount := len(skus)
		numberOfChunks := len(skus) / sizeOfChunk

		if numberOfChunks > 0 {
			pendingCount = sizeOfChunk
		}
		chunks = append(chunks, skus[:pendingCount])

		skus = skus[pendingCount:]
	}
	return chunks
}

// breakSkuList will create a channel and slices of SKUs will be inserted into the channel.
func (c *Client) breakSkuList(skus []string, sizeOfChunk int) (<-chan []string, int) {
	skusChan := make(chan []string)

	numberOfSkuSlices := (len(skus) / sizeOfChunk) + 1

	go func() {
		defer close(skusChan)
		for len(skus) > 0 {
			pendingCount := len(skus)
			numberOfChunks := len(skus) / sizeOfChunk

			if numberOfChunks > 0 {
				pendingCount = sizeOfChunk
			}
			skusChan <- skus[:pendingCount]

			skus = skus[pendingCount:]
		}
	}()

	return skusChan, numberOfSkuSlices
}

// searchProductsSerial will call catalog API to search products from the existing go routine.
func (c *Client) searchProductsSerial(ctx context.Context, term string, skus []string, workerCount int) ChainProductsResponse {
	searchQuery := ""
	if term != "" {
		searchQuery = fmt.Sprintf("field=name&operator=like&value=%v&", url.QueryEscape(term))
	}
	skus = removeSkuDuplicates(skus)
	searchQuery += fmt.Sprintf("limit=%v&skus=%v", len(skus)+1, strings.Join(skus, ","))

	if rand.Intn(100)%2 == 0 {
		fmt.Printf("worker:%d request sent - %s **error** \n", workerCount, searchQuery)
		return ChainProductsResponse{Err: fmt.Errorf("failed to do anything")}
	}

	fmt.Printf("worker:%d request sent - %s\n", workerCount, searchQuery)
	time.Sleep(10 * time.Millisecond)

	/*res, err := c.request(ctx, http.MethodGet, c.Endpoints.ChainProducts, searchQuery, nil)
	if err != nil {
		return ChainProductsResponse{}, errors.Wrapf(err, "Failed to GET products from catalog, (method, URL, query): %v, %v, %v", http.MethodGet, c.Endpoints.ChainProducts, searchQuery)
	}*/

	/*err = json.Unmarshal(res, &chpr)
	if err != nil {
		return ChainProductsResponse{}, errors.Wrapf(err, "Failed to unmarshal json response from catalog for GET products")
	}*/
	return ChainProductsResponse{
		Products: []Product{
			{Name: fmt.Sprintf("%d-1", workerCount)},
			{Name: fmt.Sprintf("%d-2", workerCount)},
		},
		Err: nil,
	}
}

func removeSkuDuplicates(skus []string) []string {
	if len(skus) == 0 {
		return skus
	}

	uniqSKUs := make(map[string]struct{})
	for _, sku := range skus {
		uniqSKUs[sku] = struct{}{}
	}

	uniqSkuList := make([]string, 0, len(uniqSKUs))
	for sku := range uniqSKUs {
		uniqSkuList = append(uniqSkuList, sku)
	}

	// Sorting SKU helps to use cache layer on Catalog service side
	sort.Strings(uniqSkuList)

	return uniqSkuList
}

func main() {
	skus := []string{
		"A1", "A2", "A3", "A4", "A5",
		"B3", "B4", "B5", "B6", "B7",
		"C3", "C4", "C5", "C6", "C7",
		"D3", "D4", "D5", "D6", "D7",
		"E3", "E4", "E5", "E6", "E7",
		"F3", "F4", "F5", "F6", "F7",
		"G3", "G4", "G5", "G6", "G7",
		"H3", "H4", "H5", "H6", "H7",
	}
	var cli Client
	cli.SkusLimit = 2
	resp, err := cli.SearchProducts(context.Background(), "term", skus)
	fmt.Println(resp, err)
	time.Sleep(5 * time.Second)
	fmt.Println("main: end")
}
