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
}

type Client struct {
	SkusLimit int
}

// SearchProducts allows to search products by term.
func (c *Client) SearchProducts(ctx context.Context, term string, skus []string) (chpr ChainProductsResponse, err error) {

	if len(skus) == 0 {
		return c.searchProductsSerial(ctx, term, skus, 0)
	}

	errorChannel := make(chan error)
	defer close(errorChannel)

	// 1. Create a channel of slice of skus, the Source.
	skusChannel, numberOfWorkers := c.breakSkuList(skus, int(c.SkusLimit))

	// 2. Worker routines to fetch products from catalog.
	searchProductsResponseChannels := make([]<-chan ChainProductsResponse, numberOfWorkers)
	for workerCount := 0; workerCount < numberOfWorkers; workerCount++ {
		searchProductsResponseChannels[workerCount] = c.searchProductsParallel(ctx, term, skusChannel, errorChannel, workerCount)
	}

	// 3. Merge the response from all the worker routines, the Sink
	chainProductsResponseChannel := c.mergeSearchPipelines(searchProductsResponseChannels...)

	var allProducts []Product

	for {
		select {
		case err := <-errorChannel:
			fmt.Println("main: error received")
			return ChainProductsResponse{}, err
		case <-ctx.Done():
			return ChainProductsResponse{}, err
		case singleChainResponse, ok := <-chainProductsResponseChannel:
			if !ok {
				return ChainProductsResponse{
					Products: allProducts,
				}, nil
			}
			allProducts = append(allProducts, singleChainResponse.Products...)
		}
	}
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

// searchProductsParallel will call searchProductsSearial in multiple go routines.
func (c *Client) searchProductsParallel(ctx context.Context, term string, skusChannel <-chan []string, errorChannel chan<- error, workerCount int) <-chan ChainProductsResponse {
	chainProductsResponseChannel := make(chan ChainProductsResponse)

	go func() {
		defer close(chainProductsResponseChannel)
		for skus := range skusChannel {
			chpr, err := c.searchProductsSerial(ctx, term, skus, workerCount)
			if err != nil {
				fmt.Printf("worker:%d writing to error channel\n", workerCount)
				errorChannel <- err
				return
			}

			chainProductsResponseChannel <- chpr
		}
	}()

	return chainProductsResponseChannel
}

// searchProductsSerial will call catalog API to search products from the existing go routine.
func (c *Client) searchProductsSerial(ctx context.Context, term string, skus []string, workerCount int) (ChainProductsResponse, error) {
	searchQuery := ""
	if term != "" {
		searchQuery = fmt.Sprintf("field=name&operator=like&value=%v&", url.QueryEscape(term))
	}
	skus = removeSkuDuplicates(skus)
	searchQuery += fmt.Sprintf("limit=%v&skus=%v", len(skus)+1, strings.Join(skus, ","))

	if rand.Intn(100)%2 == 0 {
		fmt.Printf("worker:%d request sent - %s **error** \n", workerCount, searchQuery)
		return ChainProductsResponse{}, fmt.Errorf("failed to do anything")
	}

	fmt.Printf("worker:%d request sent - %s\n", workerCount, searchQuery)
	time.Sleep(10 * time.Millisecond)

	/*res, err := c.request(ctx, http.MethodGet, c.Endpoints.ChainProducts, searchQuery, nil)
	if err != nil {
		return ChainProductsResponse{}, errors.Wrapf(err, "Failed to GET products from catalog, (method, URL, query): %v, %v, %v", http.MethodGet, c.Endpoints.ChainProducts, searchQuery)
	}*/

	var chpr ChainProductsResponse
	/*err = json.Unmarshal(res, &chpr)
	if err != nil {
		return ChainProductsResponse{}, errors.Wrapf(err, "Failed to unmarshal json response from catalog for GET products")
	}*/
	return chpr, nil
}

// mergeSearchPipelines will merge all the channels which contain response of SearchProducts from Catalog.
func (c *Client) mergeSearchPipelines(chainProductsChannels ...<-chan ChainProductsResponse) <-chan ChainProductsResponse {
	var wg sync.WaitGroup
	resultChannel := make(chan ChainProductsResponse)

	// copies values from a child channel to one single parent channel.
	merger := func(chainProductsChannel <-chan ChainProductsResponse) {
		defer wg.Done()
		for chainResponse := range chainProductsChannel {
			resultChannel <- chainResponse
		}
	}

	wg.Add(len(chainProductsChannels))
	for _, c := range chainProductsChannels {
		go merger(c)
	}

	// Start a goroutine to close out once all the output goroutines are done.
	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	return resultChannel
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
	cli.SearchProducts(context.Background(), "term", skus)
	time.Sleep(5 * time.Second)
	fmt.Println("main: end")
}
