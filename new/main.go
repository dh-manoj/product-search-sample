package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

type Product struct {
	ID string
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

func sendRequest(ctx context.Context, method string) ([]byte, error) {
	endpoint := "http://127.0.0.1:51000/product"
	values := map[string]string{"foo": "baz"}
	jsonData, err := json.Marshal(values)

	client := &http.Client{}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("Error Occurred. %+v", err)
	}

	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error sending request to API endpoint. %+v", err)
	}

	// Close the connection to reuse it
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode > 299 {
		fmt.Printf("Got unexpected status code from catalog : %v\n", response.Status)
		respBody, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("not expected HTTP status '%s' from catalog: %w", response.Status, err)
		}

		fmt.Printf("Body context of the failed request : %v\n", string(respBody))
		return nil, fmt.Errorf("not expected HTTP status '%s' from catalog, body: %s", response.Status, string(respBody))
	}


	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse response body. %+v", err)
	}

	return body, nil
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

	//This helps in Cancelling all request in case of any one response failure
	var cancelFuncs []context.CancelFunc

	for workerCount := 0; workerCount < len(skuChunks); workerCount++ {

		cancelCtx, cancel := context.WithCancel(ctx)
		cancelFuncs = append(cancelFuncs, cancel)

		go func(wc int, ctx context.Context) {
			searchProductsResponseChannel <- c.searchProductsSerial(ctx, term, skuChunks[wc], wc)
			wg.Done()
		}(workerCount, cancelCtx)
	}

	go func() {
		wg.Wait()
		close(searchProductsResponseChannel)
	}()

	var onlyOnce sync.Once
	var errorFlag bool

	//Merge all response. Cancel all request in case of err
	var allProducts []Product
	for response := range searchProductsResponseChannel {
		if response.Err != nil {
			onlyOnce.Do(func() {
				errorFlag = true
				for _, cancel := range cancelFuncs {
					cancel()
				}
				fmt.Println("all request cancelled!!")
			})	
		} else {
			allProducts = append(allProducts, response.Products...)
		}
	}
	if errorFlag {
		fmt.Println("respond with failure!!")
		return ChainProductsResponse{}, err
	} else {
		fmt.Println("respond with success!!")
		return ChainProductsResponse{Products: allProducts}, nil
	}
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

	fmt.Printf("worker:%d request sent - %s\n", workerCount, searchQuery)
	
	res, err := sendRequest(ctx,"GET")
	if err != nil {
		fmt.Printf("failed to sendRequest:%v\n", err)
		return ChainProductsResponse{Products: []Product{},Err:err}
	}
		
	/*res, err := c.request(ctx, http.MethodGet, c.Endpoints.ChainProducts, searchQuery, nil)
	if err != nil {
		return ChainProductsResponse{}, errors.Wrapf(err, "Failed to GET products from catalog, (method, URL, query): %v, %v, %v", http.MethodGet, c.Endpoints.ChainProducts, searchQuery)
	}*/

	fmt.Printf("worker:%d request response - %v\n", workerCount, string(res))
	var products []Product
	err = json.Unmarshal(res, &products)

	if err != nil {
		fmt.Printf("failed to Unmarshal:%v\n", err)
		return ChainProductsResponse{Products: []Product{},Err:err}
	}
	return ChainProductsResponse{
		Products: products,
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
