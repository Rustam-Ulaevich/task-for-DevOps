package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Result struct {
	URL     string `json:"url"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Latency int64  `json:"latency_ms"`
}

func checkService(ctx context.Context, url string) Result {
	start := time.Now()
	
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return Result{URL: url, OK: false, Error: "invalid URL", Latency: time.Since(start).Milliseconds()}
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	latency := time.Since(start).Milliseconds()
	
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded {
			return Result{URL: url, OK: false, Error: "timeout", Latency: latency}
		}
		return Result{URL: url, OK: false, Error: err.Error(), Latency: latency}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Result{URL: url, OK: false, Error: fmt.Sprintf("status %d", resp.StatusCode), Latency: latency}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return Result{URL: url, OK: false, Error: "read body error", Latency: latency}
	}

	if !strings.Contains(strings.ToLower(string(body)), "ok") {
		return Result{URL: url, OK: false, Error: "body missing 'ok'", Latency: latency}
	}

	return Result{URL: url, OK: true, Error: "", Latency: latency}
}

func main() {
	// Парсим аргументы командной строки
	var (
		urlsFile string
		timeout  int
		jsonOut  bool
	)
	
	flag.StringVar(&urlsFile, "file", "urls.txt", "file with URLs (one per line)")
	flag.IntVar(&timeout, "timeout", 3, "global timeout in seconds")
	flag.BoolVar(&jsonOut, "json", false, "output in JSON format")
	flag.Parse()

	// Читаем URL из файла или аргументов
	var urls []string
	
	// Если есть позиционные аргументы (не флаги) - используем их
	if flag.NArg() > 0 {
		urls = flag.Args()
	} else {
		// Иначе читаем файл
		data, err := os.ReadFile(urlsFile)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			os.Exit(1)
		}
		urls = strings.Split(string(data), "\n")
		// Удаляем пустые строки
		var clean []string
		for _, u := range urls {
			if u = strings.TrimSpace(u); u != "" {
				clean = append(clean, u)
			}
		}
		urls = clean
	}

	if len(urls) == 0 {
		fmt.Println("No URLs to check")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	results := make(chan Result, len(urls))
	var wg sync.WaitGroup

	// Запускаем проверку
	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			results <- checkService(ctx, u)
		}(url)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Собираем результаты
	var allResults []Result
	for res := range results {
		allResults = append(allResults, res)
	}

	// Выводим в нужном формате
	if jsonOut {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(allResults); err != nil {
			fmt.Printf("Error encoding JSON: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Человекочитаемый вывод
		for _, res := range allResults {
			if res.OK {
				fmt.Printf("[OK]     %s (latency: %dms)\n", res.URL, res.Latency)
			} else {
				fmt.Printf("[FAIL]   %s (%s) [%dms]\n", res.URL, res.Error, res.Latency)
			}
		}
	}
}