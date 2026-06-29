package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Result хранит итог проверки одного URL
type Result struct {
	URL   string
	OK    bool
	Error string // Причина ошибки, если OK == false
}

// checkService проверяет один URL с собственным таймаутом 2 сек
func checkService(ctx context.Context, url string) Result {
	// Создаём контекст с таймаутом для конкретного запроса
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Создаём запрос с контекстом
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return Result{URL: url, OK: false, Error: "invalid URL"}
	}

	// Выполняем запрос
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		// Проверяем, почему ошибка
		if reqCtx.Err() == context.DeadlineExceeded {
			return Result{URL: url, OK: false, Error: "timeout"}
		}
		return Result{URL: url, OK: false, Error: err.Error()}
	}
	defer resp.Body.Close()

	// Проверяем статус
	if resp.StatusCode != http.StatusOK {
		return Result{URL: url, OK: false, Error: fmt.Sprintf("status %d", resp.StatusCode)}
	}

	// Читаем тело (ограничиваем чтение, чтобы не съесть память)
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return Result{URL: url, OK: false, Error: "read body error"}
	}

	// Ищем подстроку "ok" (регистронезависимо)
	if !strings.Contains(strings.ToLower(string(body)), "ok") {
		return Result{URL: url, OK: false, Error: "body missing 'ok'"}
	}

	return Result{URL: url, OK: true, Error: ""}
}

func main() {
	// Список сервисов для проверки (в реальности берём из аргументов или файла)
	urls := []string{
		"https://httpbin.org/status/200", // отдаёт 200, но тело без "ok"
		"https://httpbin.org/delay/3",    // ответит через 3 сек -> timeout
		"https://httpbin.org/status/500", // вернёт 500
		"https://httpbin.org/get",        // хороший, в теле есть "url"
	}

	// Общий контекст с таймаутом 3 секунды на всю программу
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Канал для сбора результатов
	results := make(chan Result, len(urls))
	var wg sync.WaitGroup

	// Запускаем горутины
	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			// Передаём общий контекст внутрь
			results <- checkService(ctx, u)
		}(url)
	}

	// Закрываем канал, когда все горутины завершатся
	go func() {
		wg.Wait()
		close(results)
	}()

	// Выводим результаты по мере поступления
	for res := range results {
		if res.OK {
			fmt.Printf("[OK]     %s\n", res.URL)
		} else {
			fmt.Printf("[FAIL]   %s (%s)\n", res.URL, res.Error)
		}
	}
}