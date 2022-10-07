package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dev-lazarev/memcache"
)

func main() {
	mc, err := memcache.New([]memcache.Config{
		{
			Server:            "localhost:11211",
			User:              "my_user1",
			Password:          "my_password",
			InitialCap:        100,
			MaxIdle:           140,
			MaxCap:            150,
			IdleTimeout:       15 * time.Second,
			ConnectionTimeout: 30 * time.Millisecond,
		},
	})
	if err != nil {
		panic(err)
	}
	defer mc.Close()

	err = mc.Set(&memcache.Item{
		Key:        "sc/seller/id/global/1102131071",
		Value:      []byte(strconv.FormatInt(3743321700, 10)),
		Flags:      123,
		Expiration: 60 * 60 * 24 * 28,
	})
	if err != nil {
		panic(err)
	}

	it, err := mc.GetMulti([]string{"sc/seller/id/global/1102131071", "sc/seller/id/global/1102131072"})
	if err != nil {
		fmt.Println(err)
		return
	}
	for key, item := range it {
		fmt.Println(key, string(item.Value))
	}

	count := 10
	mu := sync.Mutex{}
	max := time.Microsecond
	for range make([]int64, 100) {
		{
			wg := sync.WaitGroup{}
			wg.Add(count)
			for i, _ := range make([]int64, count) {
				i := i
				go func(begin time.Time) {
					defer func() {
						wg.Done()
						since := time.Since(begin)
						mu.Lock()
						if max < since {
							max = since
						}
						mu.Unlock()
					}()
					it, err := mc.GetMulti([]string{"sc/seller/id/global/1102131071", "sc/seller/id/global/1102131072"})
					if err != nil {
						fmt.Println(err)
						return
					}
					for key, item := range it {
						fmt.Println(i, key, string(item.Value))
					}

				}(time.Now())

			}
			wg.Wait()
		}
	}
	fmt.Println(max.String())

	for range make([]int64, 100) {
		{
			wg := sync.WaitGroup{}
			wg.Add(count)
			for range make([]int64, count) {
				go func(begin time.Time) {
					defer func() {
						wg.Done()
						since := time.Since(begin)
						mu.Lock()
						if max < since {
							max = since
						}
						mu.Unlock()
					}()
					it, err := mc.Get("sc/seller/id/global/1102131071")
					if err != nil {
						fmt.Println(err)
						return
					}
					fmt.Println(string(it.Value))
				}(time.Now())

			}
			wg.Wait()
		}
	}

	fmt.Println(max.String())

	//it, err = mc.Get("sc/info/full/seller/3743321700")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(string(it.Value))
}
