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
			InitialCap:        2,
			MaxIdle:           4,
			MaxCap:            5,
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

	count := 100
	wg := sync.WaitGroup{}
	wg.Add(count)

	for i, _ := range make([]int64, count) {
		go func(i int) {
			defer wg.Done()
			it, err := mc.Get("sc/seller/id/global/1102131071")
			if err != nil {
				fmt.Println(i, "=", err)
				return
			}
			fmt.Println(i, "=", string(it.Value))
		}(i)
	}

	wg.Wait()
	//it, err = mc.Get("sc/info/full/seller/3743321700")
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(string(it.Value))
}
