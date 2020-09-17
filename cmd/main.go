package main

import (
	"git.zgwit.com/iot/beeq"
	"time"
)

func main()  {

	hive := beeq.NewHive()
	hive.ListenAndServe(":1883")

	for {
		time.Sleep(time.Minute)
	}
}