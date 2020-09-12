package main

import "github.com/zgwit/beeq"

func main()  {

	hive := beeq.NewHive()
	hive.ListenAndServe(":1883")
}