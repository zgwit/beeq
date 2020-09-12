package main

import "github.com/zgwit/beeq"

func main()  {

	hive := beeq.Hive{}
	hive.ListenAndServe(":1883")
}