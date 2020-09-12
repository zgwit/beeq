package main

import "git.zgwit.com/iot/beeq"

func main()  {

	hive := beeq.NewHive()
	hive.ListenAndServe(":1883")
}