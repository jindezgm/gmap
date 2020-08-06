/*
 * @Author: jinde.zgm
 * @Date: 2020-08-05 22:42:41
 * @Descripttion:
 */

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/jindezgm/gmap"
)

var logger = gmap.GetLogger()

func main() {
	var id int
	flag.IntVar(&id, "id", -1, "id")
	flag.Parse()

	if id < 0 || id > 2 {
		logger.Panicf("ID must be in [0, 2]")
	}

	endpoints := []string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"}
	gm, err := gmap.New(context.Background(), endpoints, id)
	if nil != err {
		logger.Panicf("create gmap failed:%v", err)
	}

	for {
		if gm.Leader() == id {
			fmt.Println("I am leader")
		} else {
			fmt.Println("I'm not leader")
		}
		time.Sleep(time.Second)
	}
}
