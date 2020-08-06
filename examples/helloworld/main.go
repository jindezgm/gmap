/*
 * @Author: jinde.zgm
 * @Date: 2020-08-05 23:05:03
 * @Descripttion:
 */

package main

import (
	"context"
	"flag"
	"time"

	"github.com/google/uuid"
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
	gm, err := gmap.New(context.Background(), endpoints, id, "")
	if nil != err {
		logger.Panicf("create gmap failed:%v", err)
	}

	m, err := gm.Map("", "helloworld")
	if nil != err {
		logger.Panicf("get map %s failed:%v", "helloworld", err)
	}
	watcher := m.Watch()
	defer watcher.Close()

	go func() {
		for event := range watcher.WatchChan() {
			logger.Infof("event:%d, key:%s, value:%v", event.Type, event.KV.Key, event.KV.Value)
		}
	}()

	for {
		if id == gm.Leader() {
			if err := m.Put(uuid.New().String(), "helloworld"); nil != err {
				logger.Errorf("put map %s failed:%v", "helloworld", err)
			}
		}

		time.Sleep(time.Second)
	}

}
