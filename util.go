/*
 * @Author: jinde.zgm
 * @Date: 2020-08-04 21:10:48
 * @Descripttion:
 */

package gmap

import "encoding/json"

// mustMarshal
func mustMarshal(i interface{}) []byte {
	data, err := json.Marshal(i)
	if nil != err {
		logger.Panicf("json marshal failed:%v", err)
	}

	return data
}

// mustUnmarshal
func mustUnmarshal(data []byte, i interface{}) {
	if err := json.Unmarshal(data, i); nil != err {
		logger.Panicf("json unmarshal failed:%v", err)
	}
}
