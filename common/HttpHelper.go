package common

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/golang/glog"
)

const (
	Illegal_Input_para = -1
)

func Logging(f http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.URL.Path)
		f(w, r)
	}
}

func MakeResponse(code int, errMsg string) (data []byte) {
	obj := map[string]interface{}{"note": errMsg, "code": code}
	b, err := json.Marshal(obj)
	if err != nil {
		glog.Error(err)
	}
	return b
}

func MakeResponseWithErr(code int, err error) (data []byte) {
	glog.Warning(err)
	return MakeResponse(code, err.Error())
}

func MakeOkRspByData(data interface{}) (b []byte) {
	b, err := json.Marshal(data)
	if err != nil {
		glog.Error(err)
	}
	return b
}
