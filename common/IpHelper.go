package common

import (
	"net"

	"github.com/golang/glog"
	"github.com/kataras/iris/core/errors"
)

func GetIntranetIp() (string, error) {
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		glog.Infof("GetIntranetIp %s", err)
		panic(err)
	}
	if len(addrs) == 0 {
		glog.Infof("did not found ip addrs")
		panic("did not found ip addrs")
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				glog.Infof("ip:ipnet.IP.String()")
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("did not found suitable ip")
}
