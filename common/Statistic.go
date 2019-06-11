package common

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/golang/glog"
)

var glbStatistic *Statistic = &Statistic{}
var lock sync.Mutex

type StatisticInfo struct {
	cnt       int64
	totaltime int64
}

type StatisticInfoMap map[string]*StatisticInfo

type Statistic struct {
	totalMap   StatisticInfoMap
	enable     bool
	unusedtime time.Time
}

type StatisticFragment struct {
	mnemonic  string
	starttime time.Time
}

func GetStatisticInstance() *Statistic {
	return glbStatistic
}

func (this *Statistic) Init(enable bool, interval int) {
	this.enable = enable
	this.unusedtime = time.Now()
	if !enable {
		return
	}

	this.totalMap = make(StatisticInfoMap)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	go func() {
		for _ = range ticker.C {
			glog.Info("start =====================")
			lock.Lock()
			var sslice []string
			for key, _ := range glbStatistic.totalMap {
				sslice = append(sslice, key)
			}
			sort.Strings(sslice)

			for _, mnemonic := range sslice {
				info := glbStatistic.totalMap[mnemonic]
				if info == nil || info.cnt == 0 {
					continue
				}
				avg := info.totaltime / info.cnt
				avg = avg / 1000
				glog.Infof("    %s cnt:%d avg:%dus total:%ds",
					mnemonic, info.cnt, avg, info.totaltime/1000000000)
			}
			lock.Unlock()
			glog.Info("end =====================")
		}
	}()
}

func StartTrace(mnemonic string) *StatisticFragment {
	if !glbStatistic.enable {
		return &StatisticFragment{
			mnemonic:  "",
			starttime: glbStatistic.unusedtime,
		}
	}

	return &StatisticFragment{
		mnemonic:  mnemonic,
		starttime: time.Now(),
	}
}

func (this *StatisticFragment) StopTrace(tid int) {
	if !glbStatistic.enable {
		return
	}

	lock.Lock()
	defer lock.Unlock()
	timeused := time.Since(this.starttime).Nanoseconds()
	key := fmt.Sprintf("%s@%d", this.mnemonic, tid)
	info, ok := glbStatistic.totalMap[key]
	if !ok {
		info := &StatisticInfo{}
		info.cnt = 1
		info.totaltime = timeused
	} else {
		if info == nil {
			info = &StatisticInfo{}
		}
		info.cnt += 1
		info.totaltime += timeused
	}
	glbStatistic.totalMap[key] = info
}
