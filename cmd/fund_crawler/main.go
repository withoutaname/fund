package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/araddon/dateparse"
	"github.com/influxdata/influxdb/client/v2"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

var (
	logger       *zap.Logger
	influxClient client.Client
)

type FundNode struct {
	Code    string
	Abridge string
	Name    string
	Type    string
	Pinyin  string
}

type FundDetail struct {
	// 日期
	FSRQ string
	// 单位净值
	DWJZ string
	// 累计净值
	LJJZ string

	SDATE     string
	ACTUALSYI string
	NAVTYPE   string
	JZZZL     string
	// 申购状态
	SGZT string
	// 赎回状态
	SHZT   string
	FHFCZ  string
	FHFCBZ string
	DTYPE  string
	FHSP   string
}

type FundDetails struct {
	LSJZList  []FundDetail
	FundType  string
	SYType    string
	isNewType bool
	Feature   string
}

type FundInfo struct {
	Data       FundDetails
	ErrCode    int
	ErrMsg     string
	TotalCount int
	Expansion  string
	PageSize   int
	PageIndex  int
}

func init() {
	var err error
	logger, _ = zap.NewDevelopment()
	influxClient, err = client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		logger.Fatal("init influxdb client error", zap.Error(err))
	}
	logger.Info("init successfully")
}

func getNodeList() ([]FundNode, error) {
	resp, err := getHttpResponse("http://fund.eastmoney.com/js/fundcode_search.js")
	if err != nil {
		return nil, err
	}
	body := resp.Body
	defer body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(body)
	s := buf.String()
	s = s[strings.Index(s, "=")+1:]
	s = strings.TrimSpace(s)
	s = s[1 : len(s)-3]
	lists := strings.Split(s, "],")
	nodeList := make([]FundNode, len(lists))
	for i, list := range lists {
		segs := strings.Split(list, "\",\"")
		if len(segs) != 5 {
			log.Printf("invalid node: %s\n", list)
			continue
		}
		nodeList[i] = FundNode{
			Code:    strings.Trim(segs[0], "\"[]"),
			Abridge: strings.Trim(segs[1], "\"[]"),
			Name:    strings.Trim(segs[2], "\"[]"),
			Type:    strings.Trim(segs[3], "\"[]"),
			Pinyin:  strings.Trim(segs[4], "\"[]"),
		}
	}
	return nodeList, nil
}

func getHttpResponse(u string) (*http.Response, error) {
	cnt := 3
	var resp *http.Response
	fundUrl, err := url.Parse(u)
	if err != nil {
		return nil, err
	}
	for cnt > 0 {
		req := http.Request{
			URL: fundUrl,
			Header: map[string][]string{
				"Referer":    []string{"http://fund.eastmoney.com/f10/jjjz_519961.html"},
				"User-Agent": []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"},
			},
		}
		resp, err = http.DefaultClient.Do(&req)
		if err != nil {
			err = fmt.Errorf("http get error, err=[%v], url=[%s]", err, fundUrl)
			cnt--
			continue
		}
		if resp.StatusCode != 200 {
			err = fmt.Errorf("http code error, code=[%d], url=[%s]", resp.StatusCode, fundUrl)
			cnt--
			continue
		}
		return resp, nil
	}
	return nil, err
}

func parseFund(resp *http.Response) (FundInfo, error) {
	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return FundInfo{}, err
	}
	data := string(bs)
	if beg := strings.Index(data, "{"); beg == -1 {
		return FundInfo{}, fmt.Errorf("invalid response body")
	} else {
		data = data[beg:]
	}
	dec := json.NewDecoder(strings.NewReader(data))
	var info FundInfo
	err = dec.Decode(&info)
	return info, err
}

func getFund(node FundNode) error {
	if node.Code == "" {
		return fmt.Errorf("empty fund code")
	}
	timestamp := (time.Now().Unix()-2)*1000 - rand.Int63n(1000)
	pageIndex := 1
	fundUrl := fmt.Sprintf("http://api.fund.eastmoney.com/f10/lsjz?callback=jQuer&fundCode=%s&pageIndex=%d&pageSize=20&startDate=&endDate=&_=%d", node.Code, pageIndex, timestamp)
	resp, err := getHttpResponse(fundUrl)
	if err != nil {
		return err
	}
	logger.Debug("get http fund successfully", zap.String("code", node.Code), zap.String("name", node.Name))
	fundInfo, err := parseFund(resp)
	if err != nil {
		return err
	}
	if fundInfo.ErrCode != 0 {
		return fmt.Errorf("fund info error, code=[%d], msg=[%s], index=[%d], size=[%d]", fundInfo.ErrCode, fundInfo.ErrMsg, fundInfo.PageIndex, fundInfo.PageSize)
	}
	if err = sink(node, fundInfo); err != nil {
		return err
	}

	return nil
}

func sink(node FundNode, info FundInfo) error {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database: "fund",
	})
	if err != nil {
		return err
	}
	tags := map[string]string{
		"code":    node.Code,
		"abridge": node.Abridge,
		"name":    node.Name,
		"type":    node.Type,
		"pinyin":  node.Pinyin,
	}
	for _, fund := range info.Data.LSJZList {
		ptime, err := dateparse.ParseLocal(fund.FSRQ)
		if err != nil {
			return err
		}
		fields := map[string]interface{}{
			"NAVTYPE": fund.NAVTYPE,
			"SGZT":    fund.SGZT,
			"SHZT":    fund.SHZT,
		}
		if DWJZ, err := strconv.ParseFloat(fund.DWJZ, 64); err == nil {
			fields["DWJZ"] = DWJZ
		}
		if LJJZ, err := strconv.ParseFloat(fund.LJJZ, 64); err == nil {
			fields["LJJZ"] = LJJZ
		}
		if JZZZL, err := strconv.ParseFloat(fund.JZZZL, 64); err == nil {
			fields["JZZZL"] = JZZZL
		}
		pt, err := client.NewPoint("fund", tags, fields, ptime)
		if err != nil {
			return err
		}
		bp.AddPoint(pt)
	}
	if err := influxClient.Write(bp); err != nil {
		return err
	} else {
		logger.Debug("sink successfully", zap.String("code", node.Code), zap.String("name", node.Name), zap.Int("count", len(bp.Points())))
	}
	return nil
}

func run() error {
	nodeList, err := getNodeList()
	if err != nil {
		return err
	}
	for _, fundNode := range nodeList {
		if err := getFund(fundNode); err != nil {
			logger.Error("get fund error", zap.String("code", fundNode.Code), zap.String("name", fundNode.Name), zap.Error(err))
		}
		time.Sleep(time.Second)
	}
	return nil
}

func main() {
	for {
		logger.Info("begin run")
		if err := run(); err != nil {
			logger.Error("run error", zap.Error(err))
		}
	}
}
