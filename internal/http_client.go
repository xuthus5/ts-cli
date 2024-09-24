package internal

import (
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type HttpClient struct {
	Host       string
	Port       int
	BasicToken string
	c          *http.Client
}

func (receiver *HttpClient) generateBasicToken(u, p string) {
	encodeString := u + ":" + p
	authorization := "Basic " + base64.StdEncoding.EncodeToString([]byte(encodeString))
	receiver.BasicToken = authorization
}

var stdHttpClient *HttpClient

func NewHttpClient(host string, port int) {
	stdHttpClient = &HttpClient{
		Host: host,
		Port: port,
		c: &http.Client{
			Timeout: time.Second * 5,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout: time.Second * 5,
				}).DialContext,
			},
		},
	}
}

type QueryValue struct {
	Database        string
	RetentionPolicy string
	Command         string
	Precision       string
}

func (receiver *HttpClient) query(q *QueryValue) ([]byte, error) {
	var qv = make(url.Values)
	qv.Add("db", q.Database)
	qv.Add("rp", q.RetentionPolicy)
	qv.Add("q", q.Command)
	qv.Add("epoch", q.Precision)
	var u = fmt.Sprintf("http://%s:%d/query", receiver.Host, receiver.Port)
	return receiver.innerRequest(u, strings.NewReader(qv.Encode()))
}

type WriteValue struct {
	Database        string
	RetentionPolicy string
	LineProtocol    io.Reader
}

func (receiver *HttpClient) write(w *WriteValue) error {
	var u = fmt.Sprintf("http://%s:%d/write?db=%s&rp=%s", receiver.Host, receiver.Port, w.Database, w.RetentionPolicy)
	_, err := receiver.innerRequest(u, w.LineProtocol)
	return err
}

func (receiver *HttpClient) innerRequest(u string, reader io.Reader) ([]byte, error) {
	req, err := http.NewRequest(http.MethodPost, u, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if receiver.BasicToken != "" {
		req.Header.Set("Authorization", "Basic "+receiver.BasicToken)
	}
	resp, err := receiver.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	r, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return r, nil
}
