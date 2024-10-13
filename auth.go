package pubsub

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"github.com/google/uuid"
	md "google.golang.org/grpc/metadata"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type auth struct {
	ClientId    string
	Brand       string
	OnlyRoot    bool
	Address     string
	Publishers  []string
	Subscribers []string
	nonce       string
	timestamp   string
	sign        string
}

func (a *auth) WithContext(ctx context.Context) context.Context {
	onlyRoot := "false"
	if a.OnlyRoot {
		onlyRoot = "true"
	}

	return md.NewOutgoingContext(ctx, md.Pairs(
		"client-id", a.ClientId,
		"brand-id", a.Brand,
		"nonce", a.nonce,
		"address", a.Address,
		"only-root", onlyRoot,
		"timestamp", a.timestamp,
		"sign", a.sign,
		"publishers", strings.Join(a.Publishers, ","),
		"subscribers", strings.Join(a.Subscribers, ","),
	))
}

func createAuth(md *auth, token string) *auth {
	md.nonce = getMD5Encode(uuid.New().String())
	md.timestamp = strconv.Itoa(int(time.Now().Unix()))
	values := url.Values{}
	values.Set("client-id", md.ClientId)
	values.Set("nonce", md.nonce)
	values.Set("timestamp", md.timestamp)
	md.sign = createSign(token, values.Encode())

	return md
}

func createSign(token, query string) string {
	hm := hmac.New(sha1.New, []byte(token))
	hm.Write([]byte(query))
	return hex.EncodeToString(hm.Sum(nil))
}

func getMD5Encode(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}
