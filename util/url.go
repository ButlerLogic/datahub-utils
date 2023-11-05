package util

import (
	"net/url"
	"strings"
)

func EncodeURL(root string, apikey ...string) string {
	if strings.Contains(root, "@") {
		base := strings.Index(root, "://") + 3
		at := strings.Index(root, "@")
		creds := root[base:at]
		if len(apikey) > 0 {
			root = root[:base] + root[(at+1):]
		} else {
			parts := strings.Split(creds, ":")
			password := parts[1]
			encodedPassword := url.QueryEscape(password)
			encodedURL := root[:base] + parts[0] + ":" + encodedPassword + root[at:]
			root = encodedURL
		}
	}

	return root
}
