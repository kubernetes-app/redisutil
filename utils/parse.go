/*
Copyright 2021 kubernetes-app Solutions.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package utils

import (
	"strconv"
	"strings"
)

func ParseRedisMemConf(p string) (string, error) {
	var mul int64 = 1
	u := strings.ToLower(p)
	digits := u

	if strings.HasSuffix(u, "k") {
		digits = u[:len(u)-len("k")]
		mul = 1000
	} else if strings.HasSuffix(u, "kb") {
		digits = u[:len(u)-len("kb")]
		mul = 1024
	} else if strings.HasSuffix(u, "m") {
		digits = u[:len(u)-len("m")]
		mul = 1000 * 1000
	} else if strings.HasSuffix(u, "mb") {
		digits = u[:len(u)-len("mb")]
		mul = 1024 * 1024
	} else if strings.HasSuffix(u, "g") {
		digits = u[:len(u)-len("g")]
		mul = 1000 * 1000 * 1000
	} else if strings.HasSuffix(u, "gb") {
		digits = u[:len(u)-len("gb")]
		mul = 1024 * 1024 * 1024
	} else if strings.HasSuffix(u, "b") {
		digits = u[:len(u)-len("b")]
		mul = 1
	}

	val, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return "", err
	}

	return strconv.FormatInt(val*mul, 10), nil
}
