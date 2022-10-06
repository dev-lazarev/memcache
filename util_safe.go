//go:build appengine
// +build appengine

package memcache

func stobs(s string) []byte {
	return []byte(s)
}
