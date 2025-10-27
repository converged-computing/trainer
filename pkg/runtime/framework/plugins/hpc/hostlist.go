package hpc

import "fmt"

// generateHostlist for a specific size given a host prefix and a size
func generateHostlist(prefix string, size int32) string {

	// Assume a setup without bursting / changing size.
	// We can extend this in the future to allow adding hosts
	return fmt.Sprintf("%s-[%s]", prefix, generateRange(size, 0))
}

// generateRange is a shared function to generate a range string
func generateRange(size int32, start int32) string {
	var rangeString string
	if size == 1 {
		rangeString = fmt.Sprintf("%d", start)
	} else {
		rangeString = fmt.Sprintf("%d-%d", start, (start+size)-1)
	}
	return rangeString
}
