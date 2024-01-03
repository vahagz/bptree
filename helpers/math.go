package helpers

// GetBit returns true if index'th bit of BYTE is 1, othervise false
func GetBit(BYTE, index uint8) bool {
	return (BYTE & (1<<index))>>index == 1
}

// SetBit sets 1 index'th bit of byte if val is true, othervise 0
func SetBit(BYTE *uint8, index uint8, val bool) {
	if val {
		*BYTE = *BYTE | (1 << index)
	} else {
		*BYTE = *BYTE &^ (1 << index)
	}
}
