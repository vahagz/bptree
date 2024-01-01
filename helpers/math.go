package helpers

func GetBit(BYTE, index uint8) bool {
	return (BYTE & (1<<index))>>index == 1
}

func SetBit(BYTE *uint8, index uint8, val bool) {
	if val {
		*BYTE = *BYTE | (1 << index)
	} else {
		*BYTE = *BYTE &^ (1 << index)
	}
}
