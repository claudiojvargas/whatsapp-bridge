package native

/*
static int native_probe() {
	return 42;
}
*/
import "C"

func Probe() int {
	return int(C.native_probe())
}
