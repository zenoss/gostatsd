package zenoss

import (
	log "github.com/sirupsen/logrus"
)

func zlog() *log.Entry {
	return zlogWithFields(log.Fields{})
}

func zlogRPC(rpc string) *log.Entry {
	return zlogRPCWithFields(rpc, log.Fields{})
}

func zlogRPCWithError(rpc string, err error) *log.Entry {
	return zlogRPCWithField(rpc, "error", err)
}

func zlogRPCWithField(rpc string, name string, value interface{}) *log.Entry {
	return zlogRPCWithFields(rpc, log.Fields{name: value})
}

func zlogRPCWithFields(rpc string, fields log.Fields) *log.Entry {
	fields["rpc"] = rpc
	return zlogWithFields(fields)
}

func zlogWithError(err error) *log.Entry {
	return zlogWithField("error", err)
}

func zlogWithField(name string, value interface{}) *log.Entry {
	return zlogWithFields(log.Fields{name: value})
}

func zlogWithFields(fields log.Fields) *log.Entry {
	fields["backend"] = BackendName
	return log.WithFields(fields)
}
