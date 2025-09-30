package tangy

import (
	"fmt"

	"github.com/rs/zerolog"
)

const DefaultMaxPoolLimit int32 = 20

// Logger configuration options for logger
type Logger struct {
	Logger   *zerolog.Logger
	LogLevel string
	Enabled  bool
}

// Database configuration options for connection to a pulp database
type Database struct {
	Name       string
	Host       string
	Port       int
	User       string
	Password   string
	CACertPath string `mapstructure:"ca_cert_path"`
	PoolLimit  int32  `mapstructure:"pool_limit"`
}

// Url return url of database
func (d Database) Url() string {
	connectStr := fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%d",
		d.User,
		d.Password,
		d.Name,
		d.Host,
		d.Port,
	)

	var sslStr string
	if d.CACertPath == "" {
		sslStr = " sslmode=disable"
	} else {
		sslStr = fmt.Sprintf(" sslmode=verify-full sslrootcert=%s", d.CACertPath)
	}
	return connectStr + sslStr
}
