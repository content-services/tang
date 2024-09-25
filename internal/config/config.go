package config

import (
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

type Config struct {
	Server   Server
	Database Database
}

func Get() Config {
	var c Config
	v := viper.New()

	v.SetConfigName("config.yaml")
	v.SetConfigType("yaml")
	v.AddConfigPath("./configs/")
	v.AddConfigPath("../../../configs/")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	setDefaults(v)

	err := v.ReadInConfig()
	if err != nil {
		log.Err(err).Msg("config.yaml file not loaded")
	}

	err = v.Unmarshal(&c)
	if err != nil {
		panic(err)
	}

	return c
}

func setDefaults(v *viper.Viper) {
	// need to set defaults for every setting until this feature is not experimental:
	// https://github.com/spf13/viper/pull/1715

	v.SetDefault("database.host", "")
	v.SetDefault("database.port", "")
	v.SetDefault("database.user", "")
	v.SetDefault("database.password", "")
	v.SetDefault("database.name", "")

	v.SetDefault("server.url", "")
	v.SetDefault("server.username", "")
	v.SetDefault("server.password", "")
	v.SetDefault("server.storage_type", "")
	v.SetDefault("server.download_policy", "")
}

// Server configuration options for connecting to a pulp server
type Server struct {
	Url            string `mapstructure:"url"`
	Username       string `mapstructure:"username"`
	Password       string `mapstructure:"password"`
	StorageType    string `mapstructure:"storage_type"`
	DownloadPolicy string `mapstructure:"download_policy"`
}

// Database configuration options for connection to a pulp database. Duplicated of tangy.Database.
type Database struct {
	Name       string `mapstructure:"name"`
	Host       string `mapstructure:"host"`
	Port       int    `mapstructure:"port"`
	User       string `mapstructure:"user"`
	Password   string `mapstructure:"password"`
	CACertPath string `mapstructure:"ca_cert_path"`
	PoolLimit  int    `mapstructure:"pool_limit"`
}
