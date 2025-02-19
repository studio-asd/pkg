package resources

type RedisResourcesConfig struct{}

type RedisOverridableConfig struct {
	DialTimeout    Duration `yaml:"dial_timeout"`
	MaxActiveConns int      `yaml:"max_active_conns"`
	MaxIdleConns   int      `yaml:"max_idle_conns"`
	MaxRetries     int      `yaml:"max_retries"`
	WriteTimeout   Duration `yaml:"write_timeout"`
	ReadTimeout    Duration `yaml:"read_timeout"`
	PoolTimeout    Duration `yaml:"pool_timeout"`
}

func (r *RedisOverridableConfig) SetDefault() {
}

type RedisConnConfig struct {
	Address                string `yaml:"address"`
	Username               string `yaml:"username"`
	Password               string `yaml:"password"`
	RedisOverridableConfig `yaml:",inline"`
}
