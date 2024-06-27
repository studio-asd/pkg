package resources

type Config struct {
	Postgres *PostgresResourcesConfig `yaml:"postgres"`
}

func (c Config) Validate() error {
	if err := c.Postgres.Validate(); err != nil {
		return err
	}
	return nil
}

type Resources struct {
}

func New() {

}
