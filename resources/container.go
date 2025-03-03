package resources

type ResourcesContainer struct {
	// PostgreSQL connections.
	postrgres *PostgresResources
	// GRPC client connections.
	grpc *grpcResources
	// Redis connections.
}

func (r *ResourcesContainer) Postgres() *PostgresResources {
	return r.postrgres
}

func (r *ResourcesContainer) GRPC() *grpcResources {
	return r.grpc
}
