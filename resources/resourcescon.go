package resources

type ResourcesContainer struct {
	// PostgreSQL connections.
	postrgres *postgresResources
	// GRPC client connections.
	grpc *grpcResources
	// Redis connections.
}

func (r *ResourcesContainer) Postgres() *postgresResources {
	return r.postrgres
}

func (r *ResourcesContainer) GRPC() *grpcResources {
	return r.grpc
}
