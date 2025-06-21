package kpgx

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/vingarcia/ksql"
	"github.com/vingarcia/ksql/sqldialect"
)

func TestAdapter(t *testing.T) {
	ctx := context.Background()

	postgresURL, closePostgres := startPostgresDB(ctx, "ksql")
	defer closePostgres()

	ksql.RunTestsForAdapter(t, "kpgx5", sqldialect.PostgresDialect{}, postgresURL, func(t *testing.T) (ksql.DBAdapter, io.Closer) {
		pool, err := pgxpool.New(ctx, postgresURL)
		if err != nil {
			t.Fatal(err.Error())
		}
		return PGXAdapter{pool}, closerAdapter{close: pool.Close}
	})
}

//
// func TestTransaction(t *testing.T) {
// 	ctx := context.Background()
//
// 	postgresURL, closePostgres := startPostgresDB(ctx, "ksql")
// 	defer closePostgres()
//
// 	pool, err := pgxpool.New(ctx, postgresURL)
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
//
// 	db, err := NewFromPgxPool(pool)
// 	if err != nil {
// 		t.Fatal(err.Error())
// 	}
//
// 	err = db.Transaction(ctx, func(p ksql.Provider) error {
// 		tx, ok :=
//
// 		return nil
// 	})
//
// 	tt.AssertNotEqual(t, err, nil)
// }

type closerAdapter struct {
	close func()
}

func (c closerAdapter) Close() error {
	c.close()
	return nil
}

func startPostgresDB(ctx context.Context, dbName string) (databaseURL string, closer func()) {
	startTime := time.Now()

	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "postgres",
			Tag:        "14.0",
			Env: []string{
				"POSTGRES_PASSWORD=postgres",
				"POSTGRES_USER=postgres",
				"POSTGRES_DB=" + dbName,
				"listen_addresses = '*'",
			},
		},
		func(config *docker.HostConfig) {
			// set AutoRemove to true so that stopped container goes away by itself
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		},
	)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	hostAndPort := resource.GetHostPort("5432/tcp")
	databaseUrl := fmt.Sprintf("postgres://postgres:postgres@%s/%s?sslmode=disable", hostAndPort, dbName)

	fmt.Println("Connecting to postgres on url: ", databaseUrl)

	resource.Expire(40) // Tell docker to hard kill the container in 40 seconds

	var sqlDB *pgxpool.Pool
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 10 * time.Second
	err = pool.Retry(func() error {
		sqlDB, err = pgxpool.New(ctx, databaseUrl)
		if err != nil {
			return err
		}

		return sqlDB.Ping(ctx)
	})
	if err != nil {
		log.Fatalf("Could not connect to docker after %v: %s", time.Since(startTime), err)
	}
	sqlDB.Close()

	fmt.Printf("db ready to run in %v", time.Since(startTime))

	return databaseUrl, func() {
		if err := pool.Purge(resource); err != nil {
			log.Fatalf("Could not purge resource: %s", err)
		}
	}
}
