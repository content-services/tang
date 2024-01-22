package tangy

import (
	"context"
	"fmt"

	zerologadapter "github.com/jackc/pgx-zerolog"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/rs/zerolog/log"
)

func New(dbConfig Database, logConfig Logger) (Tangy, error) {
	pxConfig, err := pgxpool.ParseConfig(dbConfig.Url())
	if err != nil {
		return nil, err
	}

	if dbConfig.PoolLimit == 0 {
		dbConfig.PoolLimit = DefaultMaxPoolLimit
	}
	pxConfig.MaxConns = int32(dbConfig.PoolLimit)

	if logConfig.Logger != nil && logConfig.Enabled {
		zlog := zerologadapter.NewLogger(*logConfig.Logger)
		level, err := tracelog.LogLevelFromString(logConfig.LogLevel)
		if err != nil {
			log.Error().Err(err).Msg("Error setting Pgx log level")
		}
		pxConfig.ConnConfig.Tracer = &tracelog.TraceLog{
			Logger:   zlog,
			LogLevel: level,
		}
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), pxConfig)
	if err != nil {
		return nil, fmt.Errorf("error establishing connection: %w", err)
	}

	t := tangyImpl{
		pool:   pool,
		logger: logConfig,
	}
	return &t, nil
}

type tangyImpl struct {
	pool   *pgxpool.Pool
	logger Logger
}

//go:generate mockery --name Tangy --filename tangy_mock.go --inpackage
type Tangy interface {
	RpmRepositoryVersionPackageSearch(ctx context.Context, hrefs []string, search string, limit int) ([]RpmPackageSearch, error)
	RpmRepositoryVersionPackageGroupSearch(ctx context.Context, hrefs []string, search string, limit int) ([]RpmPackageGroupSearch, error)
	RpmRepositoryVersionEnvironmentSearch(ctx context.Context, hrefs []string, search string, limit int) ([]RpmEnvironmentSearch, error)
	Close()
}

// Close closes the DB connection pool
func (t *tangyImpl) Close() {
	t.pool.Close()
}
