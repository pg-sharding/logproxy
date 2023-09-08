package general_test

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pg-sharding/logproxy"
	"github.com/stretchr/testify/assert"
)

func TestProxyPassesQueries(t *testing.T) {
	assert := assert.New(t)

	up := exec.Command("docker-compose", "up", "-d")
	err := up.Run()
	assert.NoError(err)

	defer func() {
		down := exec.Command("docker-compose", "down")
		err = down.Run()
		assert.NoError(err)
	}()

	time.Sleep(time.Second * 2)

	prox := logproxy.NewProxy("localhost", "5434", "logFiles/lll5.txt", "5433", "")
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		prox.Run(ctx)
	}()

	conn, err := pgx.Connect(ctx, "host=localhost port=5433 user=myuser dbname=mydb sslmode=disable")
	assert.NoError(err)

	_, err = conn.Exec(ctx, "SELECT 1")
	assert.NoError(err)

	_, err = conn.Exec(ctx, "CREATE TABLE test (i int)")
	assert.NoError(err)
	conn.Close(ctx)

	cancel()
	wg.Wait()

	connDb, err := pgx.Connect(context.Background(), "host=localhost port=5434 user=myuser dbname=mydb sslmode=disable")
	assert.NoError(err)

	_, err = connDb.Exec(context.Background(), "SELECT * FROM test")
	assert.NoError(err)
	connDb.Close(context.Background())
}

func TestProxyPeplaysLogs(t *testing.T) {
	assert := assert.New(t)

	f, err := os.OpenFile("logFiles/test_replay.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	assert.NoError(err)
	defer f.Close()
	f.Truncate(0)

	up := exec.Command("docker-compose", "up", "-d")
	err = up.Run()
	assert.NoError(err)

	defer func() {
		down := exec.Command("docker-compose", "down")
		err = down.Run()
		assert.NoError(err)
	}()

	time.Sleep(time.Second * 2)

	prox := logproxy.NewProxy("localhost", "5434", "logFiles/test_replay.txt", "5433", "")
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		prox.Run(ctx)
	}()

	conn, err := pgx.Connect(ctx, "host=localhost port=5433 user=myuser dbname=mydb sslmode=disable")
	assert.NoError(err)

	_, err = conn.Exec(ctx, "CREATE TABLE test (i int)")
	assert.NoError(err)
	conn.Close(ctx)

	cancel()
	wg.Wait()

	connDb, err := pgx.Connect(context.Background(), "host=localhost port=5434 user=myuser dbname=mydb sslmode=disable")
	assert.NoError(err)
	defer connDb.Close(context.Background())

	_, err = connDb.Exec(context.Background(), "CREATE DATABASE db2")
	assert.NoError(err)

	err = logproxy.ReplayLogs("localhost", "5434", "myuser", "db2", "logFiles/test_replay.txt")
	assert.NoError(err)

	_, err = connDb.Exec(context.Background(), "SELECT * FROM test")
	assert.NoError(err)
}
