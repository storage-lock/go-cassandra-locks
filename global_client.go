package cassandra_locks

import (
	"context"

	"github.com/gocql/gocql"
	cassandra_storage "github.com/storage-lock/go-cassandra-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_lock_factory "github.com/storage-lock/go-storage-lock-factory"
)

type cassandraClientConnectionManager struct {
	session *gocql.Session
}

func (m *cassandraClientConnectionManager) Name() string {
	return "cassandra-client-connection-manager"
}

func (m *cassandraClientConnectionManager) Take(ctx context.Context) (*gocql.Session, error) {
	return m.session, nil
}

func (m *cassandraClientConnectionManager) Return(ctx context.Context, connection *gocql.Session) error {
	return nil
}

func (m *cassandraClientConnectionManager) Shutdown(ctx context.Context) error {
	return nil
}

var cassandraClientBeanFactory = storage_lock_factory.NewStorageLockFactoryBeanFactory[*gocql.Session, *gocql.Session]()

// newCassandraStorage 构造存储，包级变量以便单测注入假存储/构造失败。
// 默认透传真实 cassandra_storage.NewCassandraStorage（返回 *CassandraStorage，满足 storage.Storage）。
// 单测可注入内存假存储走通成功路径（本地无 cassandra 真后端），或注入错误覆盖失败分支。
var newCassandraStorage = func(ctx context.Context, options *cassandra_storage.CassandraStorageOptions) (storage.Storage, error) {
	return cassandra_storage.NewCassandraStorage(ctx, options)
}

func NewCassandraLockBySession(ctx context.Context, session *gocql.Session, keyspace string, lockId string) (*storage_lock.StorageLock, error) {
	factory, err := GetCassandraLockFactoryBySession(ctx, session, keyspace)
	if err != nil {
		return nil, err
	}
	return factory.CreateLock(lockId)
}

func GetCassandraLockFactoryBySession(ctx context.Context, session *gocql.Session, keyspace string) (*storage_lock_factory.StorageLockFactory[*gocql.Session], error) {
	return cassandraClientBeanFactory.GetOrInit(ctx, session, func(ctx context.Context) (*storage_lock_factory.StorageLockFactory[*gocql.Session], error) {
		cm := cassandra_storage.NewCassandraConnectionManagerFromSession(session)
		options := cassandra_storage.NewCassandraStorageOptions().
			SetConnectionManager(cm).
			SetKeyspace(keyspace).
			SetTimeProvider(&cassandra_storage.LocalTimeProvider{})
		storage, err := newCassandraStorage(ctx, options)
		if err != nil {
			return nil, err
		}
		return storage_lock_factory.NewStorageLockFactory[*gocql.Session](storage, nil), nil
	})
}
