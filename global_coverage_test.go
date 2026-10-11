package cassandra_locks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	cassandra_storage "github.com/storage-lock/go-cassandra-storage"
	"github.com/storage-lock/go-storage"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

// fakeStorage 内存假存储：满足 CAS+可靠时间，走通锁创建与加解锁，不依赖真实 cassandra。
type fakeStorage struct {
	data map[string]string
	vers map[string]storage.Version
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{data: map[string]string{}, vers: map[string]storage.Version{}}
}

func (f *fakeStorage) GetName() string { return "fake-cassandra-storage" }
func (f *fakeStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{storage.CapabilityCAS, storage.CapabilityReliableTime, storage.CapabilityAtomicDelete}
}
func (f *fakeStorage) Init(ctx context.Context) error  { return nil }
func (f *fakeStorage) Close(ctx context.Context) error { return nil }
func (f *fakeStorage) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}
func (f *fakeStorage) Get(ctx context.Context, lockId string) (string, error) {
	v, ok := f.data[lockId]
	if !ok {
		return "", storage_lock.ErrLockNotFound
	}
	return v, nil
}
func (f *fakeStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, info *storage.LockInformation) error {
	if _, ok := f.data[lockId]; ok {
		return storage_lock.ErrLockAlreadyExists
	}
	f.data[lockId] = info.ToJsonString()
	f.vers[lockId] = version
	return nil
}
func (f *fakeStorage) UpdateWithVersion(ctx context.Context, lockId string, expected, newVersion storage.Version, info *storage.LockInformation) error {
	v, ok := f.vers[lockId]
	if !ok {
		return storage_lock.ErrVersionMiss
	}
	if v != expected {
		return storage_lock.ErrVersionMiss
	}
	f.data[lockId] = info.ToJsonString()
	f.vers[lockId] = newVersion
	return nil
}
func (f *fakeStorage) DeleteWithVersion(ctx context.Context, lockId string, expected storage.Version, info *storage.LockInformation) error {
	v, ok := f.vers[lockId]
	if !ok {
		return storage_lock.ErrLockNotFound
	}
	if v != expected {
		return storage_lock.ErrVersionMiss
	}
	delete(f.data, lockId)
	delete(f.vers, lockId)
	return nil
}
func (f *fakeStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	infos := make([]*storage.LockInformation, 0, len(f.data))
	for _, s := range f.data {
		info, err := storage.LockInformationFromJsonString(s)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return go_iterator.FromSlice(infos), nil
}

func withFakeCassandraStorage() func() {
	old := newCassandraStorage
	newCassandraStorage = func(ctx context.Context, options *cassandra_storage.CassandraStorageOptions) (storage.Storage, error) {
		return newFakeStorage(), nil
	}
	return func() { newCassandraStorage = old }
}

func TestCassandraClientConnectionManager(t *testing.T) {
	var session *gocql.Session
	m := &cassandraClientConnectionManager{session: session}
	assert.Equal(t, "cassandra-client-connection-manager", m.Name())

	ctx := context.Background()
	got, err := m.Take(ctx)
	assert.Nil(t, err)
	assert.Nil(t, got)
	assert.Nil(t, m.Return(ctx, got))
	assert.Nil(t, m.Shutdown(ctx))
}

func TestGetCassandraLockFactoryBySession(t *testing.T) {
	restore := withFakeCassandraStorage()
	defer restore()

	ctx := context.Background()
	var session *gocql.Session

	factory, err := GetCassandraLockFactoryBySession(ctx, session, "test-ks")
	assert.Nil(t, err)
	assert.NotNil(t, factory)

	// 缓存命中
	factory2, err := GetCassandraLockFactoryBySession(ctx, session, "test-ks")
	assert.Nil(t, err)
	assert.Equal(t, factory, factory2)

	// 端到端加解锁
	lock, err := NewCassandraLockBySession(ctx, session, "test-ks", "test-cassandra-lock")
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.Nil(t, lock.Lock(ctx, "owner-1"))
	assert.Nil(t, lock.UnLock(ctx, "owner-1"))
}

func TestGetCassandraLockFactoryBySession_StorageError(t *testing.T) {
	old := newCassandraStorage
	newCassandraStorage = func(ctx context.Context, options *cassandra_storage.CassandraStorageOptions) (storage.Storage, error) {
		return nil, errors.New("boom")
	}
	defer func() { newCassandraStorage = old }()

	ctx := context.Background()
	// 用独立 session 指针避免命中前面成功用例的缓存（bean factory 按指针缓存）
	errSession := &gocql.Session{}

	_, err := GetCassandraLockFactoryBySession(ctx, errSession, "test-ks")
	assert.NotNil(t, err)
	_, err = NewCassandraLockBySession(ctx, errSession, "test-ks", "x")
	assert.NotNil(t, err)
}

func TestNewCassandraStorage_DefaultPassthrough(t *testing.T) {
	// 直调默认透传体：空 options 触发 Check 快速失败，不联网。
	ctx := context.Background()
	_, err := newCassandraStorage(ctx, &cassandra_storage.CassandraStorageOptions{})
	assert.NotNil(t, err)
}
