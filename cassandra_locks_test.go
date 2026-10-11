package cassandra_locks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlobalCassandraLockFactory_InitiallyInitialized(t *testing.T) {
	assert.NotNil(t, cassandraClientBeanFactory)
}
