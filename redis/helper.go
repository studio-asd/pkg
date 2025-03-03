package redis

import (
	"context"
	"time"

	rd "github.com/redis/go-redis/v9"
)

// Helper wraps the redis client to directly returns the redis command result output.
type Helper struct {
	client *rd.Client
}

func Wrap(client *rd.Client) *Helper {
	return &Helper{client: client}
}

// Ping implements call to redis PING command.
// Documentation: https://redis.io/commands/ping.
func (h *Helper) Ping(ctx context.Context) (string, error) {
	cmd := h.client.Ping(ctx)
	return cmd.Result()
}

// Set implements call to redis SET command.
// Documentation: https://redis.io/commands/set.
func (h *Helper) Set(ctx context.Context, key string, value interface{}) (string, error) {
	cmd := h.client.Set(ctx, key, value, 0)
	return cmd.Result()
}

// SetEX implements call to redis SETEX command.
// Documentation: https://redis.io/commands/setex.
func (h *Helper) SetEX(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error) {
	cmd := h.client.Set(ctx, key, value, expiration)
	return cmd.Result()
}

// SetNX implements call to redis SETNX command.
// Documentation: https://redis.io/commands/setnx.
func (h *Helper) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (int, error) {
	cmd := h.client.SetNX(ctx, key, value, expiration)

	i := 0
	if cmd.Val() {
		i = 1
	}
	return i, cmd.Err()
}

// Eval implements call to redis EVAL command.
// Documentation: https://redis.io/commands/eval.
func (h *Helper) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return h.client.Eval(ctx, script, keys, args...).Result()
}

// Get implements call to redis GET command.
// Documentation: https://redis.io/commands/get.
func (h *Helper) Get(ctx context.Context, key string) (string, error) {
	cmd := h.client.Get(ctx, key)
	return cmd.Result()
}

// Delete implements call to redis DEL command.
// Documentation: https://redis.io/commands/del.
func (h *Helper) Delete(ctx context.Context, keys ...string) (int64, error) {
	cmd := h.client.Del(ctx, keys...)
	return cmd.Result()
}

// Increment implements call to redis INCR command.
// Documentation: https://redis.io/commands/incr.
func (h *Helper) Increment(ctx context.Context, key string) (int64, error) {
	cmd := h.client.Incr(ctx, key)
	return cmd.Result()
}

// IncrementBy implements call to redis INCRBY command.
// Documentation: https://redis.io/commands/incrby.
func (h *Helper) IncrementBy(ctx context.Context, key string, amount int64) (int64, error) {
	cmd := h.client.IncrBy(ctx, key, amount)
	return cmd.Result()
}

// Expire implements call to redis EXPIRE command.
// Documentation: https://redis.io/commands/expire.
func (h *Helper) Expire(ctx context.Context, key string, expiration time.Duration) (int, error) {
	cmd := h.client.Expire(ctx, key, expiration)

	expireInt := 0
	if cmd.Val() {
		expireInt = 1
	}

	return expireInt, cmd.Err()
}

// MSet implements call to redis MSET command.
// Documentation: https://redis.io/commands/mset.
func (h *Helper) MSet(ctx context.Context, pairs ...interface{}) (string, error) {
	cmd := h.client.MSet(ctx, pairs...)
	return cmd.Result()
}

// MSet implements call to redis MSETNX command.
// Documentation: https://redis.io/commands/msetnx.
func (h *Helper) MSetNX(ctx context.Context, pairs ...interface{}) (result int, err error) {
	cmd := h.client.MSetNX(ctx, pairs...)

	ok, err := cmd.Result()
	if err != nil {
		return
	}
	if ok {
		result = 1
	}
	return
}

// MGet implements call to redis MGET command.
// Documentation: https://redis.io/commands/mget.
func (h *Helper) MGet(ctx context.Context, keys ...string) ([]string, error) {
	cmd := h.client.MGet(ctx, keys...)
	if err := cmd.Err(); err != nil {
		return nil, err
	}

	intfValues := cmd.Val()
	values := make([]string, len(intfValues))

	for idx := range intfValues {
		if intfValues[idx] == nil {
			intfValues[idx] = ""
		}
		values[idx] = intfValues[idx].(string)
	}
	return values, nil
}

// HSet implements call to redis HSET command.
// Documanetation: https://redis.io/commands/hset.
func (h *Helper) HSet(ctx context.Context, key string, values ...interface{}) (int64, error) {
	cmd := h.client.HSet(ctx, key, values...)
	return cmd.Result()
}

// HSetEX implements call to redis HSET and EXPIRE.
// This is a custom helper command.
func (h *Helper) HSetEX(ctx context.Context, key string, expiration time.Duration, values ...interface{}) (int64, error) {
	result, err := h.HSet(ctx, key, values)
	if err != nil {
		return result, err
	}
	if _, err := h.Expire(ctx, key, expiration); err != nil {
		return result, err
	}
	return result, nil
}

// HSetNX implements call to redis HSETNX command.
// Documentation: https://redis.io/commands/hsetnx.
func (h *Helper) HSetNX(ctx context.Context, key, field string, value interface{}) (result int, err error) {
	cmd := h.client.HSetNX(ctx, key, field, value)
	if err = cmd.Err(); err != nil {
		return
	}

	if cmd.Val() {
		result = 1
	}
	return
}

// HGet implements call to redis HGET command.
// Documentation: https://redis.io/commands/hget.
func (h *Helper) HGet(ctx context.Context, key, field string) (string, error) {
	cmd := h.client.HGet(ctx, key, field)
	return cmd.Result()
}

// HGetAll implements call to redis HGETALL command.
// Documentation: https://redis.io/commands/hgetall.
func (h *Helper) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	cmd := h.client.HGetAll(ctx, key)
	return cmd.Result()
}

func (h *Helper) HExpire(ctx context.Context, key string, expiration time.Duration, fields ...string) ([]int64, error) {
	cmd := h.client.HExpire(ctx, key, expiration, fields...)
	return cmd.Result()
}

// HMSet implements call to redis HMSET command.
// Documentation: https://redis.io/commands/hmset.
func (h *Helper) HMSet(ctx context.Context, key string, values ...interface{}) (string, error) {
	cmd := h.client.HMSet(ctx, key, values...)
	okStr := "OK"
	if err := cmd.Err(); err != nil {
		okStr = cmd.Err().Error()
	}

	return okStr, cmd.Err()
}

// HMGet implements call to redis HMGET command.
// Documentation: https://redis.io/commands/hmget.
func (h *Helper) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	cmd := h.client.HMGet(ctx, key, fields...)

	intfValues := cmd.Val()
	values := make([]string, len(intfValues))

	for idx := range intfValues {
		if intfValues[idx] == nil {
			intfValues[idx] = ""
		}
		values[idx] = intfValues[idx].(string)
	}
	return values, nil
}

// HDel implements call to redis HDELL command.
// Documentation: https://redis.io/commands/hdel.
func (h *Helper) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	cmd := h.client.HDel(ctx, key, fields...)
	return cmd.Result()
}

// LLen implements call to redis LLEN command.
// Documentation: https://redis.io/commands/llen.
func (h *Helper) LLen(ctx context.Context, key string) (int64, error) {
	cmd := h.client.LLen(ctx, key)
	return cmd.Result()
}

// LIndex implements call to redis LINDEX command.
// Documentation: https://redis.io/commands/lindex
func (h *Helper) LIndex(ctx context.Context, key string, index int64) (string, error) {
	cmd := h.client.LIndex(ctx, key, index)
	return cmd.Result()
}

// LSet implements call to redis LSET command.
// Documentation: https://redis.io/commands/lset
func (h *Helper) LSet(ctx context.Context, key string, index int64, value interface{}) (string, error) {
	cmd := h.client.LSet(ctx, key, index, value)
	return cmd.Result()
}

// LPush implements call to redis LPUSH command.
// Documentation: https://redis.io/commands/lpush.
func (h *Helper) LPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	cmd := h.client.LPush(ctx, key, values...)
	return cmd.Result()
}

// LPushX implements call to redis LPUSHX command.
// Documentation: https://redis.io/commands/lpushx.
func (h *Helper) LPushX(ctx context.Context, key string, values ...interface{}) (int64, error) {
	cmd := h.client.LPushX(ctx, key, values...)
	return cmd.Result()
}

// LPop implements call to redis LPOP command.
// Documentation: https://redis.io/commands/lpop.
func (h *Helper) LPop(ctx context.Context, key string) (string, error) {
	cmd := h.client.LPop(ctx, key)
	return cmd.Result()
}

// RPush implements call to redis RPUSH command.
// Documentation: https://redis.io/commands/rpush.
func (h *Helper) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	cmd := h.client.RPush(ctx, key, values...)
	return cmd.Result()
}

// RPushX implements call to redis RPUSHX command.
// Documentation: https://redis.io/commands/rpushx.
func (h *Helper) RPushX(ctx context.Context, key string, values ...interface{}) (int64, error) {
	cmd := h.client.RPushX(ctx, key, values...)
	return cmd.Result()
}

// RPop implements call to redis RPOP command.
// Documentation: https://redis.io/commands/rpop.
func (h *Helper) RPop(ctx context.Context, key string) (string, error) {
	cmd := h.client.RPop(ctx, key)
	return cmd.Result()
}

// LRem implements call to redis LREM command.
// Documentation: https://redis.io/commands/lrem.
func (h *Helper) LRem(ctx context.Context, key string, count int64, value interface{}) (int64, error) {
	cmd := h.client.LRem(ctx, key, count, value)
	return cmd.Result()
}

// LRange implements call to redis LREM command.
// Documentation: https://redis.io/commands/lrange.
func (h *Helper) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	cmd := h.client.LRange(ctx, key, start, stop)
	return cmd.Result()
}

// LTrim implements call to redis LTRIM command.
// Documentation: https://redis.io/commands/ltrim
func (h *Helper) LTrim(ctx context.Context, key string, start, stop int64) (string, error) {
	cmd := h.client.LTrim(ctx, key, start, stop)
	return cmd.Result()
}

func (h *Helper) FlushAll(ctx context.Context) (string, error) {
	cmd := h.client.FlushAll(ctx)
	return cmd.Result()
}

// Keys implements call to redis KEYS command.
// Documentation: https://redis.io/commands/keys
func (h *Helper) Keys(ctx context.Context, pattern string) ([]string, error) {
	cmd := h.client.Keys(ctx, pattern)
	return cmd.Result()
}

// Keys implements call to redis SCAN command.
// Documentation: https://redis.io/commands/scan
func (h *Helper) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	cmd := h.client.Scan(ctx, cursor, match, count)
	return cmd.Result()
}

// SAdd implements call to redis SADD command.
// Documentation: https://redis.io/commands/sadd
func (h *Helper) SAdd(ctx context.Context, key string, members ...interface{}) (int64, error) {
	cmd := h.client.SAdd(ctx, key, members)
	return cmd.Result()
}

// SRem implements call to redis SREM command.
// Documentation: https://redis.io/commands/srem
func (h *Helper) SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	cmd := h.client.SRem(ctx, key, members)
	return cmd.Result()
}

// SMembers implements call to redis SMEMBERS command.
// Documentation: https://redis.io/commands/smembers
func (h *Helper) SMembers(ctx context.Context, key string) ([]string, error) {
	cmd := h.client.SMembers(ctx, key)
	return cmd.Result()
}
