package event

import "strings"

func CommandDataType(cmd string) string {
	op := strings.ToLower(cmd)
	switch op {
	case "set", "setex", "psetex", "setnx", "append", "incr", "decr", "mset", "msetnx", "incrby", "decrby", "getset":
		return "string"
	case "hset", "hmset", "hdel", "hincrby", "hincrbyfloat":
		return "hash"
	case "lpush", "rpush", "lpop", "rpop", "ltrim", "lset", "linsert", "rpoplpush":
		return "list"
	case "sadd", "srem", "spop", "sinterstore", "sunionstore", "sdiffstore":
		return "set"
	case "zadd", "zrem", "zincrby", "zremrangebyrank", "zremrangebyscore":
		return "zset"
	default:
		return "unknown"
	}
}
