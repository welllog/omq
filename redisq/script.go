package redisq

const (
	_PUSH_READY = `redis.call('rpush',KEYS[1],ARGV[1]);redis.call('setex',KEYS[2],ARGV[2],ARGV[3]);return 1`
	_PUSH_DELAY = `redis.call('zadd',KEYS[1],ARGV[1],ARGV[2]);redis.call('setex',KEYS[2],ARGV[3],ARGV[4]);return 1`

	_SIZE = `return redis.call('llen',KEYS[1])+redis.call('zcard',KEYS[2])+redis.call('zcard',KEYS[3])`

	_DELAY_TO_READY = `local v=redis.call('zrangebyscore',KEYS[1],'-inf',ARGV[1]);` +
		`if next(v)~=nil then redis.call('zremrangebyrank',KEYS[1],0,#v-1);` +
		`redis.call('rpush',KEYS[2],unpack(v)) end;return #v`

	_SAFE_POP_READY = `local a=redis.call('lrange',KEYS[1],0,ARGV[1]);local b={};` +
		`if next(a)~=nil then redis.call('ltrim',KEYS[1],#a,-1);` +
		`for k,v in ipairs(a) do table.insert(b,ARGV[2]);table.insert(b,v) end;` +
		`redis.call('zadd',KEYS[2],unpack(b)) end;` +
		`return a`

	_COMMIT_TO_READY = `local a=redis.call('zrangebyscore',KEYS[1],'-inf',ARGV[1],'limit',0,ARGV[2]);local b={};` +
		`if next(a)~=nil then redis.call('zremrangebyrank',KEYS[1],0,#a-1);` +
		`for k,v in ipairs(a) do if redis.call('exists',ARGV[3]..v)==1 then table.insert(b,v) end end;` +
		`if next(b)~=nil then redis.call('rpush',KEYS[2],unpack(b)) end end;return #b`

	_POP_READY = `local v=redis.call('lrange',KEYS[1],0,ARGV[1]);` +
		`if next(v)~=nil then redis.call('ltrim',KEYS[1],#v,-1) end;` +
		`return v`

	_REMOVE_MSG = `redis.call('zrem',KEYS[1],ARGV[1]);redis.call('del',KEYS[2]);return 1`
)
