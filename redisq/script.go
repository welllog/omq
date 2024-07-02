package redisq

import "github.com/redis/go-redis/v9"

var (
	// KEYS[1] => {<prefix>:<partition>}:ready
	// KEYS[2] => {<prefix>:<partition>}:<msgID>
	// ARGV[1] => msgID
	// ARGV[2] => msg
	// ARGV[3] => maxRetry
	// ARGV[4] => ttl
	_pushReadyCmd = redis.NewScript(
		`redis.call('RPUSH',KEYS[1],ARGV[1]);` +
			`redis.call('HSET',KEYS[2],'msg',ARGV[2],'retry',ARGV[3]);` +
			`redis.call('EXPIRE',KEYS[2],ARGV[4]);` +
			`return redis.status_reply('OK')`,
	)

	// KEYS[1] => {<prefix>:<partition>}:delay
	// KEYS[2] => {<prefix>:<partition>}:<msgID>
	// ARGV[1] => timestamp of run at
	// ARGV[2] => msgID
	// ARGV[3] => msg
	// ARGV[4] => maxRetry
	// ARGV[5] => ttl
	_pushDelayCmd = redis.NewScript(
		`redis.call('ZADD',KEYS[1],ARGV[1],ARGV[2]);` +
			`redis.call('HSET',KEYS[2],'msg',ARGV[3],'retry',ARGV[4]);` +
			`redis.call('EXPIRE',KEYS[2],ARGV[5]);` +
			`return redis.status_reply('OK')`,
	)

	// KEYS[1] => {<prefix>:<partition>}:ready
	// KEYS[2] => {<prefix>:<partition>}:delay
	// KEYS[3] => {<prefix>:<partition>}:unCommit
	_sizeCmd = redis.NewScript(
		`return redis.call('LLEN',KEYS[1])+redis.call('ZCARD',KEYS[2])+redis.call('ZCARD',KEYS[3])`,
	)

	// KEYS[1] => {<prefix>:<partition>}:delay
	// KEYS[2] => {<prefix>:<partition>}:unCommit
	_delaySizeCmd = redis.NewScript(
		`return redis.call('ZCARD',KEYS[1])+redis.call('ZCARD',KEYS[2])`,
	)

	// KEYS[1] => {<prefix>:<partition>}:ready
	// KEYS[2] => {<prefix>:<partition>}:unCommit
	_readySizeCmd = redis.NewScript(
		`return redis.call('LLEN',KEYS[1])+redis.call('ZCARD',KEYS[2])`,
	)

	// KEYS[1] => {<prefix>:<partition>}:delay
	// KEYS[2] => {<prefix>:<partition>}:ready
	// ARGV[1] => timestamp of run at
	// ARGV[2] => get delay task num
	_delayToReadyCmd = redis.NewScript(
		`local v=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'LIMIT',0,ARGV[2]);` +
			`if next(v)~=nil then redis.call('ZREMRANGEBYRANK',KEYS[1],0,#v-1);` +
			`redis.call('RPUSH',KEYS[2],unpack(v)) end;return #v`,
	)

	// KEYS[1] => {<prefix>:<partition>}:ready
	// KEYS[2] => {<prefix>:<partition>}:unCommit
	// ARGV[1] => {<prefix>:<partition>}:
	// ARGV[2] => timestamp of now
	// out: {msgID, msg, retry}
	_fetchReadyCmd = redis.NewScript(
		`local a,b,c,k=redis.call('LPOP',KEYS[1]);` +
			`if a then k=ARGV[1]..a;b=redis.call('HMGET',k,'retry','msg');` +
			`if b[2]==false then return redis.error_reply("msg not found") end;` +
			`c=b[1] and tonumber(b[1]) or 0;` +
			`if c>0 then redis.call('ZADD',KEYS[2],ARGV[2],a);redis.call('HSET',k,'retry',c-1) end;` +
			`return {a,b[2],tostring(c)} end;` +
			`return {}`,
	)

	// KEYS[1] => {<prefix>:<partition>}:ready
	// KEYS[2] => {<prefix>:<partition>}:unCommit
	// ARGV[1] => timestamp of now
	// value: @$retry#$rawMsg
	// out: {@d#msg, retry}
	_uniqueFetchReadyCmd = redis.NewScript(
		`local a,b,c,e,r=redis.call('LPOP',KEYS[1]);` +
			`if a then e=string.find(a,'#');if not e then return redis.error_reply("msg invalid") end;` +
			`b=string.sub(a,2,e-1);c=b and tonumber(b) or 0;` +
			`if c>0 then r=string.sub(a,e);redis.call('ZADD',KEYS[2],ARGV[1],'@'..(c-1)..r) end;` +
			`return {a,tostring(c)} end;return{}`,
	)

	// KEYS[1] => {<prefix>:<partition>}:delay
	// KEYS[2] => {<prefix>:<partition>}:unCommit
	// ARGV[1] => {<prefix>:<partition>}:
	// ARGV[2] => timestamp of now
	// out: {msgID, msg, retry}
	_fetchDelayCmd = redis.NewScript(
		`local a,b,c,k=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[2],'LIMIT',0,1);` +
			`if next(a)~=nil then redis.call('ZREM',KEYS[1],unpack(a));k=ARGV[1]..a[1];b=redis.call('HMGET',k,'retry','msg');` +
			`if b[2]==false then return redis.error_reply("msg not found") end;` +
			`c=b[1] and tonumber(b[1]) or 0;` +
			`if c>0 then redis.call('ZADD',KEYS[2],ARGV[2],a[1]);redis.call('HSET',k,'retry',c-1) end;` +
			`return {a[1],b[2],tostring(c)} end;` +
			`return {}`,
	)

	// KEYS[1] => {<prefix>:<partition>}:delay
	// KEYS[2] => {<prefix>:<partition>}:unCommit
	// ARGV[1] => timestamp of now
	// value: @$retry#$rawMsg
	// out: {@d#msg, retry}
	_uniqueFetchDelayCmd = redis.NewScript(
		`local a,b,c,e,r=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'LIMIT',0,1);` +
			`if next(a)~=nil then redis.call('ZREM',KEYS[1],unpack(a));e=string.find(a[1],'#');` +
			`if not e then return redis.error_reply("msg invalid") end;` +
			`b=string.sub(a[1],2,e-1);c=b and tonumber(b) or 0;` +
			`if c>0 then r=string.sub(a[1],e);redis.call('ZADD',KEYS[2],ARGV[1],'@'..(c-1)..r) end;` +
			`return {a[1],tostring(c)} end;return{}`,
	)

	// KEYS[1] => {<prefix>:<partition>}:unCommit
	// KEYS[2] => {<prefix>:<partition>}:<msgID>
	// ARGV[1] => msgID
	_removeMsgCmd = redis.NewScript(
		`redis.call('ZREM',KEYS[1],ARGV[1]);redis.call('DEL',KEYS[2]);return redis.status_reply('OK')`,
	)

	// KEYS[1] => {<prefix>:<partition>}:unCommit
	// KEYS[2] => {<prefix>:<partition>}:ready
	// ARGV[1] => timestamp of timeout
	// ARGV[2] => task num
	// ARGV[3] => {<prefix>:<partition>}:
	_unCommitToReadyCmd = redis.NewScript(
		`local a=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'limit',0,ARGV[2]);local b={};` +
			`if next(a)~=nil then redis.call('ZREMRANGEBYRANK',KEYS[1],0,#a-1);` +
			`for k,v in ipairs(a) do if redis.call('EXISTS',ARGV[3]..v)==1 then table.insert(b,v) end end;` +
			`if next(b)~=nil then redis.call('RPUSH',KEYS[2],unpack(b)) end end;return #b`,
	)

	// KEYS[1] => {<prefix>:<partition>}:unCommit
	// KEYS[2] => {<prefix>:<partition>}:delay
	// ARGV[1] => timestamp of timeout
	// ARGV[2] => task num
	// ARGV[3] => {<prefix>:<partition>}:
	// ARGV[4] => timestamp of enqueue
	_unCommitToDelayCmd = redis.NewScript(
		`local a=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'limit',0,ARGV[2]);local n=0;` +
			`if next(a)~=nil then redis.call('ZREMRANGEBYRANK',KEYS[1],0,#a-1);` +
			`for k,v in ipairs(a) do if redis.call('EXISTS',ARGV[3]..v)==1 then ` +
			`n=n+redis.call('ZADD',KEYS[2],ARGV[4],v) end end end;return n`,
	)

	// KEYS[1] => {<prefix>:<partition>}:unCommit
	// KEYS[2] => {<prefix>:<partition>}:ready
	// ARGV[1] => timestamp of timeout
	// ARGV[2] => task num
	_uniqueUnCommitToReadyCmd = redis.NewScript(
		`local a=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'limit',0,ARGV[2]);` +
			`if next(a)~=nil then redis.call('ZREMRANGEBYRANK',KEYS[1],0,#a-1);` +
			`return redis.call('RPUSH',KEYS[2],unpack(a)) end;return 0`,
	)

	// KEYS[1] => {<prefix>:<partition>}:unCommit
	// KEYS[2] => {<prefix>:<partition>}:delay
	// ARGV[1] => timestamp of timeout
	// ARGV[2] => task num
	// ARGV[3] => timestamp of enqueue
	_uniqueUnCommitToDelayCmd = redis.NewScript(
		`local a=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',ARGV[1],'limit',0,ARGV[2]);local n=0;` +
			`if next(a)~=nil then redis.call('ZREMRANGEBYRANK',KEYS[1],0,#a-1);` +
			`for k,v in ipairs(a) do n=n+redis.call('ZADD',KEYS[2],ARGV[3],v) end end;return n`,
	)
)
