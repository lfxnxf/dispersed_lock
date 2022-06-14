package dispersed_lock

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/lfxnxf/while"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"time"
)

const (
	// 解锁lua
	unLockScript = "if redis.call('get', KEYS[1]) == ARGV[1] " +
		"then redis.call('del', KEYS[1]) return 1 " +
		"else " +
		"return 0 " +
		"end"

	// 看门狗lua
	watchLogScript = "if redis.call('get', KEYS[1]) == ARGV[1] " +
		"then return redis.call('expire', KEYS[1], ARGV[2]) " +
		"else " +
		"return 0 " +
		"end"

	lockMaxLoopNum = 1000 //加锁最大循环数量
)

var scriptMap sync.Map

type option func() (bool, error)

type DispersedLock struct {
	key            string        // 锁key
	value          string        // 锁的值，随机数
	expire         int           // 锁过期时间,单位秒
	lockClient     redis.Cmdable  // 锁客户端，暂时只有redis
	unLockScript   string        // lua脚本
	watchLogScript string        // 看门狗lua
	options        []option      // 事件
	unlockCh       chan struct{} // 解锁通知通道
}

func New(ctx context.Context, client redis.Cmdable, key string, expire int) *DispersedLock {
	d := &DispersedLock{
		key:    key,
		expire: expire,
		value:  fmt.Sprintf("%d", Random(100000000, 999999999)), // 随机值作为锁的值
	}

	//初始化连接
	d.lockClient = client

	//初始化lua script
	lockScript, _ := scriptMap.LoadOrStore("dispersed_lock", d.getScript(ctx, unLockScript))
	watchLogScript, _ := scriptMap.LoadOrStore("watch_log", d.getScript(ctx, watchLogScript))

	d.unLockScript = lockScript.(string)
	d.watchLogScript = watchLogScript.(string)

	d.unlockCh = make(chan struct{}, 0)

	return d
}

func (d *DispersedLock) getScript(ctx context.Context, script string) string {
	scriptString, _ := d.lockClient.ScriptLoad(ctx, script).Result()
	return scriptString
}

//注册事件
func (d *DispersedLock) RegisterOptions(f ...option) {
	d.options = append(d.options, f...)
}

//加锁
func (d *DispersedLock) Lock(ctx context.Context) bool {
	ok, _ := d.lockClient.SetNX(ctx, d.key, d.value, time.Duration(d.expire)*time.Second).Result()
	if ok {
		go d.watchDog(ctx)
	}
	return ok
}

//循环加锁
func (d *DispersedLock) LoopLock(ctx context.Context, sleepTime int) bool {
	t := time.NewTicker(time.Duration(sleepTime) * time.Millisecond)
	w := while.NewWhile(lockMaxLoopNum)
	w.For(func() {
		if d.Lock(ctx) {
			t.Stop()
			w.Break()
		} else {
			<-t.C
		}
	})
	if !w.IsNormal() {
		return false
	}
	return true
}

//循环获取锁并且绑定事件
//eg:单个线程获取缓存、其它线程等待
func (d *DispersedLock) LoopLockWithOption(ctx context.Context, sleepTime int) (bool, error) {
	t := time.NewTicker(time.Duration(sleepTime) * time.Millisecond)
	w := while.NewWhile(lockMaxLoopNum)
	var err error
	w.For(func() {
		locked := d.Lock(ctx)
		if locked { // 获取到锁，跳出循环
			t.Stop()
			w.Break()
		}

		var flag bool
		for _, option := range d.options {
			flag, err = option()
			if err != nil { //事件代码出现异常，跳出循环
				t.Stop()
				w.Break()
			}
			if !flag {
				break
			}
		}

		//所有事件全部为true，不用等到获取锁，直接跳出
		if flag {
			t.Stop()
			w.Break()
		}

		<-t.C
	})
	return true, err
}

//解锁
func (d *DispersedLock) Unlock(ctx context.Context) bool {
	args := []interface{}{
		d.value, // 脚本中的argv
	}
	flag, _ := d.lockClient.EvalSha(ctx, d.unLockScript, []string{d.key}, args...).Result()
	// 关闭看门狗
	d.unlockCh <- struct{}{}
	return lockRes(flag.(int64))
}

//看门狗
func (d *DispersedLock) watchDog(ctx context.Context) {
	// 创建一个定时器NewTicker, 每过期时间的3分之2触发一次
	loopTime := time.Duration(d.expire*1e3/3) * time.Millisecond
	expTicker := time.NewTicker(loopTime)
	//确认锁与锁续期打包原子化
	for {
		select {
		case <-expTicker.C:
			args := []interface{}{
				d.value,
				d.expire,
			}
			res, err := d.lockClient.EvalSha(ctx, d.watchLogScript, []string{d.key}, args...).Result()
			if err != nil {
				fmt.Println("watchDog error", err)
				return
			}
			r, ok := res.(int64)
			if !ok {
				return
			}
			if r == 0 {
				return
			}
		case <-d.unlockCh: //任务完成后用户解锁通知看门狗退出
			return
		}
	}
}

func lockRes(flag int64) bool {
	if flag > 0 {
		return true
	} else {
		return false
	}
}

func Random(min, max int64) int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(max-min+1) + min
}

