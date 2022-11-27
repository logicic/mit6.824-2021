# kvraft

需要一个background的goroutine来监听applyCh是否完成，注意是每一个server，不然就会有server的db没有数据。

那么，要有一个background的goroutine，那么怎么知道哪一个command是哪一次请求的呢？怎么知道请求有没有重复执行？（由于网络忙碌，client重传后，第一次和重传的都到server了，需要handle，PS：即使接口做到了幂等性，即多次请求相同都会有相同的结果对于client来说，但是对于server来说，它就是两个独立的请求，被raft记录，被kv执行了操作，这个理论上应该是要处理的，而且更重要的是，如果在A=1，A=2，执行后，滞后的A=1再一次执行，这个很糟糕）

那么如何唯一区分某一个command呢？

由于是client的重传机制，那么就需要client的配合，在client端增加command的唯一自增id，而如果是current client，有多个client，commandID就会重复，那么如何进行进一步的区分呢？增加clientID进行client的区分，只要clientID唯一，那么理论上就可以区分所有的command了。如何让clientID唯一呢？

但是有一个问题：clientID和commandID其实是不可靠的，如果client重启，

- 在server返回成功前重启，已经成功了，client的逻辑重启后不应该重试（GET重试无所谓，关键是APPEND，PUT）
- 在server超时了，包还在网络中，client重启了，不应该重试，重试了算作另外的一次请求了，从动作上看，确实是应该把其当作第二次请求，虽然它们的key value可能一样



leader身份下，完成一次操作：
（一个client一次只能处理一个commandID，因为client发请求的时候有锁，直至这一次commandID成功，无论是被哪一个server报处理完毕）
1. 创建client-channel【LOCK】
2. 判断clientID-commandID
   1. 如果commandID > args.commmandID：说明args.commandID已经被apply，返回OK
   2. 如果commandID == args.commmandID：说明该commandID已经被更新到了kv中，但是有可能还未来得及send channel进行通知，就timeout了，但是对于kv数据来说是已经成功了的。返回OK
   3. 如果commandID < args.commmandID：该commandID未被处理，进入3
3. 把数据传入raft系统中，进行replicate
4. 返回一个commandID相匹配的channel（如果不存在则创建，存在则直接返回），进行等待（能够通过上述的commandID判断，说明这一个command是还未execute到kv中的）
5. 等待command channel的成功返回【UNLOCK】
   1. applier更新kv并把lastcommand自增（被锁为一个原子操作），由于更新了kv，则进行一个snapshot的判断，符合条件则进行snapshot操作
   2. 如果存在commandID channel，并该server是leader则发送，并close&delete channel
   3. 如果不存在则pass
   2. 如果成功，则返回OK
   3. 如果超时，则返回ErrTimeOut（client进行循环commandID进行同一个server请求，直至报成功（包括nokey）或者ErrWrongLeader）

## 数据结构设计
k-v db的抽象，暂时直接使用map作为数据结构，之后可以抽象出一个database object向kvraft提供存储服务，至于这个object内部是怎么实现的，用什么数据结构进行存储，就不是kvraft关心的问题了，只要这个db object抽象出满足它的接口即可。
```golang
kvStore map[string]string
```

channel的抽象，其实可以使用`map[int64]chan int64`，不需要双层map结构，因为一个client只能处理一个command（被client 锁锁住了），只需要记录最新的client所持有的channel即可，我也试过了，但是sys time消耗巨大，我用下面的结构sys time 1min左右，用上面的单map，sys time为4min左右，其他的user 以及real time没什么大的变化。为什么这样暂时没什么头绪
```golang
clientCh      map[int64]map[int64]chan int64
```

lastCommand的抽象，保存该client已经执行最新的commandID
```golang
lastCommandID map[int64]int64
```

channel的使用原则，在单消费者和产生者中，在sender中关闭比较合理，在recv中关闭，sender会panic，虽然可以增加recover func进行恢复，但是多少有点不优雅，所以，要么增加一个toStop chan与command channel组成一对，在op select中timeout，则发送toStop，但是又有可能applier还未进行等待，这个又无线轮回了。所以现在的策略是，在op 中make command channel，在applier中如果这个commandID还未被execute，并有command channel且未被发送，则发送后由sender进行close。
```golang
if commandCh != nil {
    DPrintf("[Server] <applier> %d enter rf.GetState clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
    // 2.3 only leader role can send data to channel
    if term, isLeader := kv.rf.GetState(); isLeader && op.Term >= term {
        DPrintf("[Server] <applier> %d sendback1 clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
        // commandCh <- op.CommandID
        select {
        case commandCh <- op.CommandID:
        case <-time.After(1 * time.Second):
        }
        DPrintf("[Server] <applier> %d sendback2 clientID[%d] commandID[%d]\n", kv.me, op.ClientID, op.CommandID)
    }
    kv.mu.Lock()
    kv.deleteCommandChanWithoutLOCK(op)
    kv.mu.Unlock()
}
```

## FIX
原raft 2D我的实现中，是保存了所有logEntry的数据到snapshot中，是我理解错了，这里的snapshot是
针对的kv的snapshot来说，而不是针对raft的logEntry来说，即kv保存的数据=logEntry committed后并执行的数据，对现有logEntry persist以及对kv数据全量snapshot，即可对整个系统进行备份了。

在2D中，snapshot发送的最新的commited logEntry已经够用了。
在3A中，snapshot需要发送的是所有的kv的interface打包。
即，raft中log state以及persist中保留还 未达到长度的snapShotSize，既有commited的数据也有未commited的数据
在snapshot中保留的是全部的当前最新的 已commited的全部数据，但是存储什么的逻辑应该是在raft外层做，
所以raft内层的逻辑不应该对其进行加工了。

2D中的具体操作：
所有的server都会检查apply后的commited的长度，是否达到上限
达到上限的server，会把最新已commited的log给剪切，在raft state和persist中只保留到未commited的全部数据
并且把传进到snapshot function的[]bytes给保存到persiste.snapshot当中。
此时，某些follower会落后于leader，而leader的log 数据已经snapshot了，在raft.log中只有未commited，
不是某些落后follower想要的数据，在代码中体现为，nextIndex比leader的能索引到的logIndex小，那么会触发
一个snapshot rpc的请求，发送到这个follower上，告诉follower当前的最新commited log，在这个之前以及这一个
都不需要在等了（已经得到绝大多数的认可了，commited），剪切follower的log（尽管他没得剪切，只能说是移除），更新
snapshot到最新的commited log，并触发applyCh到上层服务，告诉上层服务，最新的数据已经更新到这个了。上层服务也有一个conInstallSnapshot的回调
函数，执行上层逻辑后，补充raft层的逻辑操作。此时应该执行persist持久化当前的更改。