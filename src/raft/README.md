# The Raft Consensus Algorithm

raft的由来，为了简化分布式一致性算法，增加`understanding`可理解性，以及可以在工业界上构建出方便简单的系统，而设计出来的，其中放弃某些东西，而选择了某些东西，来达到可理解性，和教学性。

所以，使用`Leader`来简化replicated log的管理。比如，不需要进行cluster间server的“商讨”就可以决定新`logEntry`的添加，通过`Leader`一个server就可以决定，并转发到其他server上。`Leader` Failed就重新elect新的`leader`。

引入leader的概念后，从而需要维护leader的整个逻辑链路的完备性和正确性，需要解决以下三个相对独立的问题：

- Leader Election：当现存的`Leader`发生错误时一个新的`Leader`必须被选举出来

- Log Replication：`Leader`同意apply来自client的log entries，并replicate这些log entries到cluster中，强制其他server与自己（`Leader`）同步

- Safety：如何保证每个server的log entries是一致的，并且cluster中曾经applied过的log entries不会被覆盖和改变

  - Election Safety: 一个任期至多只能有一个`Leader`
  - Leader Append-Only：一个`Leader`从不复写或者删除entries在它(`Leader`)的log上，它仅仅只是append新的entries
  - Log Matching：如果两个logs中包含的一个相同的index和term的entry，那么在这个logs上经由这个index的所有entries是完全一致的
  - Leader Completeness：如果entry被committed在leader的一个任期中，那么这个entry应该要一直存在于之后所有更高的任期当中，不会被复写和删除
  - State Machine Safety：如果一个server已经applied一个在某个index的log entry到state machine，那么，不会再有其他的server再使用相同index来apply不同的log entry了，即在所有的server中，index和entry都是一一对应，并一致的。

  



抽象出三种身份：

- Follower：
  - response给`Candidates`和`Leaders`
  - 如果选举超时并且没有收到`AppendEntries`RPC（来自`Leader`的heartbeat或者append entries或者来自candidate的投票申请），则转换身份为`Candidate`
  - 如果client发送command请求到`Follower`，则redirect请求到`Leader`中
- Candidate：
  - 一旦转换为`Candidate`，马上开始进行选举：
    - 增加当前任期数值
    - 投票给自己
    - 重置选举定时器
    - 发生`RequestVote`RPC给cluster中所有的servers
  - 如果收到cluster中大多数的投票，则身份转换为`Leader`
  - 如果收到了来自`Leader`的`AppendEntries`RPC，则转换为`Follower`
  - 如果选举定时器超时，则开始新一轮的投票选举申请
- Leader：
  - 经由选举成为`Leader`，发送空的`AppendEntries`RPC作为heartbeat到每一个server上，并重复轮询则操作，以阻止其他server的选举发生，表明自己`Leader`的server还存活。（保活）
  - 如果接收到了来自client的命令请求，先append到local log，等到local log被applied时，response回client，表明执行状态
  - `Leader`发送`AppendEntries`RPC到`Followers`时，如果最新的log index>=nextIndex：
    - 如果成功，则更新对应的`Follower`的nextIndex 和 matchIndex
    - 如果失败，则表明`Follower`和`Leader`之间不一致，降低`Leader`的nextIndex并重试
  - 更新commitIndex的机制：如果`Leader`在运行过程中，发现有这么一个N，使得N>=commitIndex, 大多数的matchIndex[i]>=N,并且log[N].term==currentTerm，那么置commitIndex=N

## Issues：

- 如何保证`Leader`发生错误能够被发现？
  - heartbeat
- 如何保证一个cluster仅有一个`Leader`被选举出来？
  - heartbeat维护自己的身份
  - 失效后，`Candidate` elect 成为 `Leader`
  - term机制保证

Action：

- 当follower没有收到leader的heartbeat，发生timeout，会从follower转变为candidate，发起投票
- 首先投自己一票
- 当获得cluster中的大多数投票时，自己获选leader，term任期递增
  - 仅有follower身份可以投票，并且新term要比followercurrentTerm要大，follower才会投票，并且follower仅会投出一票，先到先得
  - 成为leader，发送heartbeat维护自己的身份
- 如果在等待投票结果的过程中，收到了别的leader的heartbeat，并且term>=自己，则身份转换为follower
- 如果在选举时间超时时，还没有得到大多数的投票，则并且也没有收到别的server的leader宣告，则表明，平票了，需要重新进入选举，为新的任期发起投票。
  - 为了减少这种平票重新选举，一直选不出leader的消耗，增加了一个小措施：election timeout是random的，这就以一个小措施，减少了平票的可能性
  - 其实还讨论过一个方案：ranked server，higher-ranked server可以有更大的概念选举出leader，但是有些复杂，在实际中还是随机比较好用


## 实际代码方案的思考

1. 定时器设计
   1. 能够每隔一段时间发送心跳包，心跳包会打断electionTimeout，并重新计时
   2. 能够在electionTimeout触发时，发起选举
2. 选举
   1. 选举发生的触发：electionTimeout的触发
   2. 选举的动作：
      1. 身份由follower转变为candidate，仅有follower才能发起选举，并成为candidate
      2. 首先投自己一票，任期term加1
      3. 并发发起投票给所有peer，投票需要满足：
         1. 发起投票的`Candidate`的任期term需要比follower大
         2. 没有投票给任何人（包括自己）都可以进行投票
         3. （如果是`Leader`投票，那么它的身份为变为`Follower`）
         4. 当接收到大多数的投票时（记住投票结果分为，没能收到投票的reply，收到reply以及reply的同意和不同意），voteCount大于peer的大多数时，成为这个term的`Leader`，并发送心跳包通知所有的peer；`Candidate`收到大于等于自己任期term的心跳包，会变为`Follower`，放弃当前的选举；`Follower`收到大于等于自己任期term的心跳包，重置electionTimeout；如果小于自己任期term，则返回失败和自己的任期回去；如果当前任期term在electionTimeout发生前，既没有成功竞选成功，也没有其他peer宣称它是`Leader`（心跳包），则开启新一轮选举，放弃当前选举的结果，身份`Candidate`不变
         5. 细节：
            1. 处于选举等待投票结果的过程的candidate，该如何处理发过来的心跳包呢？如果当前心跳包的任期不比currentTern小，那么这次自己的选举作废（如何code作废呢？），变为follower
            2. 选举等待投票结果的过程中，已经收到了多数票了，但是还有其他peer处于等待投票结果返回当中（或者已经失联了），这时不应该再继续等待了，可以处理大于等于peer多数的逻辑，成为leader，发送心跳包
            3. 发送所有peer投票的请求，应该如何处理？异步？同步阻塞等待？
            4. 发送所有peer心跳包的请求，应该如何处理？

Append logEntry过程

1. 只有leader处理来自client的command，非leader转发到leader

2. leader接收client的command后，转发当前想要添加到committed的log entry，到所有的followers，附加到心跳包上，发送出去，follower接收到时，提取出log entry，所带的参数有：

   1. `term`： 当follower term  > `term`，return false
   2. `leaderId`: 用于redirect
   3. `prevLogIndex`: entries[]所接的list末尾索引
   4. `prevLogTerm`: `prevLogIndex`的`term`
   5. `entries[]`:日志
   6. `leaderCommit`：leader已知已提交的日志条目的索引

3. append逻辑：

   1. return false，如果follower term  > `term`
   2. return false，如果在接收者中没有这么一个item，即这个item在`prevLogIndex`上能和`prevLogTerm`匹配上
   3. 如果能匹配上，则接收者在匹配上的item后续和即将append的entries有冲突（index相同，而term不同，则为冲突），则删除接收者发生冲突的item以及之后的（比较后面的所有item，移除不一致的，以leader发来的为准）
   4. 删除不一致的以后，添加还未存在的item到接收者
   5. 如果`leaderCommit`>`commmitIndex`，则把`commitIndex`置为MIN{`leaderCommit`和len(`commitIndex`) }

4. log state update method

   1. `commitIndex`：假设存在 N 满足`N > commitIndex`，使得大多数的 `matchIndex[i] ≥ N`以及`log[N].term == currentTerm` 成立，则令 `commitIndex = N`
   2. `lastApplied`:如果`commitIndex > lastApplied`，则 lastApplied 递增，并将`log[lastApplied]`应用到状态机中
   3. `nextIndex[]`：append成功则更新相应follower的`nextIndex`，如果发生冲突而失败则`nextIndex`递减重试
   4. `matchIndex[]`：append成功则更新相应follower的`matchIndex`

5. append log 触发机制：

   1. `Leader`的最后日志条目的索引值大于等于 nextIndex（`last Log Index ≥ nextIndex`），则发送从 nextIndex 开始的所有日志条目

   



可能的问题：

重新选举新leader的时候，选举时间非常长。任期处理得好，问题也不大

appendEntry失败时，nextIndex的回退是单步回退，可能比较慢。后面已经处理这个问题了。

nextIndex reinitial是为了减少从follower变为leader时，nextIndex除了自己外其他的peer在它这边的内存值都为0，因为一开始的时它并不是leader，没有其他人的记录。



## Issues record

1. 在`AppendEntriesReply`添加字段`RealNextIndex`：为了缩短leader和follower之间同步log index的时间，而添加的辅助字段，本质上不会影响逻辑

2. ```go
   	if args.PrevLogIndex < 0 {
   		reply.Success = false
   		reply.RealNextIndex = NONE
   		return
   	}
   // 这段代码，我认为可以reply.RealNextIndex = NONE替为reply.RealNextIndex = 1
   ```

3. ```go
   // 这段代码就是和要点1的realNextINdex字段的赋值逻辑	
   
   // append logentry
   	if args.PrevLogIndex > len(rf.logEntries)-1 || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
   		fmt.Printf("%d append leader %d prevLogIndex:%d prevLogTerm:%d\n", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
   		reply.Success = false
   		if args.PrevLogIndex > len(rf.logEntries)-1 {
   			reply.RealNextIndex = len(rf.logEntries) - 1
   		} else {
   			conflictTerm := rf.logEntries[args.PrevLogIndex].Term
   			reply.RealNextIndex = 1
   			for i := args.PrevLogIndex - 1; i > 0; i-- {
   				if rf.logEntries[i].Term != conflictTerm {
   					reply.RealNextIndex = i + 1
   					break
   				}
   			}
   		}
   		if reply.RealNextIndex == 0 {
   			reply.RealNextIndex = 1
   		}
   		return
   	}
   ```

4. ```go
   // 原本使用的是注释的代码，因为考虑到，如果新增加的log entry，是替换之前index所在的空间，而不是append，那么lastLogIndex，还是取的最后面的log entry index，而不是所替的当前位置的index，之后可以优化，既可以取到所替换位置的index，也可以使用index进行赋值，这样效率应该会快一些
   
   	if len(args.LogEntries) > 0 {
   		rf.logEntries = append(rf.logEntries[:args.PrevLogIndex+1], args.LogEntries...)
   		rf.persist()
   	}
   
   	// if len(args.LogEntries) > 0 {
   	// 	for i, v := range args.LogEntries {
   	// 		index := args.PrevLogIndex + 1 + i
   	// 		if len(rf.logEntries)-1 >= index {
   	// 			// overwrite existed log
   	// 			rf.logEntries[index] = v
   	// 		} else {
   	// 			// append new log
   	// 			rf.logEntries = append(rf.logEntries, v)
   	// 		}
   	// 	}
   	// 	rf.persist()
   	// }
   ```

5. ```go
   // 在doLeader function 中，这里是为了配合要点2的，但感觉没必要
   if reply.RealNextIndex == NONE {
   	// out-dated reply
   	return
   }
   ```

6. ```go
   // 在doLeader function 中，移除了下面的判断规则，因为有时候，即使是发送心跳包，也可以更新nextIndex[]的值，不然需要等到有新的command更新到leader的log中才会运行到下面的update nextIndex[]的逻辑
   if lastLogIndex >= nextIndex {
   ...
   }
   ```

7. ```go
   // 配合要点1和要点3的realNextIndex，来加快更新nextIndex[]
   					} else {
   						// retry
   						rf.nextIndex[pindex] = reply.RealNextIndex
   						fmt.Printf("%d not append nextIndex[%d]:%v\n", rf.me, pindex, rf.nextIndex)
   
   					}
   ```

8. ```go
   // 在checkLogEntries function中，增加了&& rf.role == LEADER判断，多做一层判断
   // 最重要的一个更改为，rf.logEntries[i].Term != rf.currentTerm，这个判断过滤是必须且重要的，之前的bug找了很久都找不清楚。BUG现象描述：假设5个节点ABCDE，A成为leader，term为1，append了log1，超过半数认可，applied，之后A失联（此时还有新的command加入A，term均为1），B成为新的leader，term为2，啥事没干，又失联了，CDE都有机会成为新的leader，互相竞争，假设C成为leader，term为6，它也啥事没做，失联了，A这时恢复，A的任期小，一开始竞选没过，更新新的term后，其实在ADE中，都有机会成为leader，此时A成为新leader，term为8，他的log为log1，term：1，然后比较稳定了，有新的command进入，并且之前A失联的时候也有新的command加入（只不过可能不被认可），询问DE，它们的prevLogIndex和prevLogTerm都是匹配的，所以它就以A，任期为8，commit了log2，log3...任期均为1的日志，而且DE都可以，都可以applied的，此时A失联，C恢复，在它的term恢复到集群中的term时，它必然是leader，因为有：投票机制，prevLogTerm必须大，或者相等的情况下比较prevLogIndex长，而C的lastLogIndex是6，DE可能它的log长度长，但是它的lastLogTerm是1，所以必然是C竞选为leader，并且会把DE已经applied 的log给刷新掉，这就是bug所在。要避免这个问题，那么就是这个判断rf.logEntries[i].Term != rf.currentTerm，防止了leader commit比它当前任期要老的log。
   func (rf *Raft) checkLogEntries() {
   
   	for i := rf.commitIndex + 1; i <= len(rf.logEntries)-1 && rf.role == LEADER; i++ {
   		if rf.logEntries[i].Term != rf.currentTerm {
   			continue
   		}
   		majority := 0
   		for pindex := range rf.peers {
   			if rf.matchIndex[pindex] >= i {
   				majority++
   			}
   		}
   		if majority >= len(rf.peers)/2+1 {
   			rf.commitIndex = i
   			log.Errorf("%d rf.commitIndex:%d\n", rf.me, rf.commitIndex)
   		}
   	}
   ```

9. ```go
   // 在checkLogEntries function中，增加了&& len(rf.logEntries)-1 > rf.lastApplied判断
   for rf.commitIndex > rf.lastApplied && len(rf.logEntries)-1 > rf.lastApplied {
   		log.Errorf("%d:%v", rf.me, time.Now())
   		rf.lastApplied++
   		fmt.Printf("%d term[%d] applyid logentry[%d]:%v\n", rf.me, rf.currentTerm, rf.lastApplied, rf.logEntries[rf.lastApplied])
   		c := ApplyMsg{
   			CommandValid: true,
   			Command:      rf.logEntries[rf.lastApplied].Command,
   			CommandIndex: rf.lastApplied,
   		}
   		rf.applyCh <- c
   		// rf.persist()
   		// flag = true
   	}
   ```

10. ```go
    // 在RequestVote function中添加了更严格的投票限制
    // 因为debug发现，有同一个任期，竟然有不同的serverID曾任leader
    /*
    错误原因：crash的节点在重启后，由与拿来的leader role变为
    follower role，是平替的，即follower 的term和leder时的term时一样的，
    由别的node提醒，即append enter，term不够，requestVote term不够，收到心跳包时
    update term。那么，就会出现两个相同的term但command不同的log，由于判断是基于term，
    其实按照raft的代码，这两个不同command的log都是合法的，但是是不同log。
    或者是ABC三个server，A为leader，任期为8，刚竞选成功，还没来得及发送心跳包，挂掉了，BC仍然为7任期，follower，它们在选举timeout发生后，开始竞选，BC都是有可能成为leader的，假设B成为leader，那么任期为8，那么任期为8，历史上曾经是两个不同的server，这个应该是不合理的地方。
    但是论文上写的确实是rf.currentTerm > args.Term，不投票，参考被别人的代码，好像这个case也会出现的样子，暂时想不通，我自己增加了这个限制，测试了100此2A 2B 2C都是通过的
    */
    rf.mu.Lock()
    	defer rf.mu.Unlock()
    	fmt.Printf("!!!!call request vote! %d vote to %d!rf.term:%d args.term:%d\n", rf.me, args.CandidateId, rf.currentTerm, args.Term)
    	if rf.currentTerm >= args.Term {
    		log.Errorf("%d's current term is %d, remote %d term is %d\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
    		reply.Term = rf.currentTerm
    		reply.VoteGranted = false
    		return
    	} else if rf.currentTerm < args.Term {}
    ```

11. 









2D lab

要求：

- 每台server自己每隔一段时间（某种策略，TEST中已经帮你规定），进行snapshot已经commited的log
- 每次snapshot，新的替换老的，依据lastIncludedIndex来进行新老判断
- 每次snapshot，删除logEntry的值，把snapshot保存到persist中
- 因为要删除logEntry的值，所以，需要在Entry本身保存全局index的值，而不能只是使用array来标记它与原本的index
- InstallSnapshot，是follower向leader询问snapshot，leader会把自己的snapshot发给follower


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





















