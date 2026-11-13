# 可用分区的原子发布扩展

## 已有正确基线
warehouse.py已有bind、prepare、publish、receipt、positions、read。现有publish为全有或全无：任何分区token或位点过期都STALE_PLAN，否则在写事务内重新计算后发布。重算持有全局写锁，正确但不能保留部分有效分区的工作。本轮新增能力，不把此基线行为称为已存在的功能缺陷。
旧publish调用和回执结构均保持；新接口可共用内部实现。prepare产出的dict是opaque计划，调用者不修改；原计划持有完整已取段，可加内部字段，不要求与原内部键集合相同。

## 数据与原不变量
token=[partition,epoch,owner]；bind(token,0)注册或换主，epoch单调增加，source_floor固定为0，不涉及源端裁剪。分区先bind再prepare，所有权可在其后改变。
deliveries=[[token,after,[[pos,record],...]],...]，计划每分区至多一段，pos从after+1连续递增，允许空段。record=[id,rev,site,amount,deleted]，id/site是非空字符串，rev为正整数，amount为整数，deleted为布尔值。同ID同rev的内容一致。
read()仍为{positions,records,totals}；records按ID排序且保留墓碑，totals按site排序，行结构由原实现固定。以已发布各分区连续前缀中的全部记录为全集，每ID选择最高rev；未删除记录按site聚合笔数与金额，n=0才删桶，金额0且n>0须保留。同ID可跨分区，批内可多次修订、移站、删除、复活。换主本身不删除已入仓事实。

## 新公开入口
publish_available(plan, before_commit=None, lose_reply=False)返回并保存{request:请求号,accepted:{分区:新位点},skipped:{分区:原因}}。
在最终原子提交点，若token仍与authority的epoch/owner相等且当前pos等于段after，则该分区必须accepted；否则跳过，所有权不同用STALE_OWNER，仅位点不同用POSITION_MOVED，前者优先。未知分区视为STALE_OWNER。accepted仅推进该段长度，skipped不写其位点或载荷。全被跳过也保存空accepted回执；各列表/字典顺序不作为业务门槛。
不得重新拉取源记录，也不能发布包含被跳过分区贡献的旧records/totals。即使被跳过输入曾覆盖仍持有分区的较低修订，也须从原已取段和当前仓内状态恢复正确结果。新接口所有载荷、位点和回执在同一短事务原子提交；非连续段仍NONCONTIGUOUS且不能留下半提交。
只有本次有效段的业务计算和相交依赖才可触发重算。无共同ID且旧新site桶无交集的外部发布不能导致本计划逻辑重试；不能使用全库代次。可在写事务内短暂读取/比较局部依赖及安装已算出的值，禁止在写锁内执行最高修订归并、桶增减重算或完整原计划重新prepare。不能通过全表读取/重算来隐藏这部分工作。
before_commit是可选无参回调，在每次候选结果已经计算完成、尚未开始其写事务时调用。用于可重放交错：回调可从第二个Warehouse连接换主或发布一次数据；第二连接应能实际写入，不能被本次业务计算的全局写锁阻塞。回调之后必须重新验证所有权、位点及业务依赖；失效则在锁外自动重算，有限次干扰结束后应完成。不能固定仅重试一次，不能把已被换主的段错误接受。
request号在源计划创建时固定且全局唯一，不复用来提交其他原计划。已经存在该request回执时，不再调用before_commit或再次应用数据，直接返回原回执；lose_reply=True在新提交后抛PUBLISH_REPLY_LOST，后续重试仍返回原结果。receipt(request)可以读取新接口回执；旧接口仍保留原回执结构。

## 验证与交付
保留checks.py normal和原断言。新增verify/demo入口（可以扩充checks.py）接入.pointwise-runtime.json：verify真实核对旧兼容回归及新功能；demo展示部分换主和真实回执，不伪造已实现的行为。
用独立前缀判定核对：部分/全部换主、位点已推进、同ID跨分区、不同ID同桶、批内多修订、移站/删除复活和零和非空组。通过两个连接打开临时SQLite文件，用before_commit重放多次所有权/逻辑依赖变化和无交集写入；记录候选次数及实际结果，证明无全局代次误重试。验证响应丢失后回执幂等、整笔失败原子性；不新增源确认、断电恢复、远端服务或多机协议。
