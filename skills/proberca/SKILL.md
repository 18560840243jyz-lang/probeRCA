---
name: proberca
description: "Enforce the final frozen ProbeRCA-BPF two-plane online RCA scheme for service, host, and directed TCP-edge root causes: service-only As, healthy masked Av, TCP Burst penalty guidance, and one non-negative Sparse-Group FISTA solve. Use when implementing, reviewing, collecting, calibrating, replaying, or evaluating the formal ProbeRCA-BPF path."
---

# Final ProbeRCA-BPF Scheme

> This is the only active project scheme skill. The former P/B-stage skill is retired.
> Keep data and control planes separate. Do not inject faults, restore a composite relation-strength variable, or add counterfactual repeated solves.

下面给出最终定稿版 ProbeRCA-BPF 在线根因定位方案。这一版严格按照我们最后商议的结果组织，不再引入额外的综合关系强度变量，也暂不加入反事实重复求解。

一、方案总目标
系统需要回答三个问题：
1.
根因发生在哪个实体上？
2.
3.
属于哪一种根因指标大类？
4.
5.
有哪些直接观测证据支持该判断？
6.
统一根因候选表示为：
[
\boxed{c=(e,f)}
]
其中：

(c)：一个完整根因候选；


(e)：实体，可以是服务、主机或有向通信边；


(f)：该实体上的根因大类。

例如：
[
(\mathrm{payment},\mathrm{CPU})
]
表示 payment 服务发生 CPU 类故障；
[
(\mathrm{Node2},\mathrm{IO})
]
表示 Node2 主机发生 I/O 类故障；
[
(\mathrm{checkout}\rightarrow\mathrm{payment},\mathrm{TCP})
]
表示 checkout 到 payment 的有向通信边发生 TCP 类故障。
最终输出形式为：
根因实体：payment
根因大类：CPU

关键指标：
- cpu_usage_rate
- cpu_throttle_ratio

Burst直接证据：
- runqueue_wait_p95 异常
- wakeup_latency_p95 异常

二、整体流程
完整在线流程为：
1. 常态采集低成本聚合指标
        ↓
2. 建立实体身份映射与多源结构关系图
        ↓
3. Healthy阶段维护健康基线
        ↓
4. Healthy阶段在线学习服务级传播矩阵 As
        ↓
5. 服务或边出现Soft Alert
        ↓
6. 冻结健康模型，根据As强度构造候选实体子图
        ↓
7. 将实体子图展开为“实体名＋指标”节点图
        ↓
8. 根据实体关系和指标语义先验构造指标传播掩码
        ↓
9. 用事故前健康数据学习指标级传播矩阵 Av
        ↓
10. Hard Alert后开启候选范围内的Burst探针
        ↓
11. 只减去健康跨指标传播，得到联合残差
        ↓
12. 将Burst数据转化为直接证据强度
        ↓
13. 直接证据调整相应根因组的稀疏惩罚
        ↓
14. 使用Sparse-Group FISTA选择根因指标与根因大类
        ↓
15. 输出Top-K实体＋根因大类＋直接证据

三、步骤1：常态化采集低成本指标
3.1 这一步的作用
常态采集需要同时满足两个要求：

能够建立健康基线和传播关系；


不能持续传输大量细粒度内核事件。

因此常态阶段只采集按服务、主机和服务对聚合后的窗口级指标，不上传逐事件、逐线程、逐连接和逐数据包记录。
设每个统计窗口长度为：
[
\Delta t
]
例如：
[
\Delta t=1\text{秒}
]

3.2 每个服务采集9个常态指标
设服务为 (s)。
业务症状类：3个
1. 请求速率
[
\boxed{
x_{s,\mathrm{rate}}(t)
\frac{N_{s,\mathrm{request}}(t)}{\Delta t}
}
]
其中：

(N_{s,\mathrm{request}}(t))：服务 (s) 在窗口 (t) 内处理的请求数；


(\Delta t)：统计窗口长度；


(x_{s,\mathrm{rate}}(t))：服务请求速率。

作用：

表示当前服务负载；


参与服务关系学习；


作为指标传播中的负载上下文。

它不是根因大类。

2. 失败率
[
\boxed{
x_{s,\mathrm{failure}}(t)
\frac{
N_{s,\mathrm{error}}(t)
+
N_{s,\mathrm{timeout}}(t)
}{
N_{s,\mathrm{request}}(t)+\epsilon
}
}
]
其中：

(N_{s,\mathrm{error}}(t))：错误请求数；


(N_{s,\mathrm{timeout}}(t))：超时请求数；


(\epsilon>0)：防止分母为零的小常数；


(x_{s,\mathrm{failure}}(t))：服务失败率。

作用：

触发异常检测；


确定症状服务；


表示业务故障现象。

它不是服务根因类别。

3. 请求延迟P95
[
\boxed{
x_{s,\mathrm{latency}}(t)
Q_{0.95}
\left(
\mathcal L_s(t)
\right)
}
]
其中：

(\mathcal L_s(t))：窗口 (t) 中服务 (s) 的请求延迟样本集合；


(Q_{0.95})：95%分位数；


(x_{s,\mathrm{latency}}(t))：请求延迟P95。

作用：

发现性能下降；


触发告警；


作为资源故障传播到业务层的症状指标。


CPU类：2个
4. CPU使用率
[
\boxed{
x_{s,\mathrm{cpu}}(t)
\frac{
\Delta T_{s,\mathrm{cpu}}(t)
}{
\Delta t\cdot N_{s,\mathrm{cpu}}
}
}
]
其中：

(\Delta T_{s,\mathrm{cpu}}(t))：服务在当前窗口消耗的CPU时间；


(N_{s,\mathrm{cpu}})：分配给该服务的CPU额度或核心数；


(x_{s,\mathrm{cpu}}(t))：CPU使用率。


5. CPU限流比例
[
\boxed{
x_{s,\mathrm{throttle}}(t)
\frac{
\Delta nr_{\mathrm{throttled}}(t)
}{
\Delta nr_{\mathrm{periods}}(t)+\epsilon
}
}
]
其中：

(\Delta nr_{\mathrm{throttled}}(t))：窗口内被限流的调度周期数；


(\Delta nr_{\mathrm{periods}}(t))：总调度周期数；


(x_{s,\mathrm{throttle}}(t))：CPU限流比例。

CPU根因组为：
[
\boxed{
G_{(s,\mathrm{CPU})}
{
s::\mathrm{cpu_usage},
s::\mathrm{cpu_throttle}
}
}
]

Memory类：1个
6. 内存工作集比例
[
\boxed{
x_{s,\mathrm{memory}}(t)
\frac{
\mathrm{working_set}_s(t)
}{
\mathrm{memory_limit}_s+\epsilon
}
}
]
其中：

(\mathrm{working_set}_s(t))：当前服务工作集内存；


(\mathrm{memory_limit}_s)：容器内存限制；


(x_{s,\mathrm{memory}}(t))：内存使用相对限制的比例。

Memory根因组为：
[
\boxed{
G_{(s,\mathrm{Memory})}
{
s::\mathrm{memory_working_set_ratio}
}
}
]
OOM和OOM kill作为低频事件计数额外保留，但不算连续高频指标。

I/O类：1个
7. I/O PSI
[
\boxed{
x_{s,\mathrm{io}}(t)
\mathrm{io_psi_some}_s(t)
}
]
它表示窗口内至少有一个任务因为I/O资源不足而阻塞的时间比例。
I/O根因组为：
[
\boxed{
G_{(s,\mathrm{IO})}
{
s::\mathrm{io_psi}
}
}
]

Lock类：1个
8. futex等待时间比例
[
\boxed{
x_{s,\mathrm{lock}}(t)
\frac{
\sum_{e\in\mathcal F_s(t)}
d_e
}{
T_{s,\mathrm{thread}}(t)+\epsilon
}
}
]
其中：

(\mathcal F_s(t))：窗口内服务 (s) 的futex等待事件集合；


(d_e)：事件 (e) 的等待持续时间；


(T_{s,\mathrm{thread}}(t))：窗口内服务所有活跃线程可用的总线程时间；


(x_{s,\mathrm{lock}}(t))：线程时间中用于futex等待的比例。

常态阶段只在eBPF map中按cgroup聚合总等待时间，不持续上传每个futex事件。
Lock根因组为：
[
\boxed{
G_{(s,\mathrm{Lock})}
{
s::\mathrm{futex_wait_time_rate}
}
}
]

LocalNet类：1个
9. 本地socket异常率
[
\boxed{
x_{s,\mathrm{localnet}}(t)
\frac{
N_{\mathrm{backlog}}(t)
+
N_{\mathrm{accept_fail}}(t)
+
N_{\mathrm{local_rst}}(t)
+
N_{\mathrm{local_drop}}(t)
}{
N_{\mathrm{socket_op}}(t)+\epsilon
}
}
]
其中：

(N_{\mathrm{backlog}})：监听队列溢出次数；


(N_{\mathrm{accept_fail}})：本地accept失败次数；


(N_{\mathrm{local_rst}})：本地RST次数；


(N_{\mathrm{local_drop}})：本地socket丢弃次数；


(N_{\mathrm{socket_op}})：socket操作或连接暴露量；


(x_{s,\mathrm{localnet}}(t))：服务本地网络栈异常率。

LocalNet根因组为：
[
\boxed{
G_{(s,\mathrm{LocalNet})}
{
s::\mathrm{local_socket_failure_rate}
}
}
]
它表示服务本地socket栈问题，不表示某条服务间链路的问题。

3.3 每个主机采集4个常态指标
设主机为 (h)。
根因大类	常态指标
CPU	(x_{h,\mathrm{cpu}}=\mathrm{cpu_psi})
Memory	(x_{h,\mathrm{memory}}=\mathrm{memory_psi})
I/O	(x_{h,\mathrm{io}}=\mathrm{io_psi})
NIC	(x_{h,\mathrm{nic}}=\mathrm{nic_drop/error_rate})
对应根因组：
[
G_{(h,\mathrm{CPU})}={h::\mathrm{cpu_psi}}
]
[
G_{(h,\mathrm{Memory})}={h::\mathrm{memory_psi}}
]
[
G_{(h,\mathrm{IO})}={h::\mathrm{io_psi}}
]
[
G_{(h,\mathrm{NIC})}={h::\mathrm{nic_drop/error}}
]

3.4 每条正式有向TCP边采集3个常态指标
设有向边为：
[
g=(s_a\rightarrow s_b)
]
其中：

(s_a)：调用方；


(s_b)：被调用方。

采集：
[
x_{g,\mathrm{count}}(t)
]
表示请求数或查询数；
[
x_{g,\mathrm{latency}}(t)
]
表示边延迟P95；
[
x_{g,\mathrm{failure}}(t)
]
表示边失败或超时率。
其中请求数只是暴露量和上下文，不作为根因指标。正式有向边只包括：
[
g=(s_a\rightarrow s_b,\mathrm{TCP})
]
对应根因组为：
[
\boxed{
G_{(g,\mathrm{TCP})}
{
g::\mathrm{latency},
g::\mathrm{failure}
}
}
]

因此正式常态指标契约固定为服务9、主机4、每条有向TCP边3，即
`9/4/3`。DNS不属于正式常态契约或正式根因坐标。

四、步骤2：建立身份映射和多源结构关系图
4.1 这一步的作用
该步骤回答：
每条指标属于哪个服务、主机和有向边，以及哪些服务之间可能存在影响关系。
身份映射为：
PID/TID
→ cgroup
→ container
→ Pod
→ workload/owner
→ service
→ node
网络连接映射为：
5-tuple
→ source Pod / destination Pod
→ source service / destination service
→ directed service edge

4.2 构造服务级允许关系图
服务集合记为：
[
\mathcal S={s_1,\ldots,s_n}
]
其中：

(\mathcal S)：全部服务集合；


(n)：服务总数。

构造允许关系集合：
[
\boxed{
\mathcal R_s^{\mathrm{allow}}
\mathcal R_{\mathrm{trace}}
\cup
\mathcal R_{\mathrm{flow}}
\cup
\mathcal R_{\mathrm{cohost}}
\cup
\mathcal R_{\mathrm{shared}}
}
]
其中：

(\mathcal R_{\mathrm{trace}})：Trace观测到的业务调用关系；


(\mathcal R_{\mathrm{flow}})：eBPF Flow观测到的真实通信关系；


(\mathcal R_{\mathrm{cohost}})：两个服务部署在同一主机上的关系；


(\mathcal R_{\mathrm{shared}})：共享数据库、缓存、磁盘或其他资源的关系。

定义服务允许图：
[
\boxed{
\mathcal G_s^{\mathrm{allow}}
(\mathcal S,\mathcal R_s^{\mathrm{allow}})
}
]
这张图只说明：
哪些服务之间允许学习传播关系。
它不直接表示传播强度。

4.3 服务关系掩码
定义：
[
\boxed{
M_s[i,j]
\begin{cases}
1,&s_j\rightarrow s_i\in\mathcal R_s^{\mathrm{allow}},\
0,&\text{其他情况}.
\end{cases}
}
]
其中：

(M_s\in{0,1}^{n\times n})：服务传播结构掩码；


(M_s[i,j]=1)：允许学习服务 (s_j) 对服务 (s_i) 的影响；


(M_s[i,j]=0)：不允许该关系存在。

Trace和Flow通常是有向关系；同主机和共享资源关系可以按双向允许处理。

五、步骤3：建立健康基线并标准化指标
5.1 这一步的作用
不同指标量纲不同：

CPU使用率可能是0到1；


延迟可能是毫秒；


futex等待可能是时间比例；


错误率可能非常接近0。

因此必须统一转化为相对健康状态的异常分数。

5.2 健康缓存
对每个具体指标节点 (i)，维护健康样本集合：
[
B_i
]
其中：

(i)：一个具体的“实体＋指标”节点；


(B_i)：该指标在Healthy状态下的历史样本。

当前原始观测记为：
[
x_i(t)
]

5.3 鲁棒标准化
[
\boxed{
z_i(t)
\kappa_i
\frac{
T_i(x_i(t))
\operatorname{median}\left(T_i(B_i)\right)
}{
1.4826
\operatorname{MAD}\left(T_i(B_i)\right)
+\epsilon
}
}
]
其中：

(z_i(t))：指标 (i) 在窗口 (t) 的标准化异常；


(T_i(\cdot))：指标变换，例如重尾指标使用 (\log(1+x))；


(\operatorname{median})：健康样本中位数；


(\operatorname{MAD})：中位绝对偏差；


(1.4826)：将MAD校准到接近标准差尺度的常数；


(\epsilon>0)：防止健康波动为0时除零；


(\kappa_i\in{-1,+1})：异常方向。

当指标越大越异常时：
[
\kappa_i=+1
]
当指标越小越异常时：
[
\kappa_i=-1
]
例如可用内存越低越异常时，可以取 (-1)。

5.4 报警用异常强度
报警阶段使用：
[
\boxed{
a_i(t)=\max(z_i(t),0)
}
]
其中：

(a_i(t)\ge0)：只保留向故障方向的异常；


它用于报警和证据强度；


不用于后面的联合残差。

联合残差仍使用带符号的 (z_i(t))。

六、步骤4：构造服务粗粒度状态
6.1 这一步的作用
服务级传播矩阵 (A_s) 只用于：

在线学习服务间稳定影响；


判断服务关系强弱；


缩小候选范围。

它不定位CPU、Memory、Lock等具体根因。

6.2 服务状态
对服务 (s)，定义：
[
\boxed{
c_s(t)
\alpha_L a_{s,\mathrm{latency}}(t)
+
\alpha_F a_{s,\mathrm{failure}}(t)
}
]
其中：

(c_s(t))：服务 (s) 的粗粒度异常状态；


(a_{s,\mathrm{latency}}(t))：延迟异常强度；


(a_{s,\mathrm{failure}}(t))：失败率异常强度；


(\alpha_L\ge0)：延迟权重；


(\alpha_F\ge0)：失败率权重。

通常可约束：
[
\alpha_L+\alpha_F=1
]
组成服务状态向量：
[
\boxed{
\mathbf c_t
[c_{s_1}(t),\ldots,c_{s_n}(t)]^{\mathsf T}
\in\mathbb R^n
}
]

七、步骤5：Healthy阶段学习服务级传播矩阵 (A_s)
7.1 这一步的作用
学习：
一个服务过去发生变化后，对哪些其他服务的当前状态具有稳定预测影响。
服务级模型为：
[
\boxed{
\mathbf c_t
\sum_{\ell=1}^{L_s}
A_s^{(\ell)}
\mathbf c_{t-\ell}
+
\boldsymbol\varepsilon_t^s
}
]
其中：

(L_s)：服务级最大滞后窗口数；


(\ell)：滞后序号，(\ell=1,\ldots,L_s)；


(A_s^{(\ell)}\in\mathbb R^{n\times n})：第 (\ell) 阶服务传播矩阵；


(A_s^{(\ell)}[i,j])：服务 (s_j) 在 (t-\ell) 时刻的状态，对服务 (s_i) 在 (t) 时刻状态的影响；


(\boldsymbol\varepsilon_t^s)：无法解释的服务级误差。

必须满足：
[
\boxed{
A_s^{(\ell)}[i,j]=0
\quad\text{若}\quad
M_s[i,j]=0
}
]
因此 (A_s) 只在Trace、Flow、同主机或共享资源允许的位置学习。

7.2 RLS在线更新
对目标服务 (s_i)，将它所有允许父服务的历史状态排列成特征向量：
[
\boldsymbol\phi_i(t)
]
例如：
[
\boldsymbol\phi_i(t)
[
c_{j_1}(t-1),
c_{j_2}(t-1),
\ldots,
c_{j_m}(t-L_s)
]^{\mathsf T}
]
其中 (j_1,\ldots,j_m) 是结构掩码允许影响服务 (i) 的服务。
对应参数向量：
[
\boldsymbol\beta_i(t)
]
它包含 (A_s^{(1)},\ldots,A_s^{(L_s)}) 中与服务 (i) 有关的系数。
预测值：
[
\boxed{
\widehat c_i(t)
\boldsymbol\phi_i(t)^{\mathsf T}
\boldsymbol\beta_i(t-1)
}
]
预测误差：
[
\boxed{
e_i(t)
c_i(t)-\widehat c_i(t)
}
]
RLS增益：
[
\boxed{
K_i(t)
\frac{
P_i(t-1)\boldsymbol\phi_i(t)
}{
\lambda_f
+
\boldsymbol\phi_i(t)^{\mathsf T}
P_i(t-1)
\boldsymbol\phi_i(t)
}
}
]
其中：

(P_i(t))：参数不确定性矩阵；


(\lambda_f\in(0,1])：遗忘因子；


(\lambda_f) 越接近1，历史数据保留越久；


(K_i(t))：当前样本对参数更新的增益。

参数更新：
[
\boxed{
\boldsymbol\beta_i(t)
\boldsymbol\beta_i(t-1)
+
K_i(t)e_i(t)
}
]
不确定性更新：
[
\boxed{
P_i(t)
\frac{1}{\lambda_f}
\left[
P_i(t-1)
K_i(t)
\boldsymbol\phi_i(t)^{\mathsf T}
P_i(t-1)
\right]
}
]
这些更新只在Healthy阶段执行。

八、步骤6：状态机
8.1 Healthy
作用：

更新健康基线；


更新 (A_s)；


更新Trace、Flow、同主机和共享资源图；


保存最近健康窗口；


低占空比采样Burst连续指标，建立Burst健康画像。


8.2 Soft Alert
服务或有向TCP边异常分数达到软阈值：
[
\boxed{
\mathrm{score}_e(t)\ge\tau_{\mathrm{soft}}=3
}
]
同一实体连续3个1秒窗口满足条件后进入Soft Alert。不同实体的异常不能拼接
成连续告警；TCP边拥有独立于端点服务的告警状态。
其中：

(\tau_{\mathrm{soft}})：软告警阈值。

Soft阶段执行：

冻结健康基线；


冻结 (A_s)；


固定当前拓扑版本；


构造候选实体子图；


使用Soft之前的健康缓存学习指标级 (A_v)。


8.3 Hard Alert
服务或有向TCP边满足：
[
\boxed{
\mathrm{score}_e(t)\ge\tau_{\mathrm{hard}}=5
}
]
并且同一实体连续2个1秒窗口达到阈值后进入Hard Alert。
其中：
[
\tau_{\mathrm{hard}}>\tau_{\mathrm{soft}}
]
Hard阶段执行：

冻结 (A_v)；


开启候选范围内Burst探针；


构造联合残差；


运行FISTA；


输出根因。


8.4 Recovery
系统恢复后进入冷却阶段。
冷却结束前：

不更新健康基线；


不更新 (A_s)；


避免把故障恢复过程错误写入健康模型。


九、步骤7：根据 (A_s) 构造候选服务子图
9.1 这一步的作用
从全系统中筛出与当前告警最可能相关的一小部分服务。
这里不再引入新的综合关系强度变量。
Trace、Flow、同主机和共享资源只决定：
[
\text{关系是否允许存在}
]
(A_s) 直接决定：
[
\text{关系实际有多强}
]

9.2 告警种子
服务告警种子集合记为：
[
Y_s^{\mathrm{service}}
]
边异常种子集合记为：
[
Y_e
]
如果某条边直接异常，将它的两个端点加入服务种子：
[
\boxed{
Y_s
Y_s^{\mathrm{service}}
\cup
\operatorname{Endpoints}(Y_e)
}
]
其中：

(Y_s)：最终服务种子集合；


(\operatorname{Endpoints}(Y_e))：异常边的调用方和被调用方服务。


9.3 高召回结构扩展
在允许关系图上，从种子扩展至最多 (K) 跳：
[
\boxed{
S_{\mathrm{raw}}
\operatorname{Expand}_{K}
\left(
Y_s;
\mathcal G_s^{\mathrm{allow}}
\right)
}
]
其中：

(K)：候选扩展最大跳数；


(S_{\mathrm{raw}})：尚未剪枝的原始候选服务集合。

这一阶段优先保证不漏掉潜在根因。

9.4 使用 (A_s) 计算关系强度
对允许关系 (s_j\rightarrow s_i)，将多个滞后的系数合并：
[
\boxed{
w_{ij}^{s}
\left(
\sum_{\ell=1}^{L_s}
\left|
\widehat A_s^{(\ell)}[i,j]
\right|^2
\right)^{1/2}
}
]
其中：

(w_{ij}^{s})：服务 (s_j\rightarrow s_i) 的总体传播强度；


(\widehat A_s^{(\ell)})：Soft Alert时冻结的服务传播矩阵；


绝对值表示这里只判断关系大小，不判断正负方向；


平方和开根号用于合并不同滞后上的影响。


9.5 删除弱关系
设服务关系阈值为：
[
\tau_s
]
保留：
[
\boxed{
\mathcal R_c^s
\left{
s_j\rightarrow s_i:
s_i,s_j\in S_{\mathrm{raw}},
\quad
w_{ij}^{s}\ge\tau_s
\right}
}
]
其中：

(\mathcal R_c^s)：最终保留的强服务关系；


(w_{ij}^{s}<\tau_s) 的关系被删除。

最终候选服务集合：
[
\boxed{
S_c
Y_s
\cup
\operatorname{Endpoints}
\left(
\mathcal R_c^s
\right)
}
]
告警种子始终保留，即使它没有达到阈值的邻接关系。

十、步骤8：挂接主机和有向边实体
10.1 这一步的作用
服务子图确定后，将与这些服务直接相关的主机和通信边加入候选范围。
候选主机集合：
[
\boxed{
H_c
{
h:
\exists s\in S_c,;
s\text{部署在}h
}
}
]
候选有向边集合：
[
\boxed{
E_c
{
g=(s_a\rightarrow s_b):
s_a,s_b\in S_c
}
\cup Y_e
}
]
其中：

(H_c)：候选服务所在主机集合；


(E_c)：候选服务之间的真实有向边及直接异常边。

最终候选实体集合：
[
\boxed{
\mathcal E_c
S_c\cup H_c\cup E_c
}
]

十一、步骤9：展开为“实体名＋指标”节点
11.1 这一步的作用
服务级 (A_s) 只能判断服务关系，不能区分：

CPU；


Memory；


I/O；


Lock；


LocalNet；


TCP；

因此需要将候选实体展开到指标粒度。

11.2 指标节点集合
对实体 (e)，其常态指标集合记为：
[
\mathcal M(e)
]
候选指标节点集合：
[
\boxed{
\mathcal V_c
{
(e,m):
e\in\mathcal E_c,;
m\in\mathcal M(e)
}
}
]
例如：
payment::request_latency
payment::cpu_usage
payment::cpu_throttle
payment::futex_wait_time_rate
payment::local_socket_failure_rate
Node2::cpu_psi
checkout→payment::edge_latency
checkout→payment::edge_failure
设指标节点总数为：
[
d=|\mathcal V_c|
]
统一指标异常向量：
[
\boxed{
\mathbf z_t
[z_i(t)]_{i\in\mathcal V_c}
\in\mathbb R^d
}
]

十二、步骤10：构造指标传播先验掩码
12.1 这一步的作用
指标之间不能全连接学习。
例如不能允许：
[
\text{无关服务的Memory}
\rightarrow
\text{另一条无关TCP边的延迟}
]
因此指标级传播必须同时满足：

实体关系合理；


指标语义合理。


12.2 实体关系先验
设两个指标节点为：
[
p=(e_p,m_p)
]
[
q=(e_q,m_q)
]
定义实体关系掩码：
[
\boxed{
M_{\mathrm{entity}}[p,q]
\begin{cases}
1,&e_p=e_q,\
1,&e_q\rightarrow e_p\text{存在于候选实体图},\
0,&\text{其他情况}.
\end{cases}
}
]
允许的实体关系包括：

同一服务内部；


强服务传播关系；


服务与所在主机；


服务与其入边或出边；


主机与该主机上服务涉及的边；


同主机或共享资源关系。


12.3 指标语义先验
定义：
[
\boxed{
M_{\mathrm{semantic}}[p,q]
\begin{cases}
1,&m_q\rightarrow m_p\text{符合故障机制先验},\
0,&\text{其他情况}.
\end{cases}
}
]
典型允许关系包括：
[
\text{request rate}
\rightarrow
\text{CPU usage}
]
[
\text{CPU throttle}
\rightarrow
\text{request latency}
]
[
\text{memory pressure}
\rightarrow
\text{request latency/failure}
]
[
\text{I/O PSI}
\rightarrow
\text{request latency}
]
[
\text{futex wait}
\rightarrow
\text{request latency}
]
[
\text{local socket failure}
\rightarrow
\text{service failure rate}
]
[
\text{host CPU PSI}
\rightarrow
\text{同机服务CPU/latency}
]
[
\text{host NIC异常}
\rightarrow
\text{相关边latency/failure}
]
[
\text{edge latency/failure}
\rightarrow
\text{调用方latency/failure}
]

12.4 最终指标掩码
[
\boxed{
M_v[p,q]
M_{\mathrm{entity}}[p,q]
\cdot
M_{\mathrm{semantic}}[p,q]
}
]
其中：

(M_v[p,q]=1)：允许指标 (q) 影响指标 (p)；


(M_v[p,q]=0)：该传播关系强制不存在。


十三、步骤11：用健康数据学习指标级传播矩阵 (A_v)
13.1 这一步的作用
学习候选范围内：
在健康状态下，一个指标的历史变化通常怎样传播到另一个指标。
只使用Soft Alert之前的健康窗口。
健康窗口集合记为：
[
\mathcal H
]

13.2 指标传播模型
[
\boxed{
\mathbf z_\tau
\sum_{\ell=1}^{L_v}
A_v^{(\ell)}
\mathbf z_{\tau-\ell}
+
\boldsymbol\varepsilon_\tau^v
}
]
其中：

(\tau\in\mathcal H)：健康时间窗口；


(L_v)：指标级最大滞后数；


(A_v^{(\ell)}\in\mathbb R^{d\times d})：第 (\ell) 阶指标传播矩阵；


(A_v^{(\ell)}[p,q])：指标 (q) 的历史值对指标 (p) 当前值的健康影响；


(\boldsymbol\varepsilon_\tau^v)：指标级建模误差。


13.3 带掩码Ridge求解
[
\boxed{
\begin{aligned}
\min_{{A_v^{(\ell)}}}
\quad&
\sum_{\tau\in\mathcal H}
\left|
\mathbf z_\tau
\sum_{\ell=1}^{L_v}
A_v^{(\ell)}
\mathbf z_{\tau-\ell}
\right|2^2
\
&+
\rho_A
\sum{\ell=1}^{L_v}
\left|
A_v^{(\ell)}
\right|_F^2
\end{aligned}
}
]
约束：
[
\boxed{
A_v^{(\ell)}[p,q]=0
\quad\text{若}\quad
M_v[p,q]=0
}
]
其中：

(\rho_A\ge0)：Ridge正则强度；


(|\cdot|_2)：向量二范数；


(|\cdot|_F)：矩阵Frobenius范数。

Ridge项用于防止候选指标数量较多时出现过拟合和系数不稳定。

十四、步骤12：只保留跨指标传播
14.1 这一步的作用
我们不希望指标自身的持续异常被自身历史解释掉。
因此虽然 (A_v) 可以学习到指标自身的自相关系数，但构造残差前要清除对角线。
[
\boxed{
A_{v,\times}^{(\ell)}
A_v^{(\ell)}
\operatorname{Diag}
\left(
\operatorname{diag}
\left(A_v^{(\ell)}\right)
\right)
}
]
其中：

(\operatorname{diag}(A))：提取矩阵 (A) 的对角元素；


(\operatorname{Diag}(\cdot))：将向量重新构造成对角矩阵；


(A_{v,\times}^{(\ell)})：去除自身历史后的跨指标传播矩阵。

因此：
[
A_{v,\times}^{(\ell)}[i,i]=0
]
不会减掉：
[
z_i(t-\ell)\rightarrow z_i(t)
]
只减掉：
[
z_j(t-\ell)\rightarrow z_i(t),
\quad i\neq j
]

十五、步骤13：构造联合残差
15.1 这一步的作用
计算：
当前异常中，有多少不能被健康跨指标传播解释。
[
\boxed{
\widetilde{\mathbf r}_t
\mathbf z_t
\sum_{\ell=1}^{L_v}
\widehat A_{v,\times}^{(\ell)}
\mathbf z_{t-\ell}
}
]
其中：

(\widetilde{\mathbf r}_t\in\mathbb R^d)：联合残差；


(\mathbf z_t)：当前指标异常向量；


(\widehat A_{v,\times}^{(\ell)})：事故前学习并冻结的健康跨指标传播矩阵；


(\mathbf z_{t-\ell})：过去指标异常向量。

联合残差保留正负号，不执行：
[
\max(\widetilde{\mathbf r}_t,0)
]
正残差表示当前异常高于健康传播预测；
负残差表示健康模型预测值高于实际观测。

十六、步骤14：确定可作为根因的残差坐标
16.1 这一步的作用
不是所有指标都能成为根因。
例如：

请求延迟；


服务失败率；


请求速率；

主要是症状或上下文，不作为服务根因变量。
定义可作为根因的指标集合：
[
\boxed{
\mathcal V_r\subset\mathcal V_c
}
]
它包括：

服务CPU、Memory、I/O、Lock、LocalNet指标；


主机CPU、Memory、I/O、NIC指标；


有向TCP边的latency和failure指标。

设：
[
p=|\mathcal V_r|
]
定义选择矩阵：
[
\boxed{
P_r\in{0,1}^{d\times p}
}
]
其中每一列从联合残差中选择一个可作为根因的坐标。
根因残差子向量：
[
\boxed{
\mathbf r_t
P_r^{\mathsf T}
\widetilde{\mathbf r}_t
\in\mathbb R^p
}
]
这样FISTA只处理真正可作为根因的残差坐标。
它等价于在原完整公式中令：
[
D=P_r
]
但直接提取 (\mathbf r_t) 更清楚，也避免症状坐标成为与优化变量无关的常数项。

十七、步骤15：开启Burst探针
17.1 这一步的作用
常态指标负责低成本筛选，Burst负责提供更接近故障机制的直接证据。
Burst只在候选实体范围内开启，例如持续：
[
T_B=30\text{秒}
]
其中 (T_B) 是Burst采样持续时间。

17.2 各根因类别的Burst数据
根因类别	Burst采集数据
服务CPU	runqueue wait p95、wakeup latency p95
服务Memory	major page fault rate、direct reclaim stall、OOM victim
服务I/O	block latency p95、block queue wait p95、device/volume ID
服务Lock	futex wait count、futex wait p95
服务LocalNet	socket queue wait p95、backlog overflow、accept/connect failure
主机CPU	host runqueue wait、调度等待
主机Memory	direct reclaim、OOM victim
主机I/O	分设备block latency与queue wait
主机NIC	NIC队列丢包、错误与softirq延迟
TCP边	retrans rate、RTO、RTT p95、connect failure、RST

十八、步骤16：判断Burst数据是否异常
Burst数据分为两类。

18.1 自然稀有事件
例如：

OOM kill；


RTO；


connect failure。

事件率：
[
\boxed{
v_q(t)
\frac{
N_q(t)
}{
E_q(t)+\epsilon
}
}
]
其中：

(q)：一条Burst证据；


(N_q(t))：当前Burst窗口中事件次数；


(E_q(t))：暴露量，例如请求数、连接数或I/O次数；


(v_q(t))：归一化事件率。

证据强度：
[
\boxed{
a_q^B(t)
\operatorname{clip}
\left(
\frac{v_q(t)}{\tau_q},
0,1
\right)
}
]
其中：

(\tau_q)：该证据达到强异常时的事件率阈值；


(\operatorname{clip}(x,0,1))：将数值限制到0到1；


(a_q^B(t)\in[0,1])：Burst异常强度。


18.2 连续型Burst指标
例如：

runqueue wait；


block latency；


futex wait；


RTT。

Healthy阶段通过低占空比短时采样建立健康参考集合：
[
B_q^B
]
当前Burst值记为：
[
x_q^B(t)
]
异常分数：
[
\boxed{
z_q^B(t)
\frac{
T_q(x_q^B(t))
\operatorname{median}
\left(
T_q(B_q^B)
\right)
}{
1.4826
\operatorname{MAD}
\left(
T_q(B_q^B)
\right)
+\epsilon
}
}
]
转化为证据强度：
[
\boxed{
a_q^B(t)
\operatorname{clip}
\left(
\frac{
\max(z_q^B(t),0)
}{
z_{\mathrm{cap}}
},
0,1
\right)
}
]
其中：

(z_{\mathrm{cap}}>0)：达到最大证据强度的异常分数；


(a_q^B(t)=0)：没有向故障方向偏离；


(a_q^B(t)=1)：证据非常强。

没有健康参考、自然零基线或可靠阈值时：
[
a_q^B(t)=0
]
即不使用该证据降低惩罚。

十九、步骤17：计算Burst直接证据强度
19.1 数据质量
定义：
[
w_q(t)\in[0,1]
]
表示证据 (q) 的数据质量。
它考虑：

eBPF ring buffer是否丢事件；


Burst窗口是否完整；


cgroup到服务的映射是否成功；


网络连接是否正确映射为服务对。

有效证据：
[
\boxed{
\psi_q(t)
a_q^B(t)w_q(t)
}
]

19.2 候选级证据合并
对根因候选：
[
c=(e,f)
]
其Burst证据集合记为：
[
\mathcal Q_c
]
例如：
[
\mathcal Q_{(\mathrm{payment},\mathrm{CPU})}
{
\mathrm{runqueue\ wait},
\mathrm{wakeup\ latency}
}
]
多条证据采用noisy-OR合并：
[
\boxed{
H_c(t)
1-
\prod_{q\in\mathcal Q_c}
\left(
1-\psi_q(t)
\right)
}
]
其中：

(H_c(t)\in[0,1])：候选 (c) 的综合直接证据；


任意一条强证据都能提高 (H_c(t))；


多条证据共同存在时，(H_c(t)) 会进一步提高。


二十、步骤18：Burst证据调整稀疏惩罚
20.1 这一步的作用
Burst证据不直接从残差中减去，也不直接替代FISTA结果。
它只用于告诉优化器：
具有强直接证据的根因组，不应被轻易压成零。
基础组惩罚：
[
\lambda_c^{\mathrm{grp},0}
]
有效组惩罚：
[
\boxed{
\lambda_c^{\mathrm{grp,eff}}(t)
\frac{
\lambda_c^{\mathrm{grp},0}
}{
1+\eta H_c(t)
}
}
]
其中：

(\eta\ge0)：Burst证据作用强度；


(H_c(t))：综合直接证据；


(\lambda_c^{\mathrm{grp,eff}}(t))：FISTA实际使用的组惩罚。

当：
[
H_c(t)\uparrow
]
则：
[
\lambda_c^{\mathrm{grp,eff}}(t)\downarrow
]
该根因组更容易保留。

二十一、步骤19：定义统一根因变量
21.1 这一步的作用
所有服务、主机和边根因统一放在同一个变量中，不拆成三套优化问题。
[
\boxed{
\boldsymbol\theta_t
[\theta_i(t)]{i\in\mathcal V_r}
\in\mathbb R+^p
}
]
其中：

(\theta_i(t))：根因指标 (i) 的自身故障贡献；


(p)：可选根因指标数量；


(\mathbb R_+^p)：所有元素非负的 (p) 维空间。

例如：
θpayment.cpu_usage
θpayment.cpu_throttle
θpayment.memory
θpayment.lock
θNode2.cpu_psi
θcheckout→payment.edge_latency
都属于同一个 (\boldsymbol\theta_t)。

二十二、步骤20：Sparse-Group FISTA目标函数
22.1 根因残差质量矩阵
定义：
[
W_t^r
\in\mathbb R^{p\times p}
]
为对角矩阵：
[
\boxed{
W_t^r
\operatorname{Diag}
(w_1^r(t),\ldots,w_p^r(t))
}
]
其中：

(w_i^r(t)\in[0,1])：根因指标 (i) 的观测质量；


采集完整、身份映射可靠时权重大；


缺失或数据质量差时权重小。

加权范数：
[
\boxed{
|\mathbf x|_{W_t^r}^2
\mathbf x^{\mathsf T}
W_t^r
\mathbf x
}
]

22.2 最终目标函数
[
\boxed{
\begin{aligned}
\min_{\boldsymbol\theta_t\ge0}\quad
&
\frac12
\left|
\mathbf r_t-\boldsymbol\theta_t
\right|{W_t^r}^{2}
\
&+
\sum{i=1}^{p}
\lambda_i\theta_i(t)
\
&+
\sum_{c\in\mathcal C}
\lambda_c^{\mathrm{grp,eff}}(t)
\left|
\boldsymbol\theta_{t,G_c}
\right|_2
\end{aligned}
}
]
其中：

(\mathbf r_t)：可作为根因的联合残差子向量；


(\boldsymbol\theta_t)：各根因指标的自身故障贡献；


(\lambda_i\ge0)：具体指标稀疏惩罚；


(\mathcal C)：所有“实体＋根因大类”候选集合；


(G_c)：候选 (c) 包含的具体指标索引集合；


(\boldsymbol\theta_{t,G_c})：根因变量中属于候选 (c) 的子向量；


(|\cdot|_2)：组内二范数。

第一项要求：
[
\boldsymbol\theta_t
]
尽量解释根因残差。
第二项要求只保留少数具体指标。
第三项要求只保留少数实体＋根因大类。

二十三、步骤21：FISTA求解
定义光滑项：
[
\boxed{
f(\boldsymbol\theta)
\frac12
|
\mathbf r_t-\boldsymbol\theta
|_{W_t^r}^{2}
}
]
梯度：
[
\boxed{
\nabla f(\boldsymbol\theta)
W_t^r
(\boldsymbol\theta-\mathbf r_t)
}
]
因为 (W_t^r) 是对角矩阵，梯度Lipschitz常数为：
[
\boxed{
L
\max_i W_t^r[i,i]
}
]
步长选择：
[
\boxed{
0<\alpha\le\frac1L
}
]

23.1 梯度步骤
[
\boxed{
\mathbf v^{(k)}
\mathbf y^{(k)}
\alpha
W_t^r
\left(
\mathbf y^{(k)}-\mathbf r_t
\right)
}
]
其中：

(k)：FISTA迭代轮数；


(\mathbf y^{(k)})：带Nesterov加速的当前点；


(\mathbf v^{(k)})：梯度更新后的临时变量；


(\alpha)：步长。


23.2 非负逐指标软阈值
[
\boxed{
\bar v_i^{(k)}
\max
\left(
v_i^{(k)}-\alpha\lambda_i,
0
\right)
}
]
它同时实现：

L1稀疏；


非负约束。


23.3 根因组收缩
对于候选组 (G_c)：
[
\boxed{
\boldsymbol\theta_{G_c}^{(k+1)}
\left(
1-
\frac{
\alpha\lambda_c^{\mathrm{grp,eff}}(t)
}{
|\bar{\mathbf v}{G_c}^{(k)}|2+\epsilon
}
\right)+
\bar{\mathbf v}{G_c}^{(k)}
}
]
其中：
[
(x)_+=\max(x,0)
]
如果一个组整体证据不足，该组会全部被压成零。

23.4 Nesterov加速
初始化：
[
q_0=1
]
更新：
[
\boxed{
q_{k+1}
\frac{
1+\sqrt{1+4q_k^2}
}{2}
}
]
加速点：
[
\boxed{
\mathbf y^{(k+1)}
\boldsymbol\theta^{(k+1)}
+
\frac{
q_k-1
}{
q_{k+1}
}
\left(
\boldsymbol\theta^{(k+1)}
\boldsymbol\theta^{(k)}
\right)
}
]
当目标函数变化或变量变化小于收敛阈值时停止。

二十四、步骤22：根因排序和结果输出
FISTA最终得到：
[
\widehat{\boldsymbol\theta}_t
]
候选根因大类分数：
[
\boxed{
S_c(t)
\left|
\widehat{\boldsymbol\theta}_{t,G_c}
\right|_2
}
]
其中：

(S_c(t))：候选 (c) 的根因得分；


分数越大，说明该实体＋根因类别的自身异常贡献越强。

按照：
[
S_c(t)
]
从大到小输出Top-K。
当前版本不执行反事实删除和重复FISTA求解。

二十五、完整例子
假设 payment 服务触发延迟告警。
服务级 (A_s) 剪枝后，候选范围包括：
checkout
payment
Node2
checkout → payment
指标级传播扣除后，根因残差为：
[
\mathbf r_t=
\begin{bmatrix}
3.5\
5.0\
0.3\
0.2\
0.1
\end{bmatrix}
]
分别对应：
payment.cpu_usage
payment.cpu_throttle
payment.memory
payment.io
payment.lock
Burst发现：
[
z_{\mathrm{runqueue}}^B=4.8
]
[
z_{\mathrm{wakeup}}^B=4.1
]
因此：
[
H_{(\mathrm{payment},\mathrm{CPU})}
]
较高，CPU组惩罚下降。
FISTA得到：
[
\widehat\theta_{\mathrm{payment.cpu_usage}}=3.1
]
[
\widehat\theta_{\mathrm{payment.cpu_throttle}}=4.6
]
CPU组得分：
[
\boxed{
S_{(\mathrm{payment},\mathrm{CPU})}
\sqrt{3.1^2+4.6^2}
}
]
最终输出：
根因实体：payment
根因大类：CPU

关键常态指标：
- cpu_usage residual = 3.5
- cpu_throttle residual = 5.0

Burst直接证据：
- runqueue_wait_p95 显著异常
- wakeup_latency_p95 显著异常

二十六、最终固定的三个模型层次
服务级 (A_s)
节点只有服务。
服务调用边、Flow关系、同主机和共享资源关系只作为：

结构掩码；


服务传播系数位置。

有向边本身不作为 (A_s) 的独立节点。

指标级 (A_v)
节点包括：

服务指标；


主机指标；


有向边指标。

它负责学习健康跨指标传播。

Sparse-Group FISTA
它不重新学习传播。
它负责：
[
\boxed{
\text{从健康跨指标传播无法解释的根因残差中，
选择少量具体指标和少量实体＋根因大类。}
}
]
整个最终方案可以概括为：
[
\boxed{
\text{多源结构约束}
\rightarrow
A_s\text{候选筛选}
\rightarrow
A_v\text{健康传播扣除}
\rightarrow
\text{Burst证据引导}
\rightarrow
\text{Sparse-Group FISTA根因选择}
}
]

二十七、校准与Readiness硬门禁

以下规则是最终方案的强制组成部分，不能通过降低Soft/Hard阈值绕过。

有效观测：

- `coverage = 0`表示缺失，不能补零、前向填充、插值或复用上一窗口值。
- 缺失指标不能进入Healthy基线、告警或(A_v)训练。
- latency P95必须满足最小样本数。
- failure rate必须拥有足够的请求暴露量；无请求窗口是缺失，不是健康零值。
- 数据面保留raw value、coverage、sample count、request count来源、quality和lineage；控制面判断是否可用。

稳健尺度：

[
s_i^{MAD}=1.4826\operatorname{MAD}(T_i(B_i))
]

MAD正常时使用MAD尺度。MAD过小或为零时：

[
\boxed{
s_i=
\max\left(
\frac{\operatorname{IQR}(T_i(B_i))}{1.349},
s_{\min,f(i)}
\right)
}
]

- `epsilon`只允许作为浮点数值保护，不能承担统计尺度作用。
- latency、ratio、count、PSI分别使用独立Healthy Pilot冻结的指标族尺度下限。
- 必须保存`scale_source = mad / iqr / family_floor`及完整尺度数值。
- 未冻结指标族尺度下限时，系统保持CALIBRATING。

逐目标(A_v)：

- Masked Ridge必须按目标坐标分别拟合。
- 目标(i)只使用“目标值及其允许父指标滞后值均有效”的训练行。
- 默认最低训练行数为：

[
N_i^{min}=
\max\left(4,\left\lceil 2p_i\right\rceil\right)
]

- 每个目标必须输出allowed feature count、valid training rows、minimum rows、effective rank、condition number、ready和not-ready reason。
- 一条稀疏边不能拖垮无关目标，也不能以未Ready坐标进入FISTA。
- 正式计划故障范围内的根因坐标必须通过`calibration_required_root_coordinates`显式冻结并全部Ready。
- `calibration_required_root_coordinates`只允许服务、主机和有向TCP边坐标；DNS坐标必须排除且不计入Baseline或(A_v) Readiness分母。
- 上述计划范围只能用于校准门禁，绝对不能进入候选排序、残差或FISTA。
- 计划范围必须在实验前统一声明并与单次故障注入标签隔离，不能按某次真实注入结果动态改变。

正式状态机：

```text
STARTING
  -> CALIBRATING
  -> READY
  -> Healthy / Soft / Hard / Recovery
```

CALIBRATING阶段：

- 不触发Soft；
- 不触发Hard；
- 不运行FISTA；
- 故障注入入口必须拒绝启动。

进入READY必须同时满足：

1. 必需指标拥有足够有效Healthy样本；
2. 所有尺度有效且指标族尺度下限已冻结；
3. (A_s) Ready；
4. 计划故障范围内的(A_v)根因坐标全部Ready；
5. 拓扑和身份映射完整；
6. 连续健康验证窗口没有伪Soft或Hard。

若Hard后候选模型意外不Ready，必须输出：

```text
RCA_NOT_READY
reason: <逐坐标真实原因>
```

此时不得静默跳过(A_v)，不得运行FISTA，也不得输出伪根因。

二十八、DNS实验能力边界

DNS事务匹配、容器归属、qname分类、重试、超时和TCP fallback等代码仅作为
`experimental / optional` 工程历史保留，默认关闭，也不在当前论文中评价。
只有显式实验配置才允许运行相关采集或诊断模式。

正式ProbeRCA-BPF路径必须满足：

- DNS不是正式根因类别或正式有向边实体；
- DNS不进入`required_candidate_scope`、Baseline或(A_v) Readiness分母；
- DNS不触发正式Soft/Hard Alert；
- DNS不生成根因残差坐标、θ变量或Sparse-Group FISTA候选组；
- DNS Burst不调整正式候选组惩罚；
- DNS不进入正式故障矩阵、主实验、消融或论文评价；
- experimental DNS开关不改变正式`9/4/3`契约、正式Dataset ID或Readiness分母。

旧v2/v3 DNS归档仍允许兼容读取和Replay，但其中DNS常态坐标与Burst证据必须
统一标记为`excluded_from_formal_rca`，不得进入基线、告警、传播学习、残差
或排序。控制面结果必须保留旧归档原始契约指纹，不能把旧归档伪装为新v4
正式归档。

正式有向通信边只保留：

```text
(src_service -> dst_service, TCP)
```

其完整路径必须保持为：

```text
TCP边独立告警
  -> 候选范围
  -> 健康A_v跨指标传播扣除
  -> TCP边根因残差
  -> TCP Burst调整组惩罚
  -> 非负Sparse-Group FISTA
  -> (src_service -> dst_service, TCP)
```
