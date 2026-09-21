# UC 网盘多线程下载器

把 UC 网页端"按连接限速"的下载变成 **64 线程并行分块下载**。

实测同一个 1.58 GB 文件：单连接 **0.5 MB/s（约 56 分钟）** → 本工具 **27.8 MB/s（58 秒）**，提速约 55 倍。

| 连接数 | 4 | 8 | 16 | 32 | 64 | 96 |
|---|---|---|---|---|---|---|
| 速度 (MB/s) | 1.9 | 3.8 | 7.5 | 15.0 | **27.8** | 25.8 |

64 是甜点值（96 反而略降，因为已经吃满带宽）。

---

## 为什么需要 Cookie

UC 的下载直链是阿里云 OSS 的签名直链，但额外带了 `callback` / `callback-var` 参数：
OSS 在返回数据前，会先拿本次请求的信息（host、Range、**Referer、Cookie**、IP、token…）
回调 `auth-cdn.uc.cn/outer/oss/checkplay` 做鉴权。

不带 Cookie 直接请求会得到：

```xml
<Code>RequestDeniedByCallback</Code>
<Message>Callback deny this request reason: require login [auth not found]</Message>
```

所以**必须带上你自己 UC 账号的登录态**——这也是本工具唯一需要你提供的东西。

> 顺带一个坑：UC 的回调只放行 `GET`，用 `HEAD` 探测文件大小会被 403 拒绝，
> 所以代码里用 `Range: bytes=0-0` 的 GET 来拿文件大小。

## 为什么并行就能提速

UC 对**每一条连接**单独限速（实测约 0.5 MB/s）。单连接再快也就那样，
所以把文件切成若干分块、同时开几十条连接各拉一块，聚合速度就上去了。

---

## 使用方法（三步）

1. 在 UC 网页点「下载」，让浏览器先开始下载（只为生成临时直链，可随时取消）
2. 浏览器按 `F12` → **Network（网络）** 面板 → 找到那条 mp4 请求
   （名字像一串 40 位十六进制、Type 显示 `document`）
   → 右键该请求 → **Copy → Copy as cURL (bash)**
3. 打开本程序，把整段内容粘进输入框 → 点「解析并开始下载」

程序会自动从这段 cURL 里解析出 **直链 / Cookie / Referer / UA**，不用你手填任何字段。
启动时会自动读剪贴板，如果里面已经是 curl 内容会自动填好。

下完点「打开所在文件夹」即可，文件默认保存在 `用户目录\Downloads`。

### 界面里能看到什么

- 进度条百分比
- `已下载 / 总大小`、**瞬时速度**、**平均速度**
- 已完成分块数 / 总分块数、预计剩余时间
- 日志区（解析结果、续传提示、错误原因、完成后的校验结果）
- 窗口标题栏同步显示 `62.3% · 27.8 MB/s`

---

## 两种版本

| 文件 | 说明 |
|---|---|
| `uc_gui.py` | 图形界面版（tkinter，**只用 Python 标准库**，无第三方依赖） |
| `download.py` | 命令行版，读同目录的 `url.txt`（直链）和 `cookie.txt`（Cookie 头） |

命令行版用法：

```bash
python download.py --conns 64 --chunk 4 --out "doro916(1).mp4"
```

### 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--conns` | 64 | 并发连接数。跑不满就加；加到 96 没提升说明卡在宽带 |
| `--chunk` | 4 MB | 分块大小。网络抖动大降到 2 MB（重试代价小），追求快可升 8 MB |
| `--out` | 自动 | 输出文件名，默认从直链的 `response-content-disposition` 还原 |

---

## 断点续传

- 进度记录在同名文件的 `.parts.json` 里，**每完成一块就原子写入一次**
- 中途关窗 / 停止 / 崩溃，下次粘贴**新直链**即可接着下（直链只有 3 小时有效期，过期必须重新获取）
- 分块是**收完才写盘**，所以最坏情况只是重下几个分块，不会出现半个分块被当成完成
- 下载完成后自动校验：文件大小一致 + 全零块扫描（检查有没有空洞）

---

## 常见问题

**报 `403 require login [auth not found]`**
没按要求用 Copy as cURL（只粘了链接），或 Cookie 已失效。按三步重来。

**报 `403 直链已过期`**
直链有效期约 3 小时。回网页重新点下载 → 重新 Copy → 粘贴，程序会自动发现上次进度继续续传。

**进度条开头几秒不动 / 增幅偏小**
正常。速度按"已接收字节"实时统计，开头建连时偏低，稳定后会拉起来。

**杀毒软件报警 / 提示未知发布者**
这类单文件小工具常被误报，添加信任即可。不放心可以直接跑源码 `python uc_gui.py`（只用标准库，无第三方依赖）。

**闪退且没有界面**
同目录会生成 `crash.log`，看里面的报错即可定位。

---

## 免责声明

- 本工具**只是把你本来就有权访问的文件用多条连接并发下载**，不破解任何付费/权限限制
- 请遵守你所在地区的法律法规以及 UC/阿里云的服务条款，仅用于个人合理使用
- 请勿传播他人的 Cookie；**Cookie 等同于账号登录态**，拿到就能进对应账号
- 若 UC 调整回调鉴权或限速策略，本工具会失效，需要跟着更新

## License

代码采用 [MIT](LICENSE)。第三方角色形象、参考素材不在授权范围内。

---

## English

Multi-threaded downloader for UC (uc.cn) share links. The official web download is throttled
**per connection** (~0.5 MB/s), so this tool splits the file into chunks and pulls them over
dozens of parallel `Range` requests — measured 1.58 GB in 58 s (27.8 MB/s).

UC's signed OSS URLs are gated by a callback that requires your **logged-in cookies**, so paste
a "Copy as cURL" from DevTools and the GUI parses URL/cookie/referer automatically.
Supports resume, retry, and post-download verification. Python 3 + tkinter only.
