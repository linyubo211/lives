import asyncio
import aiohttp
import os
import json
import re
import sys
import warnings
import random # 记得在脚本顶部添加这个导入
from tqdm import tqdm

# 屏蔽 Python 3.14+ 的弃用警告
warnings.filterwarnings("ignore", category=DeprecationWarning)

# --- 配置 ---
TARGET_PREFIX = "42.23"
TARGET_PORT = 9901
CHECK_PATH = "/iptv/live/1000.json?key=txipt"
M3U_FILE = "py/hb_telecom.m3u"
TVBOX_FILE = "py/hb_telecom_tvbox.txt"
HISTORY_FILE = "py/scanned_history.json"
CONCURRENCY = 400 if sys.platform == 'win32' else 1000 

PROVINCIAL_LOGIC = ['浙江卫视', '湖南卫视', '东方卫视', '北京卫视', '江苏卫视', '江西卫视', '深圳卫视', '湖北卫视', '吉林卫视', '四川卫视', '天津卫视', '宁夏卫视', '安徽卫视', '山东卫视', '山西卫视', '广东卫视', '广西卫视', '东南卫视', '内蒙古卫视', '黑龙江卫视', '新疆卫视', '河北卫视', '河南卫视', '云南卫视', '海南卫视', '甘肃卫视', '西藏卫视', '贵州卫视', '辽宁卫视', '陕西卫视', '青海卫视', '康巴卫视', '三沙卫视', '大湾区卫视']

def update_history_log(current_ips):
    existing_history = []
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                existing_history = json.load(f)
        except: pass
    new_ips = [ip for ip in current_ips if ip not in existing_history]
    if new_ips:
        updated_history = list(set(existing_history + new_ips))
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(updated_history, f, indent=4, ensure_ascii=False)
        print(f"\n📝 历史记录已更新，新增了 {len(new_ips)} 个 IP。")

def clean_and_weight(name):
    name_upper = name.upper().replace(" ", "").replace("-", "")
    if "CCTV5+" in name_upper: return "CCTV5+", 5.5
    if "CCTV" in name_upper:
        match = re.search(r'CCTV(\d+)', name_upper)
        if match: return f"CCTV{match.group(1)}", int(match.group(1))
        return name, 99
    for i, p in enumerate(PROVINCIAL_LOGIC):
        if p in name: return p, 100 + i 
    return name, 999

async def check_host_alive(semaphore, ip, pbar):
    async with semaphore:
        writer = None
        try:
            # 增加一点点超时缓冲，降低并发压力
            fut = asyncio.open_connection(ip, TARGET_PORT)
            reader, writer = await asyncio.wait_for(fut, timeout=2.0)
            return ip
        except:
            return None
        finally:
            # 安全地关闭，不抛出异常
            if writer:
                try:
                    writer.close()
                    # 在高并发下不一定要 wait_closed()，可以直接让循环处理
                except:
                    pass
            pbar.update(1)            



async def fetch_data(session, ip_list):
    """
    针对已确认存活的 IP，分批、限速、带重试地请求 9901 端口的数据
    """
    results = []
    # 核心：限制并发。即使扫描出了 100 个存活 IP，我们也只允许 5 个同时进行数据交换
    # 这样可以给服务器留出 CPU 响应时间，不容易超时
    fetch_limit = asyncio.Semaphore(5) 

    async def fetch_single_ip(ip):
        async with fetch_limit:
            # 给予 3 次重试机会
            for attempt in range(3):
                try:
                    url = f"http://{ip}:{TARGET_PORT}{CHECK_PATH}"
                    # 关键：将请求超时增加到 10 秒
                    # 有些酒店源扫描 JSON 列表非常慢，2-5 秒是不够的
                    async with session.get(url, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            if data.get("code") == 0 and "data" in data:
                                chunk = []
                                for item in data["data"]:
                                    name = item.get("name", "")
                                    clean_name, weight = clean_and_weight(name)
                                    cat = "央视" if weight < 100 else ("卫视" if weight < 300 else "地方")
                                    chunk.append({
                                        "name": clean_name,
                                        "url": f"http://{ip}:{TARGET_PORT}{item.get('url')}",
                                        "cat": cat,
                                        "weight": float(weight),
                                        "ip": ip
                                    })
                                # 只要抓取成功，就打印一下，方便在 Actions 日志里看
                                print(f"✅ [成功] {ip}:{TARGET_PORT} | 台数: {len(chunk)}")
                                return chunk
                except Exception as e:
                    # 如果不是最后一次尝试，就随机等一下再试
                    if attempt < 2:
                        wait_time = random.uniform(2, 5) # 错峰 2-5 秒
                        await asyncio.sleep(wait_time)
                    continue
            
            print(f"❌ [失败] {ip}:{TARGET_PORT} 响应超时或格式错误")
            return []

    # 并行启动所有存活 IP 的抓取任务，但受 fetch_limit 约束
    tasks = [fetch_single_ip(ip) for ip in ip_list]
    all_chunks = await asyncio.gather(*tasks)
    
    for chunk in all_chunks:
        results.extend(chunk)
    return results

async def main():
    # --- 1. 加载历史存量 IP ---
    history_ips = []
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                history_ips = json.load(f)
            print(f"📜 发现历史记录，包含 {len(history_ips)} 个已知 IP。")
        except:
            pass

    # --- 2. 生成全量爆破 IP 列表 ---
    print(f"🚀 开始探测 {TARGET_PREFIX}.x.y (端口: {TARGET_PORT})")
    scan_ips = [f"{TARGET_PREFIX}.{i}.{j}" for i in range(256) for j in range(256)]
    
    # --- 3. 合并：历史 IP 放在最前面优先执行 ---
    # 使用 set 去重，确保不会重复扫描
    all_ips = list(dict.fromkeys(history_ips + scan_ips))
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    alive_ips = []

    with tqdm(total=len(all_ips), desc="🔍 扫描进度", unit="IP", colour="cyan") as pbar:
        async def run_task(ip):
            res = await check_host_alive(semaphore, ip, pbar)
            if res:
                alive_ips.append(res)

        tasks = []
        for ip in all_ips:
            tasks.append(run_task(ip))
            # 控制协程积压，每 2000 个任务处理一次
            if len(tasks) >= 2000: 
                await asyncio.gather(*tasks)
                tasks = []
        
        if tasks:
            await asyncio.gather(*tasks)
    
    print(f"\n📡 探测完成，共找到 {len(alive_ips)} 个活跃服务器。")
    # ... 后面获取数据的逻辑保持不变 ...
    if alive_ips:
        async with aiohttp.ClientSession() as session:
            all_channels = await fetch_data(session, alive_ips)
        if all_channels:
            all_channels.sort(key=lambda x: ({"央视":0,"卫视":1,"地方":2}.get(x['cat'],3), x['weight'], x['name']))
            os.makedirs("py", exist_ok=True)
            
            with open(M3U_FILE, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for ch in all_channels:
                    f.write(f"#EXTINF:-1 group-title=\"{ch['cat']}\",{ch['name']}\n{ch['url']}\n")
            
            cat_dict = {}
            for ch in all_channels:
                cat_dict.setdefault(ch['cat'], []).append(f"{ch['name']},{ch['url']}")
            with open(TVBOX_FILE, "w", encoding="utf-8") as f:
                for cat in ["央视", "卫视", "地方"]:
                    if cat in cat_dict:
                        f.write(f"{cat},#genre#\n" + "\n".join(cat_dict[cat]) + "\n")
            
            update_history_log(list(set([ch['ip'] for ch in all_channels])))
            print(f"✅ 任务成功！生成源条数: {len(all_channels)}")
    else:
        print("❌ 未发现任何有效直播源。")

if __name__ == "__main__":
    # 针对 Python 3.14+ 自动处理策略
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 已由用户手动停止。")
