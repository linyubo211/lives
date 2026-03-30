import asyncio
import aiohttp
import os
import json
import re
import sys
import warnings
import random
from tqdm import tqdm

# 屏蔽 Python 3.14+ 的弃用警告
warnings.filterwarnings("ignore", category=DeprecationWarning)

# --- 配置 ---
TARGET_PREFIX = "42.231"
TARGET_PORT = 9901
CHECK_PATH = "/iptv/live/1000.json?key=txipt"
M3U_FILE = "py/hb_telecom.m3u"
TVBOX_FILE = "py/hb_telecom_tvbox.txt"
HISTORY_FILE = "py/scanned_history.json"
CONCURRENCY = 400 if sys.platform == 'win32' else 1000 

# 🚫 黑名单列表：直接在这里填写那些无流量、失效的 IP
IP_BLACKLIST = [
    "42.231.62.137", 
    "42.231.1.1",
    # "在此继续添加你想屏蔽的IP..."
]

PROVINCIAL_LOGIC = ['浙江卫视', '湖南卫视', '东方卫视', '北京卫视', '江苏卫视', '江西卫视', '深圳卫视', '湖北卫视', '吉林卫视', '四川卫视', '天津卫视', '宁夏卫视', '安徽卫视', '山东卫视', '山西卫视', '广东卫视', '广西卫视', '东南卫视', '内蒙古卫视', '黑龙江卫视', '新疆卫视', '河北卫视', '河南卫视', '云南卫视', '海南卫视', '甘肃卫视', '西藏卫视', '贵州卫视', '辽宁卫视', '陕西卫视', '青海卫视', '康巴卫视', '三沙卫视', '大湾区卫视']

# ... (update_history_log 和 clean_and_weight 函数保持不变) ...

def update_history_log(current_ips):
    existing_history = []
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                existing_history = json.load(f)
        except: pass
    new_ips = [ip for ip in current_ips if ip not in existing_history and ip not in IP_BLACKLIST]
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
            fut = asyncio.open_connection(ip, TARGET_PORT)
            reader, writer = await asyncio.wait_for(fut, timeout=2.0)
            return ip
        except:
            return None
        finally:
            if writer:
                try:
                    writer.close()
                except:
                    pass
            pbar.update(1)

async def fetch_data(session, ip_list):
    results = []
    fetch_limit = asyncio.Semaphore(5) 

    async def fetch_single_ip(ip):
        async with fetch_limit:
            for attempt in range(3):
                try:
                    url = f"http://{ip}:{TARGET_PORT}{CHECK_PATH}"
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
                                print(f"✅ [成功] {ip}:{TARGET_PORT} | 台数: {len(chunk)}")
                                return chunk
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(random.uniform(2, 5))
                    continue
            return []

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
        except: pass

    # --- 2. 生成全量爆破 IP 列表 ---
    scan_ips = [f"{TARGET_PREFIX}.{i}.{j}" for i in range(256) for j in range(256)]
    
    # --- 3. 合并并过滤黑名单 ---
    # 先合并历史和扫描列表，然后排除黑名单中的 IP
    all_ips = [ip for ip in dict.fromkeys(history_ips + scan_ips) if ip not in IP_BLACKLIST]
    
    if IP_BLACKLIST:
        print(f"🛡️ 已从扫描列表中屏蔽 {len(IP_BLACKLIST)} 个黑名单 IP。")
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    alive_ips = []

    print(f"🚀 开始探测 {len(all_ips)} 个目标 (端口: {TARGET_PORT})")
    with tqdm(total=len(all_ips), desc="🔍 扫描进度", unit="IP", colour="cyan") as pbar:
        async def run_task(ip):
            res = await check_host_alive(semaphore, ip, pbar)
            if res:
                alive_ips.append(res)

        tasks = []
        for ip in all_ips:
            tasks.append(run_task(ip))
            if len(tasks) >= 2000: 
                await asyncio.gather(*tasks)
                tasks = []
        if tasks:
            await asyncio.gather(*tasks)
    
    print(f"\n📡 探测完成，共找到 {len(alive_ips)} 个活跃服务器。")

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 已由用户手动停止。")
