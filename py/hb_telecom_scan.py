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

# --- 核心配置：支持不同网段对应不同端口 ---
TARGET_CONFIG = {
    "61.52": 8888,  
    "125.46": 60000,
    "106.46": 9000,
    "125.42": 5566,    
}
CHECK_PATH = "/iptv/live/1000.json?key=txipt"
M3U_FILE = "py/hb_telecom.m3u"
TVBOX_FILE = "py/hb_telecom_tvbox.txt"
HISTORY_FILE = "py/scanned_history.json"
# GitHub Actions 环境保持 800 并发，兼顾速度与稳定性，防止因瞬间并发太高被运营商防火墙丢包
CONCURRENCY = 200 if sys.platform == 'win32' else 800  

# 🚫 黑名单列表
IP_BLACKLIST = [
    "42.231.62.137", 
    "42.231.1.1",
]

PROVINCIAL_LOGIC = ['浙江卫视', '湖南卫视', '东方卫视', '北京卫视', '江苏卫视', '江西卫视', '深圳卫视', '湖北卫视', '吉林卫视', '四川卫视', '天津卫视', '宁夏卫视', '安徽卫视', '山东卫视', '山西卫视', '广东卫视', '广西卫视', '东南卫视', '内蒙古卫视', '黑龙江卫视', '新疆卫视', '河北卫视', '河南卫视', '云南卫视', '海南卫视', '甘肃卫视', '西藏卫视', '贵州卫视', '辽宁卫视', '陕西卫视', '青海卫视', '康巴卫视', '三沙卫视', '大湾区卫视']

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

# 修改探测函数：只要 TCP 握手成功，利用 tqdm.write 实时向控制台输出日志
async def check_host_alive(semaphore, ip, port, pbar):
    async with semaphore:
        writer = None
        try:
            fut = asyncio.open_connection(ip, port)
            reader, writer = await asyncio.wait_for(fut, timeout=2.5)  # 适当放宽超时，保障稳定性
            
            # 📢 实时日志改进：只要对应端口有反应，立即打印（不会破坏 tqdm 进度条结构）
            tqdm.write(f"📡 [发现响应] {ip}:{port} 端口处于开放状态，准备抓取数据...")
            
            return (ip, port)
        except:
            return None
        finally:
            if writer:
                try:
                    writer.close()
                except:
                    pass
            pbar.update(1)

# 获取数据函数：提取各自网段绑定的独立 port 拼接 URL
async def fetch_data(session, target_list):
    results = []
    fetch_limit = asyncio.Semaphore(5) 

    async def fetch_single_ip(ip, port):
        async with fetch_limit:
            for attempt in range(3):
                try:
                    url = f"http://{ip}:{port}{CHECK_PATH}"
                    async with session.get(url, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            if data.get("code") == 0 and "data" in data:
                                chunk = []
                                for item in data["data"]:
                                    name = item.get("name", "")
                                    raw_url = item.get("url", "")
                                    chid = item.get("chid", "")
                                    
                                    # 根据当前 IP 的专属端口进行拼接
                                    if "tsfile" in raw_url.lower() or ".m3u8" in raw_url.lower():
                                        final_url = f"http://{ip}:{port}{raw_url}"
                                    else:
                                        formatted_chid = str(chid).zfill(4)
                                        final_url = f"http://{ip}:{port}/tsfile/live/{formatted_chid}_1.m3u8?key=txiptv&playlive=1&authid=0"

                                    clean_name, weight = clean_and_weight(name)
                                    cat = "央视" if weight < 100 else ("卫视" if weight < 300 else "地方")
                                    chunk.append({
                                        "name": clean_name,
                                        "url": final_url,
                                        "cat": cat,
                                        "weight": float(weight),
                                        "ip": ip
                                    })
                                print(f"✅ [成功提取数据] {ip}:{port} | 频道总数: {len(chunk)}")
                                return chunk
                except Exception:
                    if attempt < 2:
                        await asyncio.sleep(random.uniform(2, 5))
                    continue
            return []

    tasks = [fetch_single_ip(target[0], target[1]) for target in target_list]
    all_chunks = await asyncio.gather(*tasks)
    for chunk in all_chunks:
        results.extend(chunk)
    return results

async def main():
    history_targets = []
    # 读取历史记录并智能匹配旧数据的端口号
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                old_history = json.load(f)
                for ip in old_history:
                    matched = False
                    for prefix, port in TARGET_CONFIG.items():
                        if ip.startswith(prefix):
                            history_targets.append((ip, port))
                            matched = True
                            break
                    if not matched:
                        history_targets.append((ip, 8082))  # 默认兜底端口
        except: pass

    # 生成带独立端口的任务列表
    scan_targets = []
    for prefix, port in TARGET_CONFIG.items():
        for i in range(256):
            for j in range(256):
                ip = f"{prefix}.{i}.{j}"
                if ip not in IP_BLACKLIST:
                    scan_targets.append((ip, port))

    # 合并、去重
    all_targets = list(dict.fromkeys(history_targets + scan_targets))
    
    if IP_BLACKLIST:
        print(f"🛡️ 已从扫描列表中屏蔽 {len(IP_BLACKLIST)} 个黑名单 IP。")
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    alive_targets = []

    print(f"🚀 开始探测 {len(all_targets)} 个目标（按网段动态分配对应端口）")
    
    # 📢 流式并发改进：使用 asyncio.gather 配合内部信号量做到实时产生日志，不再批量憋着
    with tqdm(total=len(all_targets), desc="🔍 扫描进度", unit="IP", colour="cyan") as pbar:
        async def run_task(ip, port):
            res = await check_host_alive(semaphore, ip, port, pbar)
            if res:
                alive_targets.append(res)

        tasks = [run_task(ip, port) for ip, port in all_targets]
        await asyncio.gather(*tasks)
    
    print(f"\n📡 探测完成，共找到 {len(alive_targets)} 个有响应的服务器，开始进入接口抓取环节...")

    if alive_targets:
        async with aiohttp.ClientSession() as session:
            all_channels = await fetch_data(session, alive_targets)
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
            print(f"✅ 任务成功！生成有效源总条数: {len(all_channels)}")
    else:
        print("❌ 未发现任何有响应的活跃直播源。")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 已由用户手动停止。")
