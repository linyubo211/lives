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
# 💡 修复一：补全 key=txiptv 的 "v"
CHECK_PATH = "/iptv/live/1000.json?key=txiptv"
M3U_FILE = "py/hb_telecom.m3u"
TVBOX_FILE = "py/hb_telecom_tvbox.txt"
HISTORY_FILE = "py/scanned_history.json"
CONCURRENCY = 200 if sys.platform == 'win32' else 800  

# 🚫 黑名单列表
IP_BLACKLIST = [
    "42.231.62.137", 
    "42.231.1.1",
]

# 💡 修复二：加入标准浏览器请求头，防止服务端返回 400 Bad Request
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9"
}

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

async def check_host_alive(semaphore, ip, port, pbar):
    async with semaphore:
        writer = None
        try:
            fut = asyncio.open_connection(ip, port)
            reader, writer = await asyncio.wait_for(fut, timeout=2.5)
            tqdm.write(f"📡 [发现响应] {ip}:{port} 端口开放，准备伪装请求抓取数据...")
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

# 获取数据函数：带 Headers 伪装请求，并支持详细报错输出
async def fetch_data(session, target_list):
    results = []
    fetch_limit = asyncio.Semaphore(5) 

    async def fetch_single_ip(ip, port):
        async with fetch_limit:
            for attempt in range(3):
                try:
                    url = f"http://{ip}:{port}{CHECK_PATH}"
                    # 💡 传入伪装 headers
                    async with session.get(url, headers=DEFAULT_HEADERS, timeout=10) as resp:
                        if resp.status != 200:
                            print(f"⚠️ [接口失败] {ip}:{port} | HTTP状态码: {resp.status} (带请求头仍400说明Key或参数需要调整)")
                            return []
                        
                        try:
                            data = await resp.json(content_type=None)
                        except Exception as je:
                            print(f"⚠️ [JSON解析错误] {ip}:{port} | 无法解析为JSON格式: {je}")
                            return []
                        
                        # 宽松匹配：支持 code=0，或者没有 code 只要包含 data 列表也行
                        if ("data" in data) and (data.get("code") == 0 or "code" not in data or data.get("code") == "0"):
                            chunk = []
                            for item in data["data"]:
                                name = item.get("name", "")
                                raw_url = item.get("url", "")
                                chid = item.get("chid", "")
                                
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
                            print(f"✅ [成功提取] {ip}:{port} | 抓取到频道数: {len(chunk)}")
                            return chunk
                        else:
                            print(f"⚠️ [数据结构不符] {ip}:{port} | 键包含值: {list(data.keys())}")
                            return []
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(random.uniform(2, 5))
                    else:
                        print(f"❌ [请求异常] {ip}:{port} | 原因: {e}")
                    continue
            return []

    tasks = [fetch_single_ip(target[0], target[1]) for target in target_list]
    all_chunks = await asyncio.gather(*tasks)
    for chunk in all_chunks:
        results.extend(chunk)
    return results

async def main():
    history_targets = []
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
                        history_targets.append((ip, 8082))  
        except: pass

    scan_targets = []
    for prefix, port in TARGET_CONFIG.items():
        for i in range(256):
            for j in range(256):
                ip = f"{prefix}.{i}.{j}"
                if ip not in IP_BLACKLIST:
                    scan_targets.append((ip, port))

    all_targets = list(dict.fromkeys(history_targets + scan_targets))
    if IP_BLACKLIST:
        print(f"🛡️ 已从扫描列表中屏蔽 {len(IP_BLACKLIST)} 个黑名单 IP。")
    
    semaphore = asyncio.Semaphore(CONCURRENCY)
    alive_targets = []

    print(f"🚀 开始探测 {len(all_targets)} 个目标（按网段分配对应端口）")
    
    with tqdm(total=len(all_targets), desc="🔍 扫描进度", unit="IP", colour="cyan") as pbar:
        async def run_task(ip, port):
            res = await check_host_alive(semaphore, ip, port, pbar)
            if res:
                alive_targets.append(res)

        tasks = [run_task(ip, port) for ip, port in all_targets]
        await asyncio.gather(*tasks)
    
    print(f"\n📡 探测完成，共找到 {len(alive_targets)} 个有响应的服务器，开始伪装抓取...")

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
            print("❌ 数据提取阶段结束：未能从响应服务器中解析出有效节目单。")
    else:
        print("❌ 未发现任何有响应的活跃直播源。")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 已由用户手动停止。")
