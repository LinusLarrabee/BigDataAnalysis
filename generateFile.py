import json
import random
from datetime import datetime, timedelta

# 定义一些路径用于生成访问链路
paths = ["/home", "/about", "/products", "/contact", "/services", "/blog", "/faq"]

# 生成访问链路数据
data = []
for i in range(500):
    uvi = f"user{i+1:03d}"
    el = []
    visit_count = random.randint(3, 7)
    base_time = datetime.now()
    for j in range(visit_count):
        path = random.choice(paths)
        ct = (base_time + timedelta(minutes=j*random.randint(1, 5))).isoformat()
        el.append({"path": path, "ct": ct})
    data.append({"uvi": uvi, "el": el})

# 保存到txt文件中
with open('input.txt', 'w') as f:
    for entry in data:
        json.dump(entry, f)
        f.write('\n')
