import pandas as pd

# 读取CSV文件
file_path = 'path_chain_counts.csv'
data = pd.read_csv(file_path)

# 解析数据源
paths = []
weights = []
full_paths = []  # 新增的列表，用于存储完整路径

for index, row in data.iterrows():
    path = row['path_chain']
    weight = int(row['count'])
    pages = path.split(' -> ')

    # 生成带有顺序的标签
    for i in range(len(pages) - 1):
        source_label = f"{pages[i]} ({i+1})"
        target_label = f"{pages[i+1]} ({i+2})"
        paths.append((source_label, target_label))
        weights.append(weight)
        full_paths.append(path)  # 记录完整路径

# 创建标签列表和索引映射
labels = list(set([item for sublist in paths for item in sublist]))
label_index = {label: i for i, label in enumerate(labels)}

# 创建source和target列表
source = [label_index[path[0]] for path in paths]
target = [label_index[path[1]] for path in paths]

# 保存处理后的数据到CSV
processed_data = pd.DataFrame({
    'source': [labels[s] for s in source],
    'target': [labels[t] for t in target],
    'weight': weights,
    'full_path': full_paths  # 添加完整路径信息
})

processed_data.to_csv('processed_path_chain_counts.csv', index=False)
