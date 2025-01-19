import csv
# 读取 featnames 文件，获取特征名称
feat_mapping = {}
with open('../facebook/414.featnames', 'r') as f:
    for line in f:
        
        parts = line.strip().split()  # 按空格分割：
        # 6 birthday;anonymized feature 211转换为
        # [0]6 [1]birthday;anonymized [2]feature [3]211
        index = parts[0]  # 特征索引
        property_parts = parts[1].split(';')[0:-1]
        # 去掉最后一个部分 "anonymized"：
        # birthday;anonymized转换为
        # [0]birthday
        property_key = '-'.join(property_parts)# 将key值当中多余的";"替换为 "-"
        property_value = parts[-1].split()[-1]  # 提取特征值
        feat_mapping[index] = (property_key, property_value)


# 处理 feat 文件
all_properties = set()
for key, (property_key, _) in feat_mapping.items():
    all_properties.add(property_key)

# 写入 CSV 文件
with open('../facebook/414.feat', 'r') as infile, open('../csv_data/features.csv', 'w', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=['node_id'] + sorted(all_properties))
    writer.writeheader()  # 写入 CSV 文件头部

    for line in infile:
        parts = line.strip().split()
        node_id = parts[0]  # 节点 ID
        features = parts[1:]  # 特征向量
        row = {'node_id': node_id}

        for i, val in enumerate(features):
            if val == '1':  # 如果特征值为 1
                property_key, property_value = feat_mapping.get(str(i), (None, None))
                if property_key:
                    row[property_key] = property_value

        # 将缺失的属性填充为空字符串，到时候创建点的时候不要加就行
        for prop in all_properties:
            if prop not in row:
                row[prop] = ''

        writer.writerow(row)
        
# 对 egofeat 文件也进行相同的处理，唯一需要注意的是egofeat文件开头没有id
with open('../facebook/414.egofeat', 'r') as infile, open('../csv_data/ego_features.csv', 'w', newline='') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=['node_id'] + sorted(all_properties))
    writer.writeheader()  # 写入 CSV 文件头部

    line = infile.readline().strip()
    features = line.split()  # 特征向量
    row = {'node_id': 414}  # ego 节点的 ID 是 414

    for i, val in enumerate(features):
        if val == '1':  # 如果特征值为 1
            property_key, property_value = feat_mapping.get(str(i), (None, None))
            if property_key:
                row[property_key] = property_value

    # 将缺失的属性填充为空字符串
    for prop in all_properties:
        if prop not in row:
            row[prop] = ''

    writer.writerow(row)