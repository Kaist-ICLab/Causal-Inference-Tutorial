import zipfile
import os
from tqdm import tqdm

# Extracting the Raw Data
src= os.path.join("/mnt","Zips", "RawZips")
target = os.path.join("/mnt", "Raws")
if not os.path.exists(target):
    os.makedirs(target)
for file in tqdm(sorted(os.listdir(src))):
    uid = os.path.splitext(file)[0]
    path = os.path.join(src, file)
    os.makedirs(os.path.join(target, uid), exist_ok= True)
    zipfile.ZipFile(path).extractall(os.path.join(target, uid))