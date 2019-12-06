from helpers import Hashes
from collections import defaultdict, Counter
import shelve
import os
import shutil
TARGET_DIR = "/6TB/data/transcend_tmp/"

if os.path.exists(TARGET_DIR):
    shutil.rmtree(TARGET_DIR)
os.makedirs(TARGET_DIR)


def main():
    images = defaultdict(list)
    cc = Counter()
    
    with shelve.open("image_hashes.dat") as db:
        for k, v in db.items():
            images[v.p_hash].append(v)

        for k, v in images.items():
            file_count = len(v)
            first_filename = os.path.basename(v[0].file_path.rsplit(".", 1)[0])
            for hash_obj in v:
                src_file_path = hash_obj.file_path
                file_name = os.path.basename(src_file_path)
                dst_file_path_dir = os.path.join(TARGET_DIR, "%s" % file_count, first_filename)
                if not os.path.exists(dst_file_path_dir):
                    os.makedirs(dst_file_path_dir)
                dst_file_path = os.path.join(dst_file_path_dir, file_name)
                try:
                    os.link(src_file_path, dst_file_path)
                except FileExistsError:
                    print("Skip: %s -> %s" % (src_file_path, dst_file_path))
#            break
                

        


        
if __name__ == "__main__":
    main()