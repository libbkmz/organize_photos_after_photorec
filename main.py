import sys
import os
import time
from random import shuffle
import shelve
from dataclasses import dataclass
import imagehash
from PIL import Image
import tqdm
import concurrent.futures as cf
import multiprocessing as mp
from more_itertools import divide
FOLDER_PATH = "/6TB/data/Transcend_HDD/"
CF_WORKERS = 6


def proceed_file(filepath):
    print(filepath)

def is_image_filename(filepath):
    return os.path.splitext(filepath.lower())[-1][1:] in ["jpg", "gif", "bmp", "png", "jpeg", "tiff"]

def get_images():
    for (dirpath, dirname, filenames) in os.walk(FOLDER_PATH):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if is_image_filename(filepath):
                yield filepath

@dataclass
class Hashes:
    file_path: str
    avg_hash: imagehash.ImageHash
    avg_hash_long: imagehash.ImageHash
    p_hash: imagehash.ImageHash

def worker(file_list, q):
    for filepath in file_list:
        img = Image.open(filepath)
        q.put_nowait(Hashes(
                file_path = filepath,
                avg_hash = imagehash.average_hash(img),
                avg_hash_long = imagehash.average_hash(img, hash_size=32),
                p_hash = imagehash.phash(img),
            )
        )

def main():
    files_list = list(get_images())
    shuffle(files_list)
    executor = cf.ProcessPoolExecutor(max_workers=CF_WORKERS)
    files_list_divided = divide(CF_WORKERS, files_list)
    fs = []
    manager = mp.Manager()
    shared_queue = manager.Queue()
    for file_list in files_list_divided:
        fs.append(
            executor.submit(worker, file_list, shared_queue)
        )
    print("SENT")
    with shelve.open("image_hashes.dat") as db:
        with tqdm.tqdm(total=len(files_list), smoothing=0.1) as pbar:
            while True:
                while not shared_queue.empty():
                    res: Hashes = shared_queue.get_nowait()
                    pbar.update(1)
                    db[res.file_path] = res
                    db.sync()
                
                done, not_done = cf.wait(fs, timeout=0, return_when=cf.ALL_COMPLETED)
                if len(not_done) == 0:
                    break
                for done_res in done:
                    if done_res.exception() is not None:
                        raise done_res.exception()
                time.sleep(0.01)
        
    for x in cf.as_completed(fs):
        print(x.result())
    

if __name__ == "__main__":
    main()
